import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import from_unixtime, col

# Retrieve secure parameters
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'DB_ENDPOINT', 'DB_USER', 'DB_PASSWORD']
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    # Input and output paths
    raw_path = "s3://weather-data-pipeline-brad-useast1/raw/"
    processed_path = "s3://weather-data-pipeline-brad-useast1/processed/"
    
    logger.info(f"Reading raw data from {raw_path}")
    
    # Read raw JSON files
    df = spark.read.json(raw_path)
    
    # Log record count
    raw_count = df.count()
    logger.info(f"Raw records read: {raw_count}")
    
    if raw_count == 0:
        logger.warn("No records found in raw data. Exiting job.")
        job.commit()
        sys.exit(0)
    
    # Clean & transform data with null handling
    clean_df = df.select(
        col("name").alias("city"),
        col("main.temp").alias("temperature"),
        col("main.humidity").alias("humidity"),
        col("weather")[0]["description"].alias("description"),
        from_unixtime(col("dt")).alias("timestamp")
    ).filter(
        col("name").isNotNull() & 
        col("main.temp").isNotNull()
    )
    
    clean_count = clean_df.count()
    logger.info(f"Cleaned records: {clean_count}")
    
    if clean_count == 0:
        logger.warn("No valid records after cleaning. Exiting job.")
        job.commit()
        sys.exit(0)
    
    # Write cleaned data to Parquet
    logger.info(f"Writing processed data to {processed_path}")
    clean_df.write.mode("overwrite").parquet(processed_path)
    
    # Build JDBC URL from secure parameter
    jdbc_url = f"jdbc:postgresql://{args['DB_ENDPOINT']}:5432/postgres"
    
    # Test connection first
    logger.info("Testing RDS connection...")
    try:
        test_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "(SELECT 1 as test) tmp") \
            .option("user", args['DB_USER']) \
            .option("password", args['DB_PASSWORD']) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        test_df.show()
        logger.info("✓ Connection test successful")
    except Exception as conn_error:
        logger.error(f"✗ Connection test failed: {str(conn_error)}")
        raise
    
    logger.info("Writing to RDS PostgreSQL")
    
    # Write to RDS PostgreSQL with optimized settings
    clean_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "weather_data") \
        .option("user", args['DB_USER']) \
        .option("password", args['DB_PASSWORD']) \
        .option("driver", "org.postgresql.Driver") \
        .option("batchsize", "1000") \
        .mode("append") \
        .save()
    
    logger.info(f"Successfully loaded {clean_count} records to RDS")
    
    job.commit()
    logger.info("Job completed successfully")

except Exception as e:
    logger.error(f"Job failed with error: {str(e)}")
    raise