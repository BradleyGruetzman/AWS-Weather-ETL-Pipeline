# AWS-Weather-ETL-Pipeline
This project demonstrates a full AWS data engineering pipeline using:
- **AWS Lambda** to pull weather data from OpenWeather API
- **S3** to store raw weather JSON files
- **AWS Glue** for ETL (transform JSON → parquet → RDS)
- **AWS RDS (Postgres)** as a final storage destination
- **pgAdmin4** used to query and validate output


# Database Schema
| column      | type             |
|-------------|------------------|
| city        | text             |
| temperature | double precision |
| humidity    | bigint           |
| description | text             |
| timestamp   | text             |


# SQL Examples

See `sql/sample_queries.sql` for:

- Highest temperature days  
- Avg temp per city  
- Humidity thresholds  
- Weather description frequency  
- Timestamp conversions

# Contact

Created by **Bradley Gruetzman**  
Feel free to reach out with questions or suggestions!
