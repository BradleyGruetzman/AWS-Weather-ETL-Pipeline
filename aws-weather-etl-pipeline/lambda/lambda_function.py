import json
import boto3
import urllib3
import json
import datetime

s3 = boto3.client('s3')
bucket_name = 'weather-data-pipeline-brad'
api_key = '60668d612af27f1484ddc59b501872e0'
city = 'Minneapolis'

def lambda_handler(event, context):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    http = urllib3.PoolManager()
    response = http.request('GET', url)
    data = json.loads(response.data.decode('utf-8'))
    
    timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    file_name = f"raw/{city}_{timestamp}.json"
    
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(data)
    )
    
    return {
        'statusCode': 200,
        'body': f"Weather data for {city} saved to {file_name}"
    }