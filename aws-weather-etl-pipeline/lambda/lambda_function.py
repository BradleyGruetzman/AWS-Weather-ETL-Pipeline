import json
import boto3
import urllib3
import datetime
import os 

s3 = boto3.client('s3')
bucket_name = 'weather-data-pipeline-brad'
city = 'Minneapolis'

def lambda_handler(event, context):
    # Get API key from Lambda environment variables
    api_key = os.environ.get('OPENWEATHER_API_KEY')
    if not api_key:
        return {
            'statusCode': 500,
            'body': 'API key not found in environment variables'
        }

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
