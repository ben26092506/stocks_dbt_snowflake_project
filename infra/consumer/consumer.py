import json
import boto3
import time
from kafka import KafkaConsumer

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9002',
    aws_access_key_id='admin',
    aws_secret_access_key='password123'
)

bucket_name = 'bronze-transactions'

consumer = KafkaConsumer(
    'stock-quotes',
    bootstrap_servers=['host.docker.internal:29092'],
    enable_auto_commit=True,
    auto_offset_reset='earliest',
    group_id='bronze-consumer',
    value_deserializer= lambda v: json.loads(v.decode('utf-8'))
)

print('Consumer streaming and saving to MinIO...')

for message in consumer:
    record = message.value
    symbol = record.get('symbol')
    ts = record.get('fetched_at', int(time.time()))
    key = f'{symbol}/{ts}.json'

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(record),
        ContentType='application/json'
    )
    print(f'Saved record for {symbol} = s3://{bucket_name}/{key}')