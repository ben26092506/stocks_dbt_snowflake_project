import os
import boto3
import snowflake.connector
from airflow.decorators import dag, task
from datetime import datetime, timedelta

MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'admin'
MINIO_SECRET_KEY = 'password123'
BUCKET = 'bronze-transactions'
LOCAL_DIR = '/tmp/minio_downloads'

SNOWFLAKE_USER = 'INSERT YOUR USER NAME HERE'
SNOWFLAKE_PASSWORD = 'INSERT YOUR PASSWORD HERE'
SNOWFLAKE_ACCOUNT = 'INSERT YOUR ACCOUNT NAME HERE'
SNOWFLAKE_WAREHOUSE = 'INSERT YOUR WAREHOUSE HERE'
SNOWFLAKE_DB = 'INSERT YOUR DATABASE HERE'
SNOWFLAKE_SCHEMA = 'INSERT YOUR SCHEMA HERE'

@dag(
    dag_id='minio_to_snowflake_decorators',
    start_date=datetime(2026, 5, 20),
    schedule='*/1 * * * *',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['stocks', 'minio', 'snowflake']
)

def minio_to_snowflake():

    @task
    def download_from_minio():
        os.makedirs(LOCAL_DIR, exist_ok=True)

        s3 = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )

        objects = s3.list_objects_v2(Bucket=BUCKET).get('Contents', [])

        local_files = []

        for obj in objects:
            key = obj['Key']
            local_file = os.path.join(LOCAL_DIR, os.path.basename(key))

            s3.download_file(BUCKET, key, local_file)

            print(f'Downloaded {key} -> {local_file}')
            local_files.append(local_file)

        return local_files

    @task
    def load_into_snowflake(local_files: list[str]):
        if not local_files:
            print('No files to load.')
            return
        
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DB,
            schema=SNOWFLAKE_SCHEMA
        )

        cur = conn.cursor()

        try:
            for f in local_files:
                cur.execute(f'PUT file://{f} @%bronze_stock_quotes_raw')
                print(f'Uploaded {f} to Snowflake stage')

            cur.execute('''
                COPY INTO bronze_stock_quotes_raw
                FROM @%bronze_stock_quotes_raw
                FILE_FORMAT = (TYPE=JSON)
            ''')
            print('COPY INTO executed')

        finally:
            cur.close()
            conn.close()

    files = download_from_minio()
    load_into_snowflake(files)

minio_to_snowflake()
