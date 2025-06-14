from airflow.hooks.base import BaseHook
import boto3
from airflow.decorators import dag, task
import pendulum

def get_sqs_client():
    conn = BaseHook.get_connection('aws_default')
    extras = conn.extra_dejson
    endpoint = extras.get('endpoint_url')
    region = extras.get('region_name', 'us-east-1')
    aws_key = conn.login or 'test'
    aws_secret = conn.password or 'test'
    client = boto3.client(
        'sqs',
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
    )
    return client

@dag(
    dag_id='list_sqs',
    schedule=None, 
    start_date=pendulum.datetime(2025,6,14, tz="America/Sao_Paulo"),
    catchup=False,
    tags=['sqs', 'list'],
)
def exemplo_sqs():
    @task
    def listar_filas():
        sqs = get_sqs_client()
        resp = sqs.list_queues()
        print(resp.get('QueueUrls'))
    listar_filas()

exemplo_sqs()
