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
    dag_id='list_sqs',              # mantém seu dag_id
    schedule=None,
    start_date=pendulum.datetime(2025, 6, 14, tz="America/Sao_Paulo"),
    catchup=False,
    tags=['sqs', 'localstack'],
)
def exemplo_sqs():
    @task
    def criar_fila():
        sqs = get_sqs_client()
        resp = sqs.create_queue(QueueName='create_user')
        queue_url = resp['QueueUrl']
        print("Queue criada/recuperada:", queue_url)
        return queue_url

    @task
    def listar_filas():
        conn = BaseHook.get_connection('aws_default')
        print("Extras da Connection:", conn.extra_dejson)
        sqs = get_sqs_client()
        # imprime o host que o boto3 vai usar
        print("Endpoint boto3:", sqs._endpoint.host)
        resp = sqs.list_queues()
        print("Queues:", resp.get('QueueUrls'))

    @task
    def enviar_mensagem(queue_url: str):
        sqs = get_sqs_client()
        resp = sqs.send_message(QueueUrl=queue_url, MessageBody='{"foo":"bar"}')
        print("SendMessage:", resp)
        return resp.get('MessageId')

    @task
    def receber_e_deletar(queue_url: str):
        sqs = get_sqs_client()
        resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
        messages = resp.get('Messages') or []
        if not messages:
            print("Nenhuma mensagem recebida.")
            return None
        for msg in messages:
            print("Mensagem recebida:", msg)
            # Deleta após processar
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg['ReceiptHandle'])
            print("Mensagem deletada:", msg['MessageId'])
        return [msg['MessageId'] for msg in messages]

    # Orquestração
    queue_url = criar_fila()
    filas = listar_filas()
    # você pode usar 'filas' se quiser, mas não é necessário para enviar/receber
    msg_id = enviar_mensagem(queue_url)
    receber_e_deletar(queue_url)

exemplo_sqs()
