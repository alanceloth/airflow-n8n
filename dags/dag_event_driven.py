from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher, dag, task

# Default arguments for the DAG
default_args = {
    'owner': 'Alan Lanceloth',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

trigger = MessageQueueTrigger(
    aws_conn_id="aws_default",
    queue="http://sqs.us-east-1.d43e-2804-d78-6db-5a00-6-ec22-c627-5d97.ngrok-free.app:4566/000000000000/create_user",
    waiter_delay=30
)

sqs_asset_queue = Asset(
    "sqs_asset_queue",
    watchers=[AssetWatcher(name="sqs_watcher", trigger=trigger)],
)

@dag(
    dag_id='n8n_webhook_btc_report',
    default_args=default_args,
    description='DAG to call n8n webhook and process response',
    schedule=[sqs_asset_queue],
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['n8n', 'webhook', 'btc', 'report'],
)
def n8n_webhook_dag():
    endpoint_url = Variable.get("N8N_WEBHOOK_ENDPOINT")

    # Task to check if endpoint is available
    check_endpoint = HttpSensor(
        task_id='check_endpoint_available',
        http_conn_id="http_n8n",
        endpoint=endpoint_url,
        request_params={},
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=60,
        mode='poke'
    )
    
    # Task to call the webhook
    call_webhook = HttpOperator(
        task_id='call_n8n_webhook',
        method='POST',
        http_conn_id="http_n8n",
        endpoint=endpoint_url,
        headers={"Content-Type": "application/json"},
        data=json.dumps({"message": "BTC"}),
        response_check=lambda response: True,
        log_response=True,
        do_xcom_push=True
    )

    @task
    def process_message(triggering_asset_events=None):
        for event in triggering_asset_events[sqs_asset_queue]:
            print(f"Processing message: {event.extra['payload']['message_batch'][0]['Body']}")

    # Task to check the response
    @task(task_id="check_webhook_response")
    def check_response(**context):
        ti = context['ti']
        response = ti.xcom_pull(task_ids="call_n8n_webhook")
        
        if not response:
            raise ValueError("No response received from webhook")
        
        try:
            response_json = json.loads(response)
            if isinstance(response_json, dict) and response_json.get('status') == 'error':
                raise ValueError(f"Error in n8n webhook response: {response_json}")
            return "Success - No errors in the response"
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON response: {response}")
    
    # Set task dependencies
    webhook_response = call_webhook
    check_result = check_response()
    
    process_message >> check_endpoint >> webhook_response >> check_result

# Instantiate the DAG
n8n_webhook_dag = n8n_webhook_dag()