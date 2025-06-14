from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
import json

# Default arguments for the DAG
default_args = {
    'owner': 'Alan Lanceloth',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='n8n_webhook_btc_report',
    default_args=default_args,
    description='DAG to call n8n webhook and process response',
    schedule=timedelta(days=1),
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['n8n', 'webhook', 'btc', 'report'],
)
def n8n_webhook_dag():
    endpoint_url = Variable.get("N8N_WEBHOOK_ENDPOINT")
    
    # Task to check if endpoint is available
    check_endpoint = HttpSensor(
        task_id='check_endpoint_available',
        http_conn_id='http_default',
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
        http_conn_id='http_default',
        endpoint=endpoint_url,
        headers={"Content-Type": "application/json"},
        data=json.dumps({"message": "BTC"}),
        response_check=lambda response: True,
        log_response=True,
        do_xcom_push=True
    )

    # Task to check the response
    @task(task_id='check_webhook_response')
    def check_response(**context):
        ti = context['ti']
        response = ti.xcom_pull(task_ids='call_n8n_webhook')
        
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
    
    check_endpoint >> webhook_response >> check_result

# Instantiate the DAG
n8n_webhook_dag = n8n_webhook_dag()