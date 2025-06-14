from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
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
    schedule_interval=timedelta(days=1),  # Runs once a day, adjust as needed
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['n8n', 'webhook', 'btc', 'report'],
)
def n8n_webhook_btc_report_workflow():
    """
    DAG to call n8n webhook and process the response.
    """
    @task(task_id='call_n8n_webhook')
    def call_webhook():
        """
        Make a POST request to the n8n webhook.
        """
        from airflow.providers.http.hooks.http import HttpHook
        
        # Get endpoint from Airflow Variables
        endpoint = Variable.get("N8N_WEBHOOK_ENDPOINT")
        
        http_hook = HttpHook(method='POST', http_conn_id='http_default')
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"message": "BTC"})
        
        response = http_hook.run(
            endpoint=endpoint,
            data=data,
            headers=headers
        )
        
        # Return the response text to be used by the next task
        return response.text
    
    @task(task_id='check_webhook_response')
    def check_response(webhook_response: str):
        """
        Process the response from the webhook.
        Raises an exception if the response contains an error status.
        """
        response_json = json.loads(webhook_response)
        
        if response_json.get('status') == 'error':
            raise ValueError(f"Error in n8n webhook response: {response_json}")
        return "Success - No errors in the response"
    
    # Call the tasks and define the execution flow
    webhook_response = call_webhook()
    check_response(webhook_response)

# Instantiate the DAG
n8n_webhook_btc_report_dag = n8n_webhook_btc_report_workflow()