from datetime import datetime, timedelta
import logging
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.exceptions import AirflowException
import json

# Configuração de logging
logger = logging.getLogger(__name__)

def log_response(response):
    """Log detalhado da resposta HTTP"""
    logger.info(f"Status Code: {response.status_code}")
    logger.info(f"Headers: {response.headers}")
    logger.info(f"Response Text: {response.text[:500]}...")  # Limita o tamanho do log

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
    schedule=timedelta(days=1),  # Runs once a day, adjust as needed
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
        
        try:
            # Get endpoint from Airflow Variables
            endpoint = Variable.get("N8N_WEBHOOK_ENDPOINT")
            logger.info(f"Using endpoint: {endpoint}")
            
            # Forçar HTTPS na conexão
            http_hook = HttpHook(
                method='POST',
                http_conn_id='http_default'
            )
            
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "Airflow"
            }
            
            data = json.dumps({"message": "BTC"})
            logger.info(f"Sending data: {data}")
            
            # Fazer a requisição com timeout explícito
            response = http_hook.run(
                endpoint=endpoint,
                data=data,
                headers=headers,
                extra_options={
                    'timeout': 30,
                    'verify': True  # Verificar certificado SSL
                }
            )
            
            # Log detalhado da resposta
            log_response(response)
            
            # Verificar se a requisição foi bem-sucedida
            response.raise_for_status()
            
            # Return the response text to be used by the next task
            return response.text
            
        except Exception as e:
            logger.error(f"Error calling webhook: {str(e)}", exc_info=True)
            raise AirflowException(f"Failed to call webhook: {str(e)}")
    
    @task(task_id='check_webhook_response')
    def check_response(webhook_response: str):
        """
        Process the response from the webhook.
        Raises an exception if the response contains an error status.
        """
        try:
            logger.info("Processing webhook response...")
            logger.debug(f"Raw response: {webhook_response}")
            
            response_json = json.loads(webhook_response)
            logger.info(f"Parsed JSON response: {json.dumps(response_json, indent=2)}")
            
            if response_json.get('status') == 'error':
                error_msg = f"Error in n8n webhook response: {response_json}"
                logger.error(error_msg)
                raise ValueError(error_msg)
                
            logger.info("Webhook call was successful")
            return "Success - No errors in the response"
            
        except json.JSONDecodeError as e:
            error_msg = f"Failed to parse JSON response: {str(e)}\nResponse: {webhook_response}"
            logger.error(error_msg)
            raise AirflowException(error_msg)
        except Exception as e:
            logger.error(f"Unexpected error processing response: {str(e)}", exc_info=True)
            raise
    
    # Call the tasks and define the execution flow
    webhook_response = call_webhook()
    check_response(webhook_response)

# Instantiate the DAG
n8n_webhook_btc_report_dag = n8n_webhook_btc_report_workflow()