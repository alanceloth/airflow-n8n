from datetime import datetime, timedelta
import logging
import sys
import traceback
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
import json
import requests

# Configuração de logging detalhada
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def log_response(response):
    """Log detalhado da resposta HTTP"""
    try:
        logger.info(f"=== HTTP Response ===")
        logger.info(f"Status Code: {response.status_code}")
        logger.info(f"Headers: {dict(response.headers)}")
        logger.info(f"Response Text: {response.text[:1000]}...")
        logger.info("=== End of HTTP Response ===")
    except Exception as e:
        logger.error(f"Error logging response: {str(e)}")
        logger.error(f"Response object type: {type(response)}")
        if hasattr(response, '__dict__'):
            logger.error(f"Response attributes: {vars(response)}")

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
        logger.info("Starting call_webhook task")
        
        try:
            # Get endpoint from Airflow Variables
            try:
                endpoint = Variable.get("N8N_WEBHOOK_ENDPOINT")
                logger.info(f"Using endpoint from Variable: {endpoint}")
            except Exception as e:
                logger.error(f"Failed to get N8N_WEBHOOK_ENDPOINT variable: {str(e)}")
                raise
            
            # Configurar a URL completa
            base_url = "https://jornada-de-dados.up.railway.app"
            full_url = f"{base_url}/{endpoint.lstrip('/')}"
            logger.info(f"Full URL: {full_url}")
            
            # Usar requests diretamente para ter mais controle
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "Airflow DAG"
            }
            
            data = json.dumps({"message": "BTC"})
            logger.info(f"Request data: {data}")
            
            # Fazer a requisição diretamente com requests
            logger.info("Sending HTTP POST request...")
            response = requests.post(
                url=full_url,
                data=data,
                headers=headers,
                timeout=30,
                verify=True
            )
            
            # Log detalhado da resposta
            log_response(response)
            
            # Verificar se a requisição foi bem-sucedida
            response.raise_for_status()
            
            logger.info("Request successful")
            return response.text
            
        except requests.exceptions.RequestException as e:
            error_msg = f"HTTP Request failed: {str(e)}"
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_msg += f"\nResponse status: {e.response.status_code}"
                    error_msg += f"\nResponse body: {e.response.text[:1000]}"
                except:
                    pass
            logger.error(error_msg, exc_info=True)
            raise AirflowException(error_msg)
            
        except Exception as e:
            error_msg = f"Unexpected error in call_webhook: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise AirflowException(error_msg)
    
    @task(task_id='check_webhook_response')
    def check_response(webhook_response: str):
        """
        Process the response from the webhook.
        Raises an exception if the response contains an error status.
        """
        logger.info("Starting check_response task")
        
        try:
            if not webhook_response or not webhook_response.strip():
                error_msg = "Empty or invalid webhook response received"
                logger.error(error_msg)
                raise AirflowException(error_msg)
                
            logger.info(f"Raw response length: {len(webhook_response)} characters")
            logger.debug(f"Raw response content: {webhook_response}")
            
            try:
                response_json = json.loads(webhook_response)
                logger.info("Successfully parsed JSON response")
                logger.debug(f"Parsed JSON: {json.dumps(response_json, indent=2, ensure_ascii=False)}")
                
                if isinstance(response_json, dict) and response_json.get('status') == 'error':
                    error_msg = f"Error in n8n webhook response: {response_json}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                    
                logger.info("Webhook call was successful")
                return "Success - No errors in the response"
                
            except json.JSONDecodeError as e:
                error_msg = (
                    f"Failed to parse JSON response. Error: {str(e)}\n"
                    f"Response (first 500 chars): {webhook_response[:500]}..."
                )
                logger.error(error_msg)
                raise AirflowException(error_msg)
                
        except Exception as e:
            error_msg = f"Error in check_response: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise AirflowException(error_msg)
    
    try:
        logger.info("Starting DAG execution")
        # Call the tasks and define the execution flow
        webhook_response = call_webhook()
        result = check_response(webhook_response)
        logger.info(f"DAG completed successfully. Result: {result}")
        return result
    except Exception as e:
        logger.error(f"DAG execution failed: {str(e)}\n{traceback.format_exc()}")
        raise

# Instantiate the DAG
n8n_webhook_btc_report_dag = n8n_webhook_btc_report_workflow()