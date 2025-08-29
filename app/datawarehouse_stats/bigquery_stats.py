import base64
import json
import os
import logging
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth import impersonated_credentials
from google.api_core.exceptions import GoogleAPIError
# from app.config import Config  # No longer needed for secrets

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def read_bigquery_secret(secret_name):
    secrets_path = '/fastbi/secrets/bigquery/'
    # Local Development
    #secrets_path = './secrets/bigquery/'
    required_secrets = [
        'BIGQUERY_PROJECT_ID',
        'BIGQUERY_REGION',
        'DBT_DEPLOY_GCP_SA_SECRET'
    ]
    try:
        if secret_name not in required_secrets:
            logger.error(f"Requested secret {secret_name} is not in the required secrets list")
            raise ValueError(f"Invalid secret name: {secret_name}")
            
        secret_path = os.path.join(secrets_path, secret_name)
        try:
            with open(secret_path, 'r') as f:
                return f.read().strip()
        except Exception as e:
            logger.error(f"Error reading secret {secret_name}: {e}")
            raise
    except Exception as e:
        logger.error(f"Error loading BigQuery secret: {e}")
        raise

def get_bq_project_id():
    return read_bigquery_secret('BIGQUERY_PROJECT_ID')

def get_bq_region():
    return read_bigquery_secret('BIGQUERY_REGION')

def get_gcp_sa_secret():
    return read_bigquery_secret('DBT_DEPLOY_GCP_SA_SECRET')

def decode_base64_sa(encoded_str):
    decoded_bytes = base64.b64decode(encoded_str)
    decoded_str = decoded_bytes.decode('utf-8')
    try:
        decoded_dict = json.loads(decoded_str)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON: {e}")
        raise ValueError(f"Failed to decode JSON: {e}")
    return decoded_dict

def handle_ssl_issues():
    ssl_vars = {
        'SSL_CERT_FILE': os.getenv('SSL_CERT_FILE'),
        'REQUESTS_CA_BUNDLE': os.getenv('REQUESTS_CA_BUNDLE')
    }
    if any(ssl_vars.values()):
        logger.info("SSL-related environment variables detected. Handling SSL configuration...")
        original_values = ssl_vars.copy()
        os.environ['SSL_CERT_FILE'] = ''
        os.environ['REQUESTS_CA_BUNDLE'] = ''
        return original_values
    return None

def restore_ssl_settings(original_values):
    if original_values:
        for key, value in original_values.items():
            if value is not None:
                os.environ[key] = value
            else:
                os.environ.pop(key, None)

def run_bigquery_query(query):
    try:
        logger.info('Preparing to connect to BigQuery...')
        project_id = get_bq_project_id()
        region = get_bq_region()
        encoded_sa = get_gcp_sa_secret()
        decoded_sa = decode_base64_sa(encoded_sa)
        scopes = [
            'https://www.googleapis.com/auth/bigquery',
            'https://www.googleapis.com/auth/cloud-platform',
            'https://www.googleapis.com/auth/iam'
        ]
        source_credentials = service_account.Credentials.from_service_account_info(
            decoded_sa,
            scopes=scopes
        )
        impersonate_email = os.environ.get('GCP_SA_IMPERSONATE_EMAIL')
        if impersonate_email:
            credentials = impersonated_credentials.Credentials(
                source_credentials=source_credentials,
                target_principal=impersonate_email,
                target_scopes=scopes,
                lifetime=3600
            )
        else:
            credentials = source_credentials
        original_ssl_settings = handle_ssl_issues()
        try:
            logger.info('Successfully connected to BigQuery!')
            client = bigquery.Client(
                project=project_id,
                credentials=credentials
            )
            location = f"{project_id}.region-{region}"
            query = query.replace("{location}", location)
            logger.info(f'Executing query: {query.strip().splitlines()[0]}...')
            query_job = client.query(query)
            result = query_job.result()
            results = [dict(row) for row in result]
            logger.info(f'Raw result: {results}')
            return results
        finally:
            restore_ssl_settings(original_ssl_settings)
    except GoogleAPIError as e:
        logger.error(f"An error occurred while querying BigQuery: {e}")
        return []
    except Exception as ex:
        logger.error(f"An unexpected error occurred: {ex}")
        return []

def convert_types(obj):
    if isinstance(obj, list):
        return [convert_types(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_types(v) for k, v in obj.items()}
    elif isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    else:
        return obj

def get_dataset_count():
    try:
        logger.info('Getting dataset count...')
        query = """
        SELECT COUNT(*) AS dataset_count FROM `{location}.INFORMATION_SCHEMA.SCHEMATA`;
        """
        results = run_bigquery_query(query)
        logger.debug(f'Raw result for dataset_count: {results}')
        if not results:
            return None
        return results[0].get('dataset_count') or results[0].get('DATASET_COUNT')
    except Exception as e:
        logger.error(f'Error in get_dataset_count: {e}')
        return None

def get_table_count():
    try:
        logger.info('Getting table count...')
        query = """
        SELECT COUNT(*) AS table_count FROM `{location}.INFORMATION_SCHEMA.TABLES`;
        """
        results = run_bigquery_query(query)
        logger.debug(f'Raw result for table_count: {results}')
        if not results:
            return None
        return results[0].get('table_count') or results[0].get('TABLE_COUNT')
    except Exception as e:
        logger.error(f'Error in get_table_count: {e}')
        return None

def get_all_query_gb_cost():
    query = """
    SELECT
        ROUND(SUM(total_bytes_billed) / 1073741824, 2) AS total_query_cost_gb
    FROM
        `{location}.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
    WHERE
        job_type = 'QUERY';    
    """
    results = run_bigquery_query(query)
    result = results[0]['total_query_cost_gb']
    return result

def get_total_query_executed():
    try:
        logger.info('Getting total queries executed...')
        query = """
            SELECT COUNT(*) AS total_queries_executed FROM `{location}.INFORMATION_SCHEMA.JOBS_BY_PROJECT` WHERE job_type = 'QUERY'
        """
        results = run_bigquery_query(query)
        logger.debug(f'Raw result for total_query_executed: {results}')
        if not results:
            return None
        return results[0].get('total_queries_executed') or results[0].get('TOTAL_QUERIES_EXECUTED')
    except Exception as e:
        logger.error(f'Error in get_total_query_executed: {e}')
        return None

def get_avg_execution_time_seconds():
    try:
        logger.info('Getting average execution time (seconds)...')
        query = """
        SELECT ROUND(AVG(TIMESTAMP_DIFF(end_time, start_time, SECOND)), 2) AS avg_execution_time_seconds FROM `{location}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT WHERE job_type = 'QUERY' AND end_time IS NOT NULL;
        """
        results = run_bigquery_query(query)
        logger.debug(f'Raw result for avg_execution_time_seconds: {results}')
        if not results:
            return None
        return results[0].get('avg_execution_time_seconds') or results[0].get('AVG_EXECUTION_TIME_SECONDS')
    except Exception as e:
        logger.error(f'Error in get_avg_execution_time_seconds: {e}')
        return None

def get_query_cost_by_month():
    try:
        logger.info('Getting query cost by month...')
        query = """
        SELECT FORMAT_TIMESTAMP('%Y-%m', creation_time) AS month, COUNT(*) AS query_count, ROUND(SUM(total_bytes_billed) / 1073741824, 2) AS total_cost_gb FROM `{location}.INFORMATION_SCHEMA.JOBS_BY_PROJECT` WHERE job_type = 'QUERY' AND DATE(creation_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) GROUP BY month ORDER BY month
        """
        results = run_bigquery_query(query)
        logger.debug(f'Raw result for query_cost_by_month: {results}')
        return json.dumps(convert_types(results))
    except Exception as e:
        logger.error(f'Error in get_query_cost_by_month: {e}')
        return json.dumps([])

def get_query_cost_for_last_30_days():
    try:
        logger.info('Getting query cost for last 30 days...')
        query = """
        SELECT FORMAT_TIMESTAMP('%Y-%m-%d', creation_time, 'UTC') AS day, COUNT(*) AS query_count, ROUND(SUM(total_bytes_billed) / 1073741824, 2) AS total_cost_gb FROM `{location}.INFORMATION_SCHEMA.JOBS_BY_PROJECT` WHERE job_type = 'QUERY' AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) GROUP BY day ORDER BY day
        """
        results = run_bigquery_query(query)
        logger.debug(f'Raw result for query_cost_for_last_30_days: {results}')
        return json.dumps(convert_types(results))
    except Exception as e:
        logger.error(f'Error in get_query_cost_for_last_30_days: {e}')
        return json.dumps([])

def get_failure_rate_percentage():
    try:
        logger.info('Getting failure rate percentage...')
        query = """
        SELECT ROUND(100 * COUNTIF(error_result IS NOT NULL) / COUNT(*), 2) AS query_failure_rate_percentage FROM `{location}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT WHERE job_type = 'QUERY'
        """
        results = run_bigquery_query(query)
        logger.debug(f'Raw result for failure_rate_percentage: {results}')
        if not results:
            return None
        return results[0].get('query_failure_rate_percentage') or results[0].get('QUERY_FAILURE_RATE_PERCENTAGE')
    except Exception as e:
        logger.error(f'Error in get_failure_rate_percentage: {e}')
        return None

def get_total_cost_gb_by_users():
    try:
        logger.info('Getting total cost GB by users...')
        query = """
        SELECT user_email, ROUND(SUM(total_bytes_billed) / 1073741824, 2) AS total_cost_gb, COUNT(job_id) AS total_queries, ROUND(AVG(total_bytes_billed) / 1073741824, 2) AS avg_query_cost_gb, MIN(creation_time) AS first_query_date, MAX(creation_time) AS last_query_date, ROUND(SUM(TIMESTAMP_DIFF(end_time, start_time, SECOND)) / 60, 2) AS total_execution_time_min, ROUND(AVG(TIMESTAMP_DIFF(end_time, start_time, SECOND)), 2) AS avg_execution_time_sec, SUM(CASE WHEN state = 'DONE' THEN 1 ELSE 0 END) AS success_count, SUM(CASE WHEN error_result is not null THEN 1 ELSE 0 END) AS failure_count FROM `{location}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT WHERE job_type = 'QUERY' GROUP BY user_email ORDER BY total_cost_gb DESC LIMIT 15
        """
        results = run_bigquery_query(query)
        logger.debug(f'Raw result for total_cost_gb_by_users: {results}')
        results = convert_types(results)
        return json.dumps(results)
    except Exception as e:
        logger.error(f'Error in get_total_cost_gb_by_users: {e}')
        return json.dumps([])

def get_total_cost_gb_by_table():
    try:
        logger.info('Getting total cost GB by table...')
        query = """
        SELECT destination_table.dataset_id AS dataset, destination_table.table_id AS table, ROUND(SUM(total_bytes_billed) / 1073741824, 2) AS total_cost_gb, COUNT(job_id) AS total_queries, ROUND(AVG(total_bytes_billed) / 1073741824, 2) AS avg_query_cost_gb, MIN(p.creation_time) AS first_query_date, MAX(p.creation_time) AS last_query_date, ROUND(SUM(TIMESTAMP_DIFF(end_time, start_time, SECOND)) / 60, 2) AS total_execution_time_min, ROUND(AVG(TIMESTAMP_DIFF(end_time, start_time, SECOND)), 2) AS avg_execution_time_sec, SUM(CASE WHEN state = 'DONE' THEN 1 ELSE 0 END) AS success_count, SUM(CASE WHEN error_result is not null THEN 1 ELSE 0 END) AS failure_count FROM `{location}.INFORMATION_SCHEMA.JOBS_BY_PROJECT` p JOIN `{location}.INFORMATION_SCHEMA.TABLES` t on t.table_schema =p.destination_table.dataset_id and t.table_name = p.destination_table.table_id WHERE t.table_type = 'BASE TABLE' AND job_type = 'QUERY' AND destination_table.dataset_id IS NOT NULL AND destination_table.table_id IS NOT NULL GROUP BY dataset, table ORDER BY total_cost_gb desc
        """
        results = run_bigquery_query(query)
        logger.debug(f'Raw result for total_cost_gb_by_table: {results}')
        results = convert_types(results)
        return json.dumps(results)
    except Exception as e:
        logger.error(f'Error in get_total_cost_gb_by_table: {e}')
        return json.dumps([])

# Debug Local testing
# if __name__ == "__main__":
#     try:
#         print("Dataset count:", get_dataset_count())
#         print("Table count:", get_table_count())
#         print("Total queries executed:", get_total_query_executed())
#         print("Average execution time (seconds):", get_avg_execution_time_seconds())
#         print("Failure rate percentage:", get_failure_rate_percentage())
#         print("Query cost by months chart:", get_query_cost_by_month())
#         print("Query cost by days chart:", get_query_cost_for_last_30_days())
#         print("Total cost GB by users:", get_total_cost_gb_by_users())
#         print("Total cost GB by table:", get_total_cost_gb_by_table())
#     except Exception as e:
#         logger.error(f'Error in main: {e}') 