import os
import pyodbc
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def load_fabric_secrets():
    secrets_path = '/fastbi/secrets/fabric/'
    required_secrets = [
        'FABRIC_SERVER',
        'FABRIC_PORT',
        'FABRIC_DATABASE',
        'FABRIC_USER',
        'FABRIC_PASSWORD'
    ]
    secrets = {}
    try:
        for secret_name in required_secrets:
            secret_path = os.path.join(secrets_path, secret_name)
            try:
                with open(secret_path, 'r') as f:
                    secrets[secret_name] = f.read().strip()
            except Exception as e:
                logger.error(f"Error reading secret {secret_name}: {e}")
                raise
        return secrets
    except Exception as e:
        logger.error(f"Error loading Fabric secrets: {e}")
        raise

def get_connection():
    secrets = load_fabric_secrets()
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={secrets['FABRIC_SERVER']},{secrets['FABRIC_PORT']};"
            f"DATABASE={secrets['FABRIC_DATABASE']};"
            f"UID={secrets['FABRIC_USER']};"
            f"PWD={secrets['FABRIC_PASSWORD']}"
        )
        conn = pyodbc.connect(conn_str)
        logger.info('Successfully connected to Fabric!')
        return conn
    except Exception as e:
        logger.error(f'Failed to connect to Fabric: {e}')
        raise

def run_query(query):
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            logger.debug(f'Executing query: {query.strip().splitlines()[0]}...')
            cur.execute(query)
            columns = [col[0] for col in cur.description]
            results = [dict(zip(columns, row)) for row in cur.fetchall()]
            logger.debug(f'Raw result: {results}')
            return results
        finally:
            conn.close()
    except Exception as e:
        logger.error(f'Error running query: {e}\nQuery: {query}')
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
        query = "SELECT COUNT(*) AS dataset_count FROM sys.databases"
        result = run_query(query)
        logger.debug(f'Raw result for dataset_count: {result}')
        if not result:
            return None
        return result[0].get('dataset_count') or result[0].get('DATASET_COUNT')
    except Exception as e:
        logger.error(f'Error in get_dataset_count: {e}')
        return None

def get_table_count():
    try:
        logger.info('Getting table count...')
        query = "SELECT COUNT(*) AS table_count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'"
        result = run_query(query)
        logger.debug(f'Raw result for table_count: {result}')
        if not result:
            return None
        return result[0].get('table_count') or result[0].get('TABLE_COUNT')
    except Exception as e:
        logger.error(f'Error in get_table_count: {e}')
        return None

def get_total_query_executed():
    try:
        logger.info('Getting total queries executed...')
        # Placeholder: Not all SQL Server/Fabric environments track query history by default
        return None
    except Exception as e:
        logger.error(f'Error in get_total_query_executed: {e}')
        return None

def get_avg_execution_time_seconds():
    try:
        logger.info('Getting average execution time (seconds)...')
        # Placeholder: Not all SQL Server/Fabric environments track query history by default
        return None
    except Exception as e:
        logger.error(f'Error in get_avg_execution_time_seconds: {e}')
        return None

def get_failure_rate_percentage():
    try:
        logger.info('Getting failure rate percentage...')
        # Placeholder: Not all SQL Server/Fabric environments track query history by default
        return None
    except Exception as e:
        logger.error(f'Error in get_failure_rate_percentage: {e}')
        return None

def get_query_cost_by_month():
    try:
        logger.info('Getting query cost by month...')
        # Placeholder: Not all SQL Server/Fabric environments track query history by default
        return json.dumps([])
    except Exception as e:
        logger.error(f'Error in get_query_cost_by_month: {e}')
        return json.dumps([])

def get_query_cost_for_last_30_days():
    try:
        logger.info('Getting query cost for last 30 days...')
        # Placeholder: Not all SQL Server/Fabric environments track query history by default
        return json.dumps([])
    except Exception as e:
        logger.error(f'Error in get_query_cost_for_last_30_days: {e}')
        return json.dumps([])

def get_total_cost_gb_by_users():
    try:
        logger.info('Getting total cost GB by users...')
        # Placeholder: Not all SQL Server/Fabric environments track query history by default
        return json.dumps([])
    except Exception as e:
        logger.error(f'Error in get_total_cost_gb_by_users: {e}')
        return json.dumps([])

def get_total_cost_gb_by_table():
    try:
        logger.info('Getting total cost GB by table...')
        query = """
        SELECT TABLE_SCHEMA, TABLE_NAME, SUM(CAST(reserved_page_count AS BIGINT)) * 8.0 / 1024 AS size_mb
        FROM sys.dm_db_partition_stats AS ps
        JOIN INFORMATION_SCHEMA.TABLES AS t ON OBJECT_NAME(ps.object_id) = t.TABLE_NAME
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        GROUP BY TABLE_SCHEMA, TABLE_NAME
        ORDER BY size_mb DESC
        """
        results = run_query(query)
        logger.debug(f'Raw result for total_cost_gb_by_table: {results}')
        results = convert_types(results)
        # Standardize output: dataset, table, and metrics
        transformed = []
        for row in results:
            dataset = row.get('TABLE_SCHEMA') or 'system'
            table = row.get('TABLE_NAME') or 'unknown'
            transformed.append({
                'dataset': dataset,
                'table': table,
                'size_mb': row.get('size_mb')
            })
        return json.dumps(transformed)
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
