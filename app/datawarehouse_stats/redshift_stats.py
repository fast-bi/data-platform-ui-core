import os
import psycopg2
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def load_redshift_secrets():
    # First try environment variables
    secrets = {
        'REDSHIFT_DATABASE': os.getenv('REDSHIFT_DATABASE'),
        'REDSHIFT_USER': os.getenv('REDSHIFT_USER'),
        'REDSHIFT_PASSWORD': os.getenv('REDSHIFT_PASSWORD'),
        'REDSHIFT_HOST': os.getenv('REDSHIFT_HOST'),
        'REDSHIFT_PORT': os.getenv('REDSHIFT_PORT')
    }
    
    # If any secret is missing from env vars, try reading from files
    if not all(secrets.values()):
        secrets_path = '/fastbi/secrets/redshift/'
        required_secrets = [
            'REDSHIFT_DATABASE',
            'REDSHIFT_USER',
            'REDSHIFT_PASSWORD',
            'REDSHIFT_HOST',
            'REDSHIFT_PORT'
        ]
        try:
            for secret_name in required_secrets:
                if not secrets.get(secret_name):
                    secret_path = os.path.join(secrets_path, secret_name)
                    try:
                        with open(secret_path, 'r') as f:
                            secrets[secret_name] = f.read().strip()
                    except Exception as e:
                        logger.error(f"Error reading secret {secret_name}: {e}")
                        raise
        except Exception as e:
            logger.error(f"Error loading Redshift secrets: {e}")
            raise
    
    # Validate all secrets are present
    missing_secrets = [k for k, v in secrets.items() if not v]
    if missing_secrets:
        raise ValueError(f"Missing required secrets: {', '.join(missing_secrets)}")
    
    return secrets

def get_connection():
    secrets = load_redshift_secrets()
    try:
        conn = psycopg2.connect(
            dbname=secrets['REDSHIFT_DATABASE'],
            user=secrets['REDSHIFT_USER'],
            password=secrets['REDSHIFT_PASSWORD'],
            host=secrets['REDSHIFT_HOST'],
            port=secrets['REDSHIFT_PORT']
        )
        logger.info('Successfully connected to Redshift!')
        return conn
    except Exception as e:
        logger.error(f'Failed to connect to Redshift: {e}')
        raise

def run_query(query):
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            logger.debug(f'Executing query: {query.strip().splitlines()[0]}...')
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            results = [dict(zip(columns, row)) for row in cur.fetchall()]
            logger.debug(f'Raw result: {results}')
            return results
        finally:
            conn.close()
    except Exception as e:
        logger.error(f'Error running query: {e}\nQuery: {query}')
        return []

def get_dataset_count():
    try:
        logger.info('Getting dataset count...')
        query = "SELECT COUNT(DISTINCT datname) AS dataset_count FROM pg_database"
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
        query = "SELECT COUNT(*) AS table_count FROM svv_tables WHERE table_type='BASE TABLE'"
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
        query = """
        SELECT COUNT(*) AS total_queries_executed 
        FROM SYS_QUERY_HISTORY 
        WHERE status != 'failed'
        """
        result = run_query(query)
        if not result:
            return None
        return result[0].get('total_queries_executed') or result[0].get('TOTAL_QUERIES_EXECUTED')
    except Exception as e:
        logger.error(f'Error in get_total_query_executed: {e}')
        return None

def get_avg_execution_time_seconds():
    try:
        logger.info('Getting average execution time (seconds)...')
        query = """
        SELECT ROUND(AVG(execution_time/1000000.0), 2) AS avg_execution_time_seconds
        FROM SYS_QUERY_HISTORY
        WHERE status != 'failed'
        """
        result = run_query(query)
        if not result:
            return None
        return result[0].get('avg_execution_time_seconds') or result[0].get('AVG_EXECUTION_TIME_SECONDS')
    except Exception as e:
        logger.error(f'Error in get_avg_execution_time_seconds: {e}')
        return None

def get_failure_rate_percentage():
    try:
        logger.info('Getting failure rate percentage...')
        query = """
        SELECT ROUND(100.0 * SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) / 
            NULLIF(COUNT(*), 0), 2) AS query_failure_rate_percentage
        FROM SYS_QUERY_HISTORY
        """
        result = run_query(query)
        if not result:
            return None
        return result[0].get('query_failure_rate_percentage') or result[0].get('QUERY_FAILURE_RATE_PERCENTAGE')
    except Exception as e:
        logger.error(f'Error in get_failure_rate_percentage: {e}')
        return None

def get_query_cost_by_month():
    try:
        logger.info('Getting query cost by month...')
        query = """
        WITH monthly_queries AS (
            SELECT 
                TO_CHAR(DATE_TRUNC('month', start_time), 'YYYY-MM') AS month,
                COUNT(*) AS query_count,
                CAST(SUM(returned_bytes) AS FLOAT) / 1073741824.0 AS query_cost_gb
            FROM SYS_QUERY_HISTORY
            WHERE start_time >= dateadd(month, -6, current_date)
            GROUP BY month
        ),
        table_sizes AS (
            SELECT 
                CAST(SUM(size) AS FLOAT) / 1024.0 AS total_table_size_mb  -- Convert 1MB blocks to MB
            FROM SVV_TABLE_INFO
            WHERE "table" NOT LIKE 'pg_%'  -- Exclude system tables
        )
        SELECT 
            m.month,
            m.query_count,
            m.query_cost_gb,
            COALESCE(t.total_table_size_mb / 1024.0, 0) AS total_table_size_gb  -- Convert MB to GB
        FROM monthly_queries m
        CROSS JOIN table_sizes t
        ORDER BY month
        """
        result = run_query(query)
        transformed = [
            {
                'month': row.get('month') or row.get('MONTH'),
                'query_count': int(row.get('query_count') or row.get('QUERY_COUNT') or 0),
                'query_cost_gb': float(row.get('query_cost_gb') or row.get('QUERY_COST_GB') or 0),
                'total_table_size_gb': float(row.get('total_table_size_gb') or row.get('TOTAL_TABLE_SIZE_GB') or 0),
                'total_cost_gb': float(row.get('query_cost_gb') or row.get('QUERY_COST_GB') or 0) + 
                               float(row.get('total_table_size_gb') or row.get('TOTAL_TABLE_SIZE_GB') or 0)
            }
            for row in result
        ]
        return json.dumps(transformed)
    except Exception as e:
        logger.error(f'Error in get_query_cost_by_month: {e}')
        return json.dumps([])

def get_query_cost_for_last_30_days():
    try:
        logger.info('Getting query cost for last 30 days...')
        query = """
        WITH daily_queries AS (
            SELECT 
                TO_CHAR(DATE_TRUNC('day', start_time), 'YYYY-MM-DD') AS day,
                COUNT(*) AS query_count,
                CAST(SUM(returned_bytes) AS FLOAT) / 1073741824.0 AS query_cost_gb
            FROM SYS_QUERY_HISTORY
            WHERE start_time >= dateadd(day, -30, current_date)
            GROUP BY day
        ),
        table_sizes AS (
            SELECT 
                CAST(SUM(size) AS FLOAT) / 1024.0 AS total_table_size_mb  -- Convert 1MB blocks to MB
            FROM SVV_TABLE_INFO
            WHERE "table" NOT LIKE 'pg_%'  -- Exclude system tables
        )
        SELECT 
            d.day,
            d.query_count,
            d.query_cost_gb,
            COALESCE(t.total_table_size_mb / 1024.0, 0) AS total_table_size_gb  -- Convert MB to GB
        FROM daily_queries d
        CROSS JOIN table_sizes t
        ORDER BY day
        """
        result = run_query(query)
        transformed = [
            {
                'day': row.get('day') or row.get('DAY'),
                'query_count': int(row.get('query_count') or row.get('QUERY_COUNT') or 0),
                'query_cost_gb': float(row.get('query_cost_gb') or row.get('QUERY_COST_GB') or 0),
                'total_table_size_gb': float(row.get('total_table_size_gb') or row.get('TOTAL_TABLE_SIZE_GB') or 0),
                'total_cost_gb': float(row.get('query_cost_gb') or row.get('QUERY_COST_GB') or 0) + 
                               float(row.get('total_table_size_gb') or row.get('TOTAL_TABLE_SIZE_GB') or 0)
            }
            for row in result
        ]
        return json.dumps(transformed)
    except Exception as e:
        logger.error(f'Error in get_query_cost_for_last_30_days: {e}')
        return json.dumps([])

def get_total_cost_gb_by_users():
    try:
        logger.info('Getting total cost GB by users...')
        query = """
        WITH user_queries AS (
            SELECT 
                username as user_email,
                COUNT(*) AS total_queries,
                TO_CHAR(MIN(start_time), 'YYYY-MM-DD HH24:MI:SS') AS first_query_date,
                TO_CHAR(MAX(start_time), 'YYYY-MM-DD HH24:MI:SS') AS last_query_date,
                ROUND(AVG(execution_time/1000000.0), 2) AS avg_execution_time_sec,
                SUM(CASE WHEN status != 'failed' THEN 1 ELSE 0 END) AS success_count,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failure_count,
                CAST(SUM(returned_bytes) AS FLOAT) / 1073741824.0 AS total_cost_gb
            FROM SYS_QUERY_HISTORY
            GROUP BY username
        )
        SELECT 
            user_email,
            total_cost_gb,
            total_queries,
            CASE 
                WHEN total_queries > 0 THEN 
                    ROUND(total_cost_gb / total_queries, 4)
                ELSE 0 
            END AS avg_query_cost_gb,
            first_query_date,
            last_query_date,
            ROUND(total_cost_gb * 60, 2) AS total_execution_time_min,
            avg_execution_time_sec,
            success_count,
            failure_count
        FROM user_queries
        ORDER BY total_cost_gb DESC
        LIMIT 15
        """
        results = run_query(query)
        transformed = [
            {
                'user_email': (row.get('user_email') or row.get('USER_EMAIL') or '').strip(),
                'total_cost_gb': float(row.get('total_cost_gb') or row.get('TOTAL_COST_GB') or 0),
                'total_queries': int(row.get('total_queries') or row.get('TOTAL_QUERIES') or 0),
                'avg_query_cost_gb': float(row.get('avg_query_cost_gb') or row.get('AVG_QUERY_COST_GB') or 0),
                'first_query_date': row.get('first_query_date') or row.get('FIRST_QUERY_DATE'),
                'last_query_date': row.get('last_query_date') or row.get('LAST_QUERY_DATE'),
                'total_execution_time_min': float(row.get('total_execution_time_min') or row.get('TOTAL_EXECUTION_TIME_MIN') or 0),
                'avg_execution_time_sec': float(row.get('avg_execution_time_sec') or row.get('AVG_EXECUTION_TIME_SEC') or 0),
                'success_count': int(row.get('success_count') or row.get('SUCCESS_COUNT') or 0),
                'failure_count': int(row.get('failure_count') or row.get('FAILURE_COUNT') or 0)
            }
            for row in results
        ]
        return json.dumps(transformed)
    except Exception as e:
        logger.error(f'Error in get_total_cost_gb_by_users: {e}')
        return json.dumps([])

def get_total_cost_gb_by_table():
    try:
        logger.info('Getting total cost GB by table...')
        query = """
        WITH table_stats AS (
            SELECT 
                h.database_name as dataset,
                REGEXP_REPLACE(REGEXP_REPLACE(d.table_name, '^[^.]+\\.', ''), '^[^.]+\\.', '') as table_name,
                SUM(h.returned_bytes) / 1073741824.0 AS total_cost_gb,
                COUNT(*) AS total_queries,
                TO_CHAR(MIN(h.start_time), 'YYYY-MM-DD HH24:MI:SS') AS first_query_date,
                TO_CHAR(MAX(h.start_time), 'YYYY-MM-DD HH24:MI:SS') AS last_query_date,
                ROUND(AVG(h.execution_time/1000000.0), 2) AS avg_execution_time_sec,
                SUM(CASE WHEN h.status != 'failed' THEN 1 ELSE 0 END) AS success_count,
                SUM(CASE WHEN h.status = 'failed' THEN 1 ELSE 0 END) AS failure_count
            FROM SYS_QUERY_HISTORY h
            JOIN SYS_QUERY_DETAIL d ON h.query_id = d.query_id
            WHERE d.table_name IS NOT NULL
            GROUP BY h.database_name, REGEXP_REPLACE(REGEXP_REPLACE(d.table_name, '^[^.]+\\.', ''), '^[^.]+\\.', '')
        )
        SELECT 
            dataset,
            table_name as table,
            total_cost_gb,
            total_queries,
            CASE 
                WHEN total_queries > 0 THEN 
                    ROUND(total_cost_gb / total_queries, 4)
                ELSE 0 
            END AS avg_query_cost_gb,
            first_query_date,
            last_query_date,
            ROUND(total_cost_gb * 60, 2) AS total_execution_time_min,
            avg_execution_time_sec,
            success_count,
            failure_count
        FROM table_stats
        WHERE table_name IS NOT NULL
        ORDER BY total_cost_gb DESC
        LIMIT 50
        """
        results = run_query(query)
        
        transformed = []
        seen_tables = set()  # Track unique table names
        
        for row in results:
            dataset = row.get('dataset') or row.get('DATASET') or 'system'
            table = row.get('table') or row.get('TABLE')
            
            # Handle empty table names as temporary queries
            if not table or table.strip() == '':
                table = f"temporary_query_{dataset}"
            
            # Clean up table name
            original_table = table
            table = table.replace('$', '').replace('..', '.').strip('.')
            if table.startswith('dev.'):
                table = table[4:]
            if table.startswith('raw_sys_'):
                table = table[8:]
            # Remove any remaining schema prefixes
            if '.' in table:
                table = table.split('.')[-1]
            
            # Skip if we've seen this table before
            table_key = f"{dataset}.{table}"
            if table_key in seen_tables:
                continue
            seen_tables.add(table_key)
            
            # Convert Decimal values to float
            def convert_decimal(value):
                if isinstance(value, (int, float)):
                    return value
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return value

            transformed_row = {
                'dataset': dataset,
                'table': table,
                'total_cost_gb': convert_decimal(row.get('total_cost_gb') or row.get('TOTAL_COST_GB') or 0),
                'total_queries': int(row.get('total_queries') or row.get('TOTAL_QUERIES') or 0),
                'avg_query_cost_gb': convert_decimal(row.get('avg_query_cost_gb') or row.get('AVG_QUERY_COST_GB') or 0),
                'first_query_date': row.get('first_query_date') or row.get('FIRST_QUERY_DATE'),
                'last_query_date': row.get('last_query_date') or row.get('LAST_QUERY_DATE'),
                'total_execution_time_min': convert_decimal(row.get('total_execution_time_min') or row.get('TOTAL_EXECUTION_TIME_MIN') or 0),
                'avg_execution_time_sec': convert_decimal(row.get('avg_execution_time_sec') or row.get('AVG_EXECUTION_TIME_SEC') or 0),
                'success_count': int(row.get('success_count') or row.get('SUCCESS_COUNT') or 0),
                'failure_count': int(row.get('failure_count') or row.get('FAILURE_COUNT') or 0)
            }
            transformed.append(transformed_row)
        
        logger.info(f'Processed {len(transformed)} tables')
        return json.dumps(transformed, default=str)
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
