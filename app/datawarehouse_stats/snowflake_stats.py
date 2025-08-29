import os
import snowflake.connector
import json
from datetime import datetime
import logging
from decimal import Decimal
from contextlib import contextmanager

# Disable Snowflake connector logging
snowflake.connector.logging.getLogger().setLevel(logging.WARNING)
logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

class SnowflakeConnection:
    _instance = None
    _connection = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self.secrets = self._load_secrets()
        self._connect()

    def _load_secrets(self):
        secrets = {
            'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT'),
            'SNOWFLAKE_DATABASE': os.getenv('SNOWFLAKE_DATABASE'),
            'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER'),
            'SNOWFLAKE_PASSWORD': os.getenv('SNOWFLAKE_PASSWORD'),
            'SNOWFLAKE_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE')
        }
        
        if not all(secrets.values()):
            secrets_path = '/fastbi/secrets/snowflake/'
            required_secrets = [
                'SNOWFLAKE_ACCOUNT',
                'SNOWFLAKE_DATABASE',
                'SNOWFLAKE_USER',
                'SNOWFLAKE_PASSWORD',
                'SNOWFLAKE_WAREHOUSE'
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
                logger.error(f"Error loading Snowflake secrets: {e}")
                raise
        
        missing_secrets = [k for k, v in secrets.items() if not v]
        if missing_secrets:
            raise ValueError(f"Missing required secrets: {', '.join(missing_secrets)}")
        
        return secrets

    def _connect(self):
        try:
            self._connection = snowflake.connector.connect(
                user=self.secrets['SNOWFLAKE_USER'],
                password=self.secrets.get('SNOWFLAKE_PASSWORD'),
                account=self.secrets['SNOWFLAKE_ACCOUNT'],
                warehouse=self.secrets['SNOWFLAKE_WAREHOUSE'],
                database=self.secrets['SNOWFLAKE_DATABASE'],
            )
            # Switch to ACCOUNTADMIN role for storage metrics
            self._connection.cursor().execute("USE ROLE ACCOUNTADMIN;")
        except Exception as e:
            logger.error(f'Failed to connect to Snowflake: {e}')
            raise

    def get_connection(self):
        if not self._connection or self._connection.is_closed():
            self._connect()
        return self._connection

    def execute_queries(self, queries):
        """Execute multiple queries in a single connection"""
        try:
            conn = self.get_connection()
            results = {}
            cur = conn.cursor()
            for name, query in queries.items():
                cur.execute(query)
                columns = [col[0] for col in cur.description]
                results[name] = [dict(zip(columns, row)) for row in cur.fetchall()]
            return {k: normalize_keys(v) for k, v in results.items()}
        except Exception as e:
            logger.error(f'Error executing queries: {e}')
            return {}

def normalize_keys(results):
    """Convert all dictionary keys to uppercase for consistent access."""
    if isinstance(results, list):
        return [normalize_keys(r) for r in results]
    elif isinstance(results, dict):
        return {k.upper(): v for k, v in results.items()}
    return results

def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj

def get_all_stats():
    """Get all statistics in a single connection"""
    conn = SnowflakeConnection.get_instance()
    
    # Get database name for table costs query
    database = conn.secrets['SNOWFLAKE_DATABASE']
    
    # Create the table costs query with proper database filtering - matching restore.py
    table_costs_query = f"""
        SELECT
            CASE
                WHEN REGEXP_SUBSTR(QUERY_TEXT, '\\\\bFROM\\\\s+{database}\\\\.([\\\\w\\\\.]+)', 1, 1, 'i', 1) IS NULL THEN 'temporary_table'
                ELSE REGEXP_SUBSTR(QUERY_TEXT, '\\\\bFROM\\\\s+{database}\\\\.([\\\\w\\\\.]+)', 1, 1, 'i', 1)
            END AS table_name,
            COUNT(*) AS total_queries,
            SUM(bytes_scanned) / 1073741824 AS total_cost_gb,
            AVG(bytes_scanned) / 1073741824 AS avg_query_cost_gb,
            MIN(start_time) AS first_query_date,
            MAX(start_time) AS last_query_date,
            SUM(DATEDIFF('second', start_time, end_time)) / 60 AS total_execution_time_min,
            ROUND(AVG(DATEDIFF('second', start_time, end_time)), 2) AS avg_execution_time_sec,
            SUM(CASE WHEN error_code IS NULL THEN 1 ELSE 0 END) AS success_count,
            SUM(CASE WHEN error_code IS NOT NULL THEN 1 ELSE 0 END) AS failure_count
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE QUERY_TEXT ILIKE '%FROM%{database}.%' AND bytes_scanned IS NOT NULL
        GROUP BY table_name
        ORDER BY total_cost_gb DESC
    """
    
    queries = {
        'basic_metrics': """
            SELECT 
                (SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA) AS dataset_count,
                (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES) AS table_count,
                (SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY) AS total_queries_executed,
                (SELECT ROUND(AVG(DATEDIFF('second', start_time, end_time)), 2) 
                 FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY) AS avg_execution_time_seconds,
                (SELECT ROUND(100 * COUNT_IF(error_code IS NOT NULL) / COUNT(*), 2) 
                 FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY) AS query_failure_rate_percentage
        """,
        'monthly_costs': """
            WITH monthly_queries AS (
                SELECT 
                    TO_VARCHAR(DATE_TRUNC('month', start_time), 'YYYY-MM') AS month,
                    COUNT(*) AS query_count,
                    SUM(bytes_scanned) / 1073741824.0 AS query_cost_gb
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE start_time >= DATEADD(month, -6, CURRENT_DATE())
                GROUP BY month
            ),
            storage_metrics AS (
                SELECT 
                    SUM(((ACTIVE_BYTES + TIME_TRAVEL_BYTES + FAILSAFE_BYTES + RETAINED_FOR_CLONE_BYTES) / 1024)/1024)/1024 AS total_storage_gb
                FROM "INFORMATION_SCHEMA".TABLE_STORAGE_METRICS
                WHERE TABLE_CATALOG = CURRENT_DATABASE()
            )
            SELECT 
                m.month,
                m.query_count,
                m.query_cost_gb,
                COALESCE(s.total_storage_gb, 0) AS storage_cost_gb,
                COALESCE(m.query_cost_gb, 0) + COALESCE(s.total_storage_gb, 0) AS total_cost_gb
            FROM monthly_queries m
            CROSS JOIN storage_metrics s
            ORDER BY month
        """,
        'daily_costs': """
            WITH daily_queries AS (
                SELECT 
                    TO_VARCHAR(DATE_TRUNC('day', start_time), 'YYYY-MM-DD') AS day,
                    COUNT(*) AS query_count,
                    SUM(bytes_scanned) / 1073741824.0 AS query_cost_gb
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE start_time >= DATEADD(day, -30, CURRENT_DATE())
                GROUP BY day
            ),
            storage_metrics AS (
                SELECT 
                    SUM(((ACTIVE_BYTES + TIME_TRAVEL_BYTES + FAILSAFE_BYTES + RETAINED_FOR_CLONE_BYTES) / 1024)/1024)/1024 AS total_storage_gb
                FROM "INFORMATION_SCHEMA".TABLE_STORAGE_METRICS
                WHERE TABLE_CATALOG = CURRENT_DATABASE()
            )
            SELECT 
                d.day,
                d.query_count,
                d.query_cost_gb,
                COALESCE(s.total_storage_gb, 0) AS storage_cost_gb,
                COALESCE(d.query_cost_gb, 0) + COALESCE(s.total_storage_gb, 0) AS total_cost_gb
            FROM daily_queries d
            CROSS JOIN storage_metrics s
            ORDER BY day
        """,
        'user_costs': """
            SELECT 
                USER_NAME as user_email,
                COUNT(*) AS total_queries,
                SUM(bytes_scanned) / 1073741824.0 AS total_cost_gb,
                TO_CHAR(MIN(start_time), 'YYYY-MM-DD HH24:MI:SS') AS first_query_date,
                TO_CHAR(MAX(start_time), 'YYYY-MM-DD HH24:MI:SS') AS last_query_date,
                ROUND(AVG(DATEDIFF('second', start_time, end_time)), 2) AS avg_execution_time_sec,
                SUM(CASE WHEN error_code IS NULL THEN 1 ELSE 0 END) AS success_count,
                SUM(CASE WHEN error_code IS NOT NULL THEN 1 ELSE 0 END) AS failure_count
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            GROUP BY user_email
            ORDER BY total_cost_gb DESC
            LIMIT 15
        """,
        'table_costs': table_costs_query
    }
    
    results = conn.execute_queries(queries)
    
    # Process results and convert decimals
    basic_metrics = convert_decimals(results.get('basic_metrics', [{}])[0])
    monthly_costs = convert_decimals(results.get('monthly_costs', []))
    daily_costs = convert_decimals(results.get('daily_costs', []))
    user_costs = convert_decimals(results.get('user_costs', []))
    table_costs = convert_decimals(results.get('table_costs', []))
    
    # Transform table costs to match restore.py format exactly
    transformed_table_costs = []
    for row in table_costs:
        table_name = row.get('TABLE_NAME')
        dataset = 'system'
        table = table_name
        if table_name:
            parts = table_name.split('.')
            if len(parts) == 2:
                dataset, table = parts
            elif len(parts) == 1:
                table = parts[0]
        transformed_table_costs.append({
            'dataset': dataset,
            'table': table,
            'total_cost_gb': row.get('TOTAL_COST_GB'),
            'total_queries': row.get('TOTAL_QUERIES'),
            'avg_query_cost_gb': row.get('AVG_QUERY_COST_GB'),
            'first_query_date': row.get('FIRST_QUERY_DATE'),
            'last_query_date': row.get('LAST_QUERY_DATE'),
            'total_execution_time_min': row.get('TOTAL_EXECUTION_TIME_MIN'),
            'avg_execution_time_sec': row.get('AVG_EXECUTION_TIME_SEC'),
            'success_count': row.get('SUCCESS_COUNT'),
            'failure_count': row.get('FAILURE_COUNT')
        })
    
    # Transform monthly and daily costs to match format
    transformed_monthly_costs = [{
        'month': row.get('MONTH'),
        'query_count': int(row.get('QUERY_COUNT', 0)),
        'total_cost_gb': float(row.get('TOTAL_COST_GB', 0))
    } for row in monthly_costs]
    
    transformed_daily_costs = [{
        'day': row.get('DAY'),
        'query_count': int(row.get('QUERY_COUNT', 0)),
        'total_cost_gb': float(row.get('TOTAL_COST_GB', 0))
    } for row in daily_costs]
    
    # Transform user costs to match format
    transformed_user_costs = []
    for row in user_costs:
        total_cost_gb = float(row.get('TOTAL_COST_GB', 0))
        total_queries = int(row.get('TOTAL_QUERIES', 0))
        avg_execution_time_sec = float(row.get('AVG_EXECUTION_TIME_SEC', 0))
        avg_query_cost_gb = total_cost_gb / total_queries if total_queries > 0 else 0
        total_execution_time_min = (avg_execution_time_sec * total_queries) / 60 if total_queries > 0 else 0
        transformed_user_costs.append({
            'user_email': row.get('USER_EMAIL'),
            'total_cost_gb': total_cost_gb,
            'total_queries': total_queries,
            'avg_query_cost_gb': avg_query_cost_gb,
            'first_query_date': row.get('FIRST_QUERY_DATE'),
            'last_query_date': row.get('LAST_QUERY_DATE'),
            'total_execution_time_min': total_execution_time_min,
            'avg_execution_time_sec': avg_execution_time_sec,
            'success_count': int(row.get('SUCCESS_COUNT', 0)),
            'failure_count': int(row.get('FAILURE_COUNT', 0))
        })
    
    return {
        'dataset_count': int(basic_metrics.get('DATASET_COUNT', 0)),
        'table_count': int(basic_metrics.get('TABLE_COUNT', 0)),
        'total_queries': int(basic_metrics.get('TOTAL_QUERIES_EXECUTED', 0)),
        'avg_execution_time': float(basic_metrics.get('AVG_EXECUTION_TIME_SECONDS', 0)),
        'failure_rate': float(basic_metrics.get('QUERY_FAILURE_RATE_PERCENTAGE', 0)),
        'monthly_costs': json.dumps(transformed_monthly_costs),
        'daily_costs': json.dumps(transformed_daily_costs),
        'user_costs': json.dumps(transformed_user_costs),
        'table_costs': json.dumps(transformed_table_costs)
    }

def get_dataset_count():
    try:
        query = "SELECT COUNT(*) AS dataset_count FROM INFORMATION_SCHEMA.SCHEMATA"
        conn = SnowflakeConnection.get_instance()
        results = conn.execute_queries({'dataset_count': query})
        if not results.get('dataset_count'):
            return None
        return int(results['dataset_count'][0].get('DATASET_COUNT', 0))
    except Exception as e:
        logger.error(f'Error in get_dataset_count: {e}')
        return None

def get_table_count():
    try:
        query = "SELECT COUNT(*) AS table_count FROM INFORMATION_SCHEMA.TABLES"
        conn = SnowflakeConnection.get_instance()
        results = conn.execute_queries({'table_count': query})
        if not results.get('table_count'):
            return None
        return int(results['table_count'][0].get('TABLE_COUNT', 0))
    except Exception as e:
        logger.error(f'Error in get_table_count: {e}')
        return None

def get_total_query_executed():
    try:
        query = "SELECT COUNT(*) AS total_queries_executed FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY"
        conn = SnowflakeConnection.get_instance()
        results = conn.execute_queries({'total_queries': query})
        if not results.get('total_queries'):
            return None
        return int(results['total_queries'][0].get('TOTAL_QUERIES_EXECUTED', 0))
    except Exception as e:
        logger.error(f'Error in get_total_query_executed: {e}')
        return None

def get_avg_execution_time_seconds():
    try:
        query = """
        SELECT ROUND(AVG(DATEDIFF('second', start_time, end_time)), 2) AS avg_execution_time_seconds
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE end_time IS NOT NULL
        """
        conn = SnowflakeConnection.get_instance()
        results = conn.execute_queries({'avg_time': query})
        if not results.get('avg_time'):
            return None
        return float(results['avg_time'][0].get('AVG_EXECUTION_TIME_SECONDS', 0))
    except Exception as e:
        logger.error(f'Error in get_avg_execution_time_seconds: {e}')
        return None

def get_failure_rate_percentage():
    try:
        query = """
        SELECT ROUND(100 * COUNT_IF(error_code IS NOT NULL) / COUNT(*), 2) AS query_failure_rate_percentage
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        """
        conn = SnowflakeConnection.get_instance()
        results = conn.execute_queries({'failure_rate': query})
        if not results.get('failure_rate'):
            return None
        return float(results['failure_rate'][0].get('QUERY_FAILURE_RATE_PERCENTAGE', 0))
    except Exception as e:
        logger.error(f'Error in get_failure_rate_percentage: {e}')
        return None

def get_query_cost_by_month():
    try:
        query = """
        WITH monthly_queries AS (
            SELECT 
                TO_VARCHAR(DATE_TRUNC('month', start_time), 'YYYY-MM') AS month,
                COUNT(*) AS query_count,
                SUM(bytes_scanned) / 1073741824.0 AS query_cost_gb
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE start_time >= DATEADD(month, -6, CURRENT_DATE())
            GROUP BY month
        ),
        storage_metrics AS (
            SELECT 
                SUM(((ACTIVE_BYTES + TIME_TRAVEL_BYTES + FAILSAFE_BYTES + RETAINED_FOR_CLONE_BYTES) / 1024)/1024)/1024 AS total_storage_gb
            FROM "INFORMATION_SCHEMA".TABLE_STORAGE_METRICS
            WHERE TABLE_CATALOG = CURRENT_DATABASE()
        )
        SELECT 
            m.month,
            m.query_count,
            m.query_cost_gb,
            COALESCE(s.total_storage_gb, 0) AS storage_cost_gb,
            COALESCE(m.query_cost_gb, 0) + COALESCE(s.total_storage_gb, 0) AS total_cost_gb
        FROM monthly_queries m
        CROSS JOIN storage_metrics s
        ORDER BY month
        """
        conn = SnowflakeConnection.get_instance()
        results = conn.execute_queries({'monthly_costs': query})
        monthly_costs = convert_decimals(results.get('monthly_costs', []))
        transformed = [{
            'month': row.get('MONTH'),
            'query_count': int(row.get('QUERY_COUNT', 0)),
            'total_cost_gb': float(row.get('TOTAL_COST_GB', 0))
        } for row in monthly_costs]
        return json.dumps(transformed)
    except Exception as e:
        logger.error(f'Error in get_query_cost_by_month: {e}')
        return json.dumps([])

def get_query_cost_for_last_30_days():
    try:
        query = """
        WITH daily_queries AS (
            SELECT 
                TO_VARCHAR(DATE_TRUNC('day', start_time), 'YYYY-MM-DD') AS day,
                COUNT(*) AS query_count,
                SUM(bytes_scanned) / 1073741824.0 AS query_cost_gb
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE start_time >= DATEADD(day, -30, CURRENT_DATE())
            GROUP BY day
        ),
        storage_metrics AS (
            SELECT 
                SUM(((ACTIVE_BYTES + TIME_TRAVEL_BYTES + FAILSAFE_BYTES + RETAINED_FOR_CLONE_BYTES) / 1024)/1024)/1024 AS total_storage_gb
            FROM "INFORMATION_SCHEMA".TABLE_STORAGE_METRICS
            WHERE TABLE_CATALOG = CURRENT_DATABASE()
        )
        SELECT 
            d.day,
            d.query_count,
            d.query_cost_gb,
            COALESCE(s.total_storage_gb, 0) AS storage_cost_gb,
            COALESCE(d.query_cost_gb, 0) + COALESCE(s.total_storage_gb, 0) AS total_cost_gb
        FROM daily_queries d
        CROSS JOIN storage_metrics s
        ORDER BY day
        """
        conn = SnowflakeConnection.get_instance()
        results = conn.execute_queries({'daily_costs': query})
        daily_costs = convert_decimals(results.get('daily_costs', []))
        transformed = [{
            'day': row.get('DAY'),
            'query_count': int(row.get('QUERY_COUNT', 0)),
            'total_cost_gb': float(row.get('TOTAL_COST_GB', 0))
        } for row in daily_costs]
        return json.dumps(transformed)
    except Exception as e:
        logger.error(f'Error in get_query_cost_for_last_30_days: {e}')
        return json.dumps([])

def get_total_cost_gb_by_users():
    try:
        query = """
        SELECT 
            USER_NAME as user_email,
            COUNT(*) AS total_queries,
            SUM(bytes_scanned) / 1073741824.0 AS total_cost_gb,
            TO_CHAR(MIN(start_time), 'YYYY-MM-DD HH24:MI:SS') AS first_query_date,
            TO_CHAR(MAX(start_time), 'YYYY-MM-DD HH24:MI:SS') AS last_query_date,
            ROUND(AVG(DATEDIFF('second', start_time, end_time)), 2) AS avg_execution_time_sec,
            SUM(CASE WHEN error_code IS NULL THEN 1 ELSE 0 END) AS success_count,
            SUM(CASE WHEN error_code IS NOT NULL THEN 1 ELSE 0 END) AS failure_count
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        GROUP BY user_email
        ORDER BY total_cost_gb DESC
        LIMIT 15
        """
        conn = SnowflakeConnection.get_instance()
        results = conn.execute_queries({'user_costs': query})
        user_costs = convert_decimals(results.get('user_costs', []))
        transformed = []
        for row in user_costs:
            total_cost_gb = float(row.get('TOTAL_COST_GB', 0))
            total_queries = int(row.get('TOTAL_QUERIES', 0))
            avg_execution_time_sec = float(row.get('AVG_EXECUTION_TIME_SEC', 0))
            avg_query_cost_gb = total_cost_gb / total_queries if total_queries > 0 else 0
            total_execution_time_min = (avg_execution_time_sec * total_queries) / 60 if total_queries > 0 else 0
            transformed.append({
                'user_email': row.get('USER_EMAIL'),
                'total_cost_gb': total_cost_gb,
                'total_queries': total_queries,
                'avg_query_cost_gb': avg_query_cost_gb,
                'first_query_date': row.get('FIRST_QUERY_DATE'),
                'last_query_date': row.get('LAST_QUERY_DATE'),
                'total_execution_time_min': total_execution_time_min,
                'avg_execution_time_sec': avg_execution_time_sec,
                'success_count': int(row.get('SUCCESS_COUNT', 0)),
                'failure_count': int(row.get('FAILURE_COUNT', 0))
            })
        return json.dumps(transformed)
    except Exception as e:
        logger.error(f'Error in get_total_cost_gb_by_users: {e}')
        return json.dumps([])

def get_total_cost_gb_by_table():
    try:
        print("Starting get_total_cost_gb_by_table...")
        
        # Get database name from secrets
        secrets = SnowflakeConnection.get_instance().secrets
        database = secrets['SNOWFLAKE_DATABASE']
        print(f"Executing query for database: {database}")
        
        # Create the raw string with placeholders - exactly matching restore.py
        query = r"""
        SELECT
            CASE
                WHEN REGEXP_SUBSTR(QUERY_TEXT, '\\bFROM\\s+{0}\\.([\\w\\.]+)', 1, 1, 'i', 1) IS NULL THEN 'temporary_table'
                ELSE REGEXP_SUBSTR(QUERY_TEXT, '\\bFROM\\s+{0}\\.([\\w\\.]+)', 1, 1, 'i', 1)
            END AS table_name,
            COUNT(*) AS total_queries,
            SUM(bytes_scanned) / 1073741824 AS total_cost_gb,
            AVG(bytes_scanned) / 1073741824 AS avg_query_cost_gb,
            MIN(start_time) AS first_query_date,
            MAX(start_time) AS last_query_date,
            SUM(DATEDIFF('second', start_time, end_time)) / 60 AS total_execution_time_min,
            ROUND(AVG(DATEDIFF('second', start_time, end_time)), 2) AS avg_execution_time_sec,
            SUM(CASE WHEN error_code IS NULL THEN 1 ELSE 0 END) AS success_count,
            SUM(CASE WHEN error_code IS NOT NULL THEN 1 ELSE 0 END) AS failure_count
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE QUERY_TEXT ILIKE '%FROM%{0}.%' AND bytes_scanned IS NOT NULL
        GROUP BY table_name
        ORDER BY total_cost_gb DESC
        """
        
        # Format the string after defining it - exactly matching restore.py
        query = query.format(database)
        
        print(f"Executing query: {query}")  # Debug print
        
        # Use direct connection like restore.py
        conn = snowflake.connector.connect(
            user=secrets['SNOWFLAKE_USER'],
            password=secrets.get('SNOWFLAKE_PASSWORD'),
            account=secrets['SNOWFLAKE_ACCOUNT'],
            warehouse=secrets['SNOWFLAKE_WAREHOUSE'],
            database=secrets['SNOWFLAKE_DATABASE'],
        )
        
        try:
            cur = conn.cursor()
            cur.execute(query)
            columns = [col[0] for col in cur.description]
            results = [dict(zip(columns, row)) for row in cur.fetchall()]
            results = normalize_keys(results)  # Normalize keys immediately like restore.py
        finally:
            conn.close()
        
        print(f"Raw results: {results}")  # Debug print
        
        if not results:
            print("No table costs results returned from query")
            return json.dumps([])
            
        results = convert_decimals(results)
        print(f"Converted table costs: {results}")  # Debug print
        
        transformed = []
        for row in results:
            table_name = row.get('TABLE_NAME')
            print(f"Processing table: {table_name}")  # Debug print
            
            dataset = 'system'
            table = table_name
            if table_name:
                parts = table_name.split('.')
                if len(parts) == 2:
                    dataset, table = parts
                elif len(parts) == 1:
                    table = parts[0]
            
            transformed.append({
                'dataset': dataset,
                'table': table,
                'total_cost_gb': float(row.get('TOTAL_COST_GB', 0)),
                'total_queries': int(row.get('TOTAL_QUERIES', 0)),
                'avg_query_cost_gb': float(row.get('AVG_QUERY_COST_GB', 0)),
                'first_query_date': row.get('FIRST_QUERY_DATE'),
                'last_query_date': row.get('LAST_QUERY_DATE'),
                'total_execution_time_min': float(row.get('TOTAL_EXECUTION_TIME_MIN', 0)),
                'avg_execution_time_sec': float(row.get('AVG_EXECUTION_TIME_SEC', 0)),
                'success_count': int(row.get('SUCCESS_COUNT', 0)),
                'failure_count': int(row.get('FAILURE_COUNT', 0))
            })
        
        print(f"Final transformed results: {transformed}")  # Debug print
        return json.dumps(transformed)
    except Exception as e:
        print(f'Error in get_total_cost_gb_by_table: {str(e)}')
        print(f'Error type: {type(e)}')
        import traceback
        print(f'Traceback: {traceback.format_exc()}')
        return json.dumps([])

# Debug Local testing
# if __name__ == "__main__":
#     try:
#         stats = get_all_stats()
#         print("Dataset count:", stats['dataset_count'])
#         print("Table count:", stats['table_count'])
#         print("Total queries executed:", stats['total_queries'])
#         print("Average execution time (seconds):", stats['avg_execution_time'])
#         print("Failure rate percentage:", stats['failure_rate'])
#         print("Query cost by months chart:", stats['monthly_costs'])
#         print("Query cost by days chart:", stats['daily_costs'])
#         print("Total cost GB by users:", stats['user_costs'])
#         print("Total cost GB by table:", stats['table_costs'])
#     except Exception as e:
#         logger.error(f'Error in main: {e}')
