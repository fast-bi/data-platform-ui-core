from datetime import timedelta
import redis
import os
import ssl
from pathlib import Path

# Set Parameters from Env Variables

class Config:
    # SSL Configuration
    SSL_CERT_FILE = os.getenv('SSL_CERT_FILE')
    REQUESTS_CA_BUNDLE = os.getenv('REQUESTS_CA_BUNDLE')
    PYTHONHTTPSVERIFY = os.getenv('PYTHONHTTPSVERIFY', '1')

    @classmethod
    def get_ssl_context(cls):
        """Create an SSL context that combines both default and custom certificates."""
        context = ssl.create_default_context()
        
        # If custom CA is specified and exists, load it
        if cls.SSL_CERT_FILE and Path(cls.SSL_CERT_FILE).exists():
            # Load both the custom CA and the default system certificates
            context.load_verify_locations(cafile=cls.SSL_CERT_FILE)
            context.load_default_certs()
        
        # Set verification mode
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = True
        
        return context

    # Flask configuration
    DEBUG = os.getenv('DEBUG', False)

    DELEVOPMENT_TEAM = os.getenv('DELEVOPMENT_TEAM', "False")

    FLASK_ENV = os.environ.get("FLASK_ENV", "development")

    SECRET_KEY = os.getenv('FN_FLASK_SECRET_KEY')
    if not SECRET_KEY:
        raise EnvironmentError("FN_FLASK_SECRET_KEY environment variable is missing. Please set it before running the application.")

    MAIL_DEBUG = os.getenv('MAIL_DEBUG', False)

    MAIL_SERVER = os.getenv('MAIL_SERVER')
    if not MAIL_SERVER:
        raise EnvironmentError("MAIL_SERVER environment variable is missing. Please set it before running the application.")

    MAIL_PORT = os.getenv('MAIL_PORT')
    if not MAIL_PORT:
        raise EnvironmentError("MAIL_PORT environment variable is missing. Please set it before running the application.")

    MAIL_USE_TLS = os.getenv('MAIL_USE_TLS')
    if MAIL_USE_TLS is None:
        raise EnvironmentError("MAIL_USE_TLS environment variable is missing. Please set it before running the application.")

    MAIL_USE_SSL = os.getenv('MAIL_USE_SSL')
    if MAIL_USE_SSL is None:
        raise EnvironmentError("MAIL_USE_SSL environment variable is missing. Please set it before running the application.")

    MAIL_USERNAME = os.getenv('MAIL_USERNAME')
    if not MAIL_USERNAME:
        raise EnvironmentError("MAIL_USERNAME environment variable is missing. Please set it before running the application.")

    MAIL_PASSWORD = os.getenv('MAIL_PASSWORD')
    if not MAIL_PASSWORD:
        raise EnvironmentError("MAIL_PASSWORD environment variable is missing. Please set it before running the application.")

    MAIL_DEFAULT_SENDER = os.getenv('MAIL_DEFAULT_SENDER')
    if not MAIL_DEFAULT_SENDER:
        raise EnvironmentError("MAIL_DEFAULT_SENDER environment variable is missing. Please set it before running the application.")

    # Database configuration
    DB_HOST = os.getenv('DB_HOST')
    if not DB_HOST:
        raise EnvironmentError("DB_HOST environment variable is missing. Please set it before running the application.")

    DB_PORT = os.getenv('DB_PORT')
    if not DB_PORT:
        raise EnvironmentError("DB_PORT environment variable is missing. Please set it before running the application.")

    DB_USER = os.getenv('DB_USER')
    if not DB_USER:
        raise EnvironmentError("DB_USER environment variable is missing. Please set it before running the application.")

    DB_PASSWORD = os.getenv('DB_PASSWORD')
    if not DB_PASSWORD:
        raise EnvironmentError("DB_PASSWORD environment variable is missing. Please set it before running the application.")

    DB_NAME = os.getenv('DB_NAME')
    if not DB_NAME:
        raise EnvironmentError("DB_NAME environment variable is missing. Please set it before running the application.")

    SQLALCHEMY_DATABASE_URI = 'postgresql://{}:{}@{}:{}/{}'.format(
        DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)

    # Redis configuration
    REDIS_HOST = os.getenv('REDIS_HOST')
    if not REDIS_HOST:
        raise EnvironmentError("REDIS_HOST environment variable is missing. Please set it before running the application.")

    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))  # Default to 6379 if not set
    if not REDIS_PORT:
        raise EnvironmentError("REDIS_PORT environment variable is missing. Please set it before running the application.")

    REDIS_DB = os.getenv('REDIS_DB')
    if REDIS_DB is None:
        raise EnvironmentError("REDIS_DB environment variable is missing. Please set it before running the application.")

    REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')

    # Configure SESSION_REDIS
    if REDIS_PASSWORD:
        SESSION_REDIS = redis.StrictRedis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=False
        )
    else:
        SESSION_REDIS = redis.StrictRedis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=False
        )

    SESSION_TYPE = 'redis'
    SESSION_COOKIE_NAME = 'flask_oidc_session'
    SESSION_PERMANENT = False
    PERMANENT_SESSION_LIFETIME = timedelta(seconds=21600)

    # Cache configuration
    CACHE_TYPE = 'redis'
    CACHE_KEY_PREFIX = 'flask_cache_'
    if REDIS_PASSWORD:
        CACHE_REDIS_URL = f'redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}'
    else:
        CACHE_REDIS_URL = f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}'

    # DCDQ Meta Collect Service API Configuration
    DC_DQ_ENDPOINT_URL = os.getenv('DC_DQ_ENDPOINT_URL', 'http://data-dcdq-metacollect.data-dcdq-metacollect.svc.cluster.local')
    if not DC_DQ_ENDPOINT_URL:
        raise EnvironmentError("DC_DQ_ENDPOINT_URL environment variable is missing. Please set it before running the application.")

    DC_DQ_BEARER_TOKEN = os.getenv('DC_DQ_BEARER_TOKEN')
    if DC_DQ_BEARER_TOKEN:
        BEARER_TOKEN = f'Bearer {DC_DQ_BEARER_TOKEN}'
    else:
        raise EnvironmentError("DC_DQ_BEARER_TOKEN environment variable is missing. Please set it before running the application.")

    # OIDC configuration
    OIDC_CLIENT_SECRETS = 'client_secrets.json'
    OIDC_COOKIE_SECURE = True
    OIDC_ID_TOKEN_COOKIE_SECURE = True
    OIDC_ACCESS_TOKEN_COOKIE_SECURE = True
    OIDC_REFRESH_TOKEN_COOKIE_SECURE = True
    OIDC_CALLBACK_ROUTE = '/oidc/callback'
    OIDC_SCOPES = ['openid', 'email', 'profile', 'groups']

    # System Local Connection Configuration
    AIRBYTE_API_LINK = os.environ.get('AIRBYTE_API_LINK', 'http://data-replication-airbyte-webapp-svc.data-replication.svc.cluster.local')
    AIRBYTE_API_BASE = os.environ.get('AIRBYTE_API_BASE', 'api/public')

    # Local embeded Monitoring Configuration
    GRAFANA_JWT_KEY_PATH = os.environ.get('GRAFANA_JWT_KEY_PATH', '/usr/src/app/instance/grafana.key')

    # IDP SSO Configuration
    IDP_SSO_CONTROL_MODE = os.environ.get('IDP_SSO_CONTROL_MODE', 'disabled')
    IDP_SSO_LINK = os.environ.get('IDP_SSO_LINK')
    IDP_SSO_USERS_LINK = os.environ.get('IDP_SSO_USERS_LINK')

    #Stats

    # Datawarehouse Stats
    FASTBI_PLATFORM_DWH = os.environ.get('FASTBI_PLATFORM_DWH', 'bigquery')

    # Add the Celery configuration to your Flask app's configuration
    BQ_PROJECT_ID = os.getenv('BQ_PROJECT_ID')
    BQ_REGION = os.getenv('BQ_REGION')
    GCP_SA_SECRET = os.getenv('GCP_SA_SECRET')
    GCP_SA_IMPERSONATE_EMAIL = os.getenv('GCP_SA_IMPERSONATE_EMAIL')

    # Add Statistics
    FAST_BI_STATISTICS_ID=os.getenv('FAST_BI_STATISTICS_ID')

    # Application configuration - enpoint urls
class SourceConfig:
    @staticmethod
    def _str_to_bool(value):
        """Convert string value to boolean"""
        return str(value).lower() in ('true', '1', 'yes', 'on')

    @staticmethod
    def get_environment_variables():
        return {
            'airbyte_link': os.environ.get('AIRBYTE_LINK', ''),
            'airflow_link': os.environ.get('AIRFLOW_LINK', ''),
            'datahub_link': os.environ.get('DATAHUB_LINK', ''),
            'gitlab_link': os.environ.get('GITLAB_LINK', ''),
            'git_provider': os.environ.get('GIT_PROVIDER', ''),
            'lightdash_link': os.environ.get('BI_LINK') or os.environ.get('LIGHTDASH_LINK', ''),
            'data_quality_link': os.environ.get('DATA_QUALITY_LINK', ''),
            'data_catalog_link': os.environ.get('DATA_CATALOG_LINK', ''),
            'monitoring_link': os.environ.get('MONITORING_LINK', ''),
            'bi_platform_gcp_id': os.environ.get('BI_PLATFORM_GCP_ID', ''),
            'bi_platform_bq_id': os.environ.get('BI_PLATFORM_BQ_ID', ''),
            'ide_link': os.environ.get('IDE_LINK', ''),
            's3_link': os.environ.get('S3_LINK', ''),
            'main_link': os.environ.get('MAIN_LINK', ''),
            'dbt_init_api_link': os.environ.get('DBT_INIT_API_LINK', ''),
            'dbt_init_api_key': os.environ.get('DBT_INIT_API_KEY', ''),
            'customer_repo_link': os.environ.get('CUSTOMER_REPO_LINK', ''),
            'monitoring_basic_auth_user': os.environ.get('MONITORING_BASIC_AUTH_USER', ''),
            'monitoring_basic_auth_pass': os.environ.get('MONITORING_BASIC_AUTH_PASS', ''),
            'wiki_link': os.environ.get('WIKI_LINK', ''),
            'iam_idp_management_link': os.environ.get('IDP_SSO_LINK', ''),
            'iam_idp_user_management_link': os.environ.get('IDP_SSO_USERS_LINK', ''),
            'cicd_workflow_link': os.environ.get('CICD_WORKFLOW_LINK', ''),
            'customer': os.environ.get('CUSTOMER', 'Fast.bi'),
            'enable_bash_operator_tab': SourceConfig._str_to_bool(os.environ.get('ENABLE_BASH_OPERATOR_TAB', 'False')),
            'enable_gke_operator_tab': SourceConfig._str_to_bool(os.environ.get('ENABLE_GKE_OPERATOR_TAB', 'False')),
            'enable_api_operator_tab': SourceConfig._str_to_bool(os.environ.get('ENABLE_API_OPERATOR_TAB', 'False'))
        }
