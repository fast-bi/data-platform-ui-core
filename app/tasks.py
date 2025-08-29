import redis
import pickle
from app.celery_app import celery
from app.config import Config
from celery.schedules import crontab
from celery.signals import beat_init
from concurrent.futures import ThreadPoolExecutor
from flask import Flask
from flask_caching import Cache

def get_bq_stats():
    # Import bigquery_stats only when needed
    import app.datawarehouse_stats.bigquery_stats as bq
    with ThreadPoolExecutor() as executor:
        futures = {
            # cards
            'dataset_count': executor.submit(bq.get_dataset_count),
            'total_query_executed': executor.submit(bq.get_total_query_executed),
            'table_count': executor.submit(bq.get_table_count),
            'avg_execution_time_seconds': executor.submit(bq.get_avg_execution_time_seconds),
            'failure_rate_percentage': executor.submit(bq.get_failure_rate_percentage),
            # charts
            'query_cost_by_months_chart': executor.submit(bq.get_query_cost_by_month),
            'query_cost_by_days_chart': executor.submit(bq.get_query_cost_for_last_30_days),
            # tables
            'total_cost_gb_by_users': executor.submit(bq.get_total_cost_gb_by_users),
            'total_cost_gb_by_table': executor.submit(bq.get_total_cost_gb_by_table)
        }
        results = {key: future.result() for key, future in futures.items()}
    return results

def get_sf_stats():
    # Import snowflake_stats only when needed
    import app.datawarehouse_stats.snowflake_stats as sf
    with ThreadPoolExecutor() as executor:
        futures = {
            # cards
            'dataset_count': executor.submit(sf.get_dataset_count),
            'total_query_executed': executor.submit(sf.get_total_query_executed),
            'table_count': executor.submit(sf.get_table_count),
            'avg_execution_time_seconds': executor.submit(sf.get_avg_execution_time_seconds),
            'failure_rate_percentage': executor.submit(sf.get_failure_rate_percentage),
            # charts
            'query_cost_by_months_chart': executor.submit(sf.get_query_cost_by_month),
            'query_cost_by_days_chart': executor.submit(sf.get_query_cost_for_last_30_days),
            # tables
            'total_cost_gb_by_users': executor.submit(sf.get_total_cost_gb_by_users),
            'total_cost_gb_by_table': executor.submit(sf.get_total_cost_gb_by_table)
        }
        results = {key: future.result() for key, future in futures.items()}
    return results

def get_rd_stats():
    # Import redshift_stats only when needed
    import app.datawarehouse_stats.redshift_stats as rd
    with ThreadPoolExecutor() as executor:
        futures = {
            # cards
            'dataset_count': executor.submit(rd.get_dataset_count),
            'total_query_executed': executor.submit(rd.get_total_query_executed),
            'table_count': executor.submit(rd.get_table_count),
            'avg_execution_time_seconds': executor.submit(rd.get_avg_execution_time_seconds),
            'failure_rate_percentage': executor.submit(rd.get_failure_rate_percentage),
            # charts
            'query_cost_by_months_chart': executor.submit(rd.get_query_cost_by_month),
            'query_cost_by_days_chart': executor.submit(rd.get_query_cost_for_last_30_days),
            # tables
            'total_cost_gb_by_users': executor.submit(rd.get_total_cost_gb_by_users),
            'total_cost_gb_by_table': executor.submit(rd.get_total_cost_gb_by_table)
        }
        results = {key: future.result() for key, future in futures.items()}
    return results

def get_ft_stats():
    # Import fabric_stats only when needed
    import app.datawarehouse_stats.fabric_stats as ft
    with ThreadPoolExecutor() as executor:
        futures = {
            # cards
            'dataset_count': executor.submit(ft.get_dataset_count),
            'total_query_executed': executor.submit(ft.get_total_query_executed),
            'table_count': executor.submit(ft.get_table_count),
            'avg_execution_time_seconds': executor.submit(ft.get_avg_execution_time_seconds),
            'failure_rate_percentage': executor.submit(ft.get_failure_rate_percentage),
            # charts
            'query_cost_by_months_chart': executor.submit(ft.get_query_cost_by_month),
            'query_cost_by_days_chart': executor.submit(ft.get_query_cost_for_last_30_days),
            # tables
            'total_cost_gb_by_users': executor.submit(ft.get_total_cost_gb_by_users),
            'total_cost_gb_by_table': executor.submit(ft.get_total_cost_gb_by_table)
        }
        results = {key: future.result() for key, future in futures.items()}
    return results

@celery.task
def cache_dwh_stats():
    try:
        # Create a Flask app instance and configure it
        app = Flask(__name__)
        app.config.from_object(Config)
        cache = Cache(app, config={'CACHE_TYPE': 'RedisCache'})

        with app.app_context():
            # Get stats only for the configured data warehouse type
            dwh_type = app.config['FASTBI_PLATFORM_DWH']
            stats = None
            
            if dwh_type == 'bigquery':
                stats = get_bq_stats()
            elif dwh_type == 'snowflake':
                stats = get_sf_stats()
            elif dwh_type == 'redshift':
                stats = get_rd_stats()
            elif dwh_type == 'fabric':
                stats = get_ft_stats()
            else:
                print(f"Unsupported data warehouse type: {dwh_type}")
                return None

            if stats:
                # Cache the stats with a consistent key
                cache_key = 'global_stats'
                cache.set(cache_key, stats, timeout=7200)
                print(f"cache_dwh_stats executed and set {cache_key} in Redis for {dwh_type}.")
                return stats
            return None
    except Exception as e:
        print(f"Error in cache_dwh_stats: {e}")
        return None

@celery.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    # Schedule cache_dwh_stats to run every 24 hours
    sender.add_periodic_task(
        crontab(hour=0, minute=0),  # Every day at midnight
        cache_dwh_stats.s(),
        name='cache_dwh_stats_every_24_hours'
    )

@beat_init.connect
def at_beat_start(sender, **kwargs):
    print("Celery Beat started, triggering cache_dwh_stats")
    cache_dwh_stats.delay()
