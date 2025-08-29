# celery_app.py

from celery import Celery
from app.config import Config

def make_celery():
    celery = Celery(
        __name__,
        backend=Config.CACHE_REDIS_URL,
        broker=Config.CACHE_REDIS_URL
    )
    celery.conf.update({
        'broker_url': Config.CACHE_REDIS_URL,
        'result_backend': Config.CACHE_REDIS_URL,
        'timezone': 'UTC',
        'imports': ('app.tasks',),  # Ensure tasks are imported
    })
    return celery

celery = make_celery()
