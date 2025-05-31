from celery import Celery
from kombu import Exchange, Queue

app = Celery(
    'tasks',
    broker='pyamqp://guest@localhost//',
    backend='db+sqlite:///results.sqlite3'
)

app.conf.task_queues = (
    Queue('extract_q', Exchange('extract', type='topic'), routing_key='extract', queue_arguments={'x-queue-type': 'quorum'}),
    Queue('preprocess_q', Exchange('preprocess', type='topic'), routing_key='preprocess', queue_arguments={'x-queue-type': 'quorum'}),
    Queue('forecast_q', Exchange('forecast', type='topic'), routing_key='forecast', queue_arguments={'x-queue-type': 'quorum'}),
    Queue('load_q', Exchange('load', type='topic'), routing_key='load', queue_arguments={'x-queue-type': 'quorum'}),
)

app.conf.task_routes = {
    'extract_task.extract_data': {'queue': 'extract_q', 'routing_key': 'extract'},
    'transform_task.preprocess_data': {'queue': 'preprocess_q', 'routing_key': 'preprocess'},
    'transform_task.forecast_data': {'queue': 'forecast_q', 'routing_key': 'forecast'},
    'load_task.load_to_db': {'queue': 'load_q', 'routing_key': 'load'},
}

# Import semua task agar dikenali Celery
import extract_task
import transform_task
import load_task
