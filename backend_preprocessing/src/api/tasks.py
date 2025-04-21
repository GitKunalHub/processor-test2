import os
from celery import Celery
from kombu import Exchange, Queue
from dotenv import load_dotenv

from models.config import Config
from utils.tasks_helpers import (
    declare_queues,
    wait_for_main_queue_empty,
    send_email_notification,
    send_json_notification,
    process_topic_batch,
)

load_dotenv()

app = Celery(
    'tasks',
    broker=f'amqp://{Config.RABBITMQ_USER}:{Config.RABBITMQ_PASS}@{Config.RABBITMQ_HOST}:{Config.RABBITMQ_PORT}//',
    backend="rpc://"
)

MAIN_QUEUE = "main_queue"
DLQ_QUEUE = "custom_dlq"
MAIN_EXCHANGE = Exchange("main_exchange", type="direct", durable=True)
DLX_EXCHANGE = Exchange("dlx_exchange", type="direct", durable=True)

app.conf.update(
    task_queues=[
        Queue(
            MAIN_QUEUE,
            exchange=MAIN_EXCHANGE,
            routing_key=MAIN_QUEUE,
            queue_arguments={
                "x-dead-letter-exchange": DLX_EXCHANGE.name,
                "x-dead-letter-routing-key": DLQ_QUEUE,
            },
        ),
        Queue(
            DLQ_QUEUE,
            exchange=DLX_EXCHANGE,
            routing_key=DLQ_QUEUE,
        ),
    ],
    task_default_queue=MAIN_QUEUE,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_acks_on_failure_or_timeout=False,
)

# Register the actual Celery task, importing only the signature/wrapper
app.task(
    name="tasks.process_topic_batch",
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 0},
)(process_topic_batch)