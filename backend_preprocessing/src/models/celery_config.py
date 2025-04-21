from kombu import Exchange, Queue

# Custom queue and exchange names for Celery
MAIN_QUEUE = "main_queue"
DLQ_QUEUE = "custom_dlq"
MAIN_EXCHANGE = Exchange("main_exchange", type="direct", durable=True)
DLX_EXCHANGE = Exchange("dlx_exchange", type="direct", durable=True)