import os
import json
import pandas as pd
import openai
import time

from models.config import Config
from utils.topic_extraction import (
    fetch_saved_topics,
    process_batch,
    read_azure_file,
    fetch_saved_queries,
)

import pika

MAIN_QUEUE = "main_queue"
DLQ_QUEUE = "custom_dlq"
MAIN_EXCHANGE_NAME = "main_exchange"
DLX_EXCHANGE_NAME = "dlx_exchange"

def declare_queues():
    credentials = pika.PlainCredentials(Config.RABBITMQ_USER, Config.RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=Config.RABBITMQ_HOST,
        port=int(Config.RABBITMQ_PORT),
        credentials=credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange=MAIN_EXCHANGE_NAME, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=DLX_EXCHANGE_NAME, exchange_type="direct", durable=True)
    channel.queue_delete(MAIN_QUEUE)
    channel.queue_delete(DLQ_QUEUE)
    channel.queue_declare(
        queue=MAIN_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX_EXCHANGE_NAME,
            "x-dead-letter-routing-key": DLQ_QUEUE,
        },
    )
    channel.queue_declare(queue=DLQ_QUEUE, durable=True)
    channel.queue_declare(queue='interactions_ready', durable=True)
    channel.queue_bind(exchange=MAIN_EXCHANGE_NAME, queue=MAIN_QUEUE, routing_key=MAIN_QUEUE)
    channel.queue_bind(exchange=DLX_EXCHANGE_NAME, queue=DLQ_QUEUE, routing_key=DLQ_QUEUE)
    connection.close()
    print("Queues and exchanges configured successfully.")

def wait_for_main_queue_empty():
    credentials = pika.PlainCredentials(Config.RABBITMQ_USER, Config.RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=Config.RABBITMQ_HOST,
        port=int(Config.RABBITMQ_PORT),
        credentials=credentials
    )
    while True:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        main_queue = channel.queue_declare(MAIN_QUEUE, passive=True)
        if main_queue.method.message_count == 0:
            connection.close()
            return
        print(f"Main queue has {main_queue.method.message_count} pending messages...")
        connection.close()
        time.sleep(5)

def send_email_notification(error_message, task_id):
    print(f"Simulated Email for Missing API: Task {task_id} encountered an error: {error_message}")

def send_json_notification(error_message, task_id):
    print(f"Simulated Email for JSON: Task {task_id} encountered an error: {error_message}")

def process_topic_batch(self, batch_ids):
    is_dlq = (
        self.request.delivery_info.get("routing_key") == DLQ_QUEUE
        if self.request.delivery_info else False
    )
    try:
        time.sleep(2)
        input_content = read_azure_file("amazon_interactions.json")
        full_data = json.loads(input_content)
        df_full = pd.DataFrame(full_data)
        df = df_full[df_full["interaction_id"].isin(batch_ids)]
        if df.empty:
            raise ValueError("No matching records found for provided IDs")
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("Invalid JSON Parsing")
        openai.api_key = api_key
        client = openai
        existing_topics = fetch_saved_topics()
        existing_queries = fetch_saved_queries()
        result = process_batch(client, df, existing_topics, existing_queries)
        print(f"Processed batch {self.request.id} successfully.")
        return result
    except Exception as e:
        error_message = str(e)
        if is_dlq:
            if "Missing OPENAI_API_KEY" in error_message or "Openai Balance" in error_message:
                send_email_notification(error_message, self.request.id)
            elif "No JSON array found in the response." in error_message:
                send_json_notification(error_message, self.request.id)
            print(f"DLQ Handling: Task {self.request.id} failed with error: {error_message}")
            return {
                "status": "failed",
                "error": error_message,
                "task_id": self.request.id,
                "batch_ids": batch_ids,
            }
        else:
            if "No JSON array found in the response." in error_message:
                send_json_notification(error_message, self.request.id)
            print(f"Main queue error: {error_message} - Routing to DLQ")
            self.request.requeue = False
            raise