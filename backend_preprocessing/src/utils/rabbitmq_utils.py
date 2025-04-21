import time
import socket
import pika
import models.config as Config
import json

def wait_for_rabbitmq(host, port, timeout=30):
    start_time = time.time()
    while True:
        try:
            with socket.create_connection((host, int(port)), timeout=2):
                print("RabbitMQ is up!")
                return
        except Exception:
            if time.time() - start_time > timeout:
                print(f"Timeout waiting for RabbitMQ! host: {host}, port: {port}")
                raise
            print(f"Waiting for RabbitMQ... host: {host}, port: {port}")
            time.sleep(2)

def wait_for_interactions_ready():
    print("🌟 Waiting for interactions-ready message...")
    credentials = pika.PlainCredentials(Config.RABBITMQ_USER, Config.RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=Config.RABBITMQ_HOST, port=int(Config.RABBITMQ_PORT), credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    agent_id = None
    while True:
        method_frame, _, body = channel.basic_get(queue='processor_queue', auto_ack=True)
        if method_frame:
            try:
                message = json.loads(body.decode('utf-8'))
                agent_id = message.get('agent_id')
                if agent_id:
                    print(f"✅ Received interactions ready for agent {agent_id}")
                    connection.close()
                    return agent_id
                else:
                    print("⚠️ Message missing agent_id")
            except json.JSONDecodeError:
                print(f"⚠️ Invalid message format: {body}")
        time.sleep(1)