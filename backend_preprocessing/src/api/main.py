import subprocess
import sys
import time
import pika
import json
from models.config import Config
from utils.redis_utils import set_processor_availability, wait_for_redis
from utils.rabbitmq_utils import wait_for_rabbitmq, wait_for_interactions_ready
from utils.producer_helpers import log_message
from api.tasks import declare_queues, wait_for_main_queue_empty

def main():
    processes = []
    try:
        set_processor_availability(True)
        print("Initial: Processor Availability = True")
        print("🌟 Waiting for infrastructure...")
        wait_for_rabbitmq(Config.RABBITMQ_HOST, Config.RABBITMQ_PORT)
        wait_for_redis(Config.REDIS_HOST, Config.REDIS_PORT)

        print("🚀 Starting Celery workers...")
        main_worker = subprocess.Popen([
            "celery", "-A", "api.tasks", "worker",
            "--loglevel=info", "-Q", "main_queue", "--concurrency=4"
        ])
        processes.append(main_worker)

        dlq_worker = subprocess.Popen([
            "celery", "-A", "api.tasks", "worker",
            "--loglevel=info", "-Q", "custom_dlq", "--concurrency=1"
        ])
        processes.append(dlq_worker)

        while True:
            agent_id = wait_for_interactions_ready()
            set_processor_availability(False)
            declare_queues()
            print("📤 Starting producer for new batch...")
            subprocess.run([sys.executable, "-m", "api.producer", agent_id], check=True)
            print("⏳ Waiting for queue drain...")
            wait_for_main_queue_empty()
            set_processor_availability(True)
    except Exception as e:
        print(f"Error: {e}")
        set_processor_availability(False)
        sys.exit(1)
    finally:
        for p in processes: p.terminate()

if __name__ == "__main__":
    main()