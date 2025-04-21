import sys
import time
import os
from utils.producer_helpers import (
    get_interactions_from_redis,
    log_message
)
from utils.rabbitmq_utils import wait_for_rabbitmq
from utils.batch_utils import process_records_in_batches_by_session
from models.config import Config
from api.tasks import declare_queues, wait_for_main_queue_empty, process_topic_batch

def send_batch(batch_ids):
    return process_topic_batch.delay(batch_ids)

if __name__ == "__main__":
    agent_id = sys.argv[1] if len(sys.argv) > 1 else None
    if not agent_id:
        print("❌ Agent ID not provided!")
        sys.exit(1)

    open("producer_ran.flag", "w").close()
    wait_for_rabbitmq(Config.RABBITMQ_HOST, Config.RABBITMQ_PORT, timeout=60)
    declare_queues()
    try:
        data = get_interactions_from_redis(agent_id, Config.REDIS_HOST, Config.REDIS_PORT)
        if not data:
            print(f"No interactions found for agent {agent_id} in Redis!")
        else:
            print(f"Retrieved {len(data)} interactions for agent {agent_id}.")
    except Exception as e:
        log_message(f"❌ Failed to read input file from Azure: {e}")
        print(f"Failed to read input file from Azure: {e}")
        raise

    batches = process_records_in_batches_by_session(
        records=data,
        max_tokens=1024,
        reserved_model_response_tokens=200,
        encoding_name="cl100k_base"
    )

    all_tasks = []
    for i, batch_ids in enumerate(batches):
        task = send_batch(batch_ids)
        all_tasks.append(task)
        log_message(f"📤 Sent batch {i+1} with {len(batch_ids)} record IDs.")
        time.sleep(0.5)

    all_results = []
    for task in all_tasks:
        try:
            res = task.get(timeout=260)
            log_message(f"✅ Task result: {res}")
            print(f"Task result: {res}")
            all_results.extend(res)
        except Exception as e:
            log_message(f"❌ Task failed: {e}")
            print(f"Task failed: {e}")

    print("Waiting for main queue to drain...")
    wait_for_main_queue_empty()
    print("DLQ tasks will be processed automatically by the worker.")