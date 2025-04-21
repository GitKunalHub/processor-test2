import redis
import json
from datetime import datetime

LOG_FILE = "producer_ran.log"

def get_interactions_from_redis(agent_id: str, redis_host, redis_port):
    file_path = "interactions.json"  # Hardcoded file path for testing
    try:
        with open(file_path, 'r') as file:
            interactions = json.load(file)
        print(f"Loaded {len(interactions)} interactions from file: {file_path}")
        return interactions
    except Exception as e:
        print(f"Error reading interactions from file: {e}")
        return []
    # redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)
    # interactions = []
    # agent_queue_key = f"interactions_queue:{agent_id}"
    # print("Redis list length before popping:", redis_client.llen(agent_queue_key))
    # while True:
    #     interaction_data = redis_client.rpop(agent_queue_key)
    #     if not interaction_data:
    #         break
    #     interactions.append(json.loads(interaction_data))
    # return interactions

def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a") as f:
        f.write(f"{timestamp} - {message}\n")