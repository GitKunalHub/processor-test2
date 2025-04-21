import time
import redis

def set_processor_availability(available):
    r = redis.Redis(host='redis', port=6379)
    r.set('processor_available', '1' if available else '0')

def wait_for_redis(host, port, timeout=30):
    print("🌟 Waiting for Redis to be ready...")
    r = redis.Redis(host=host, port=port)
    start = time.time()
    while True:
        try:
            if r.ping():
                print("Redis is up!")
                return
        except Exception as e:
            if time.time() - start > timeout:
                raise RuntimeError("Timeout waiting for Redis") from e
            print("Waiting for Redis...")
            time.sleep(2)