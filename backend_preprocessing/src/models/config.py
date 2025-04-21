import os
from urllib.parse import quote_plus

class Config:
    # RabbitMQ
    RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
    RABBITMQ_USER = os.getenv("RABBITMQ_USER")
    RABBITMQ_PASS = os.getenv("RABBITMQ_PASS")
    RABBITMQ_PORT = os.getenv("RABBITMQ_PORT")
    # Redis
    REDIS_PORT = os.getenv("REDIS_PORT")
    REDIS_HOST = os.getenv("REDIS_HOST")
    # MongoDB
    MONGO_USER = quote_plus(os.getenv("MONGODB_USER"))
    MONGO_PASS = quote_plus(os.getenv("MONGODB_PASS"))
    MONGO_HOST = os.getenv("MONGODB_HOST")
    MONGO_PORT = os.getenv("MONGODB_PORT")
    MONGO_AUTH_SOURCE = quote_plus(os.getenv("MONGODB_AUTH_SOURCE"))
    MONGODB_URI = (
        f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_AUTH_SOURCE}"
    )
    MONGODB_DB = os.getenv("MONGODB_DB")
    MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION")
    # Azure
    AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    AZURE_BLOB_CONTAINER_NAME = os.getenv("AZURE_BLOB_CONTAINER_NAME")
    # OpenAI
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")