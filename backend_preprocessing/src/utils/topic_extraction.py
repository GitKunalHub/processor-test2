import logging
import os
import json
import re
from typing import List, Dict, Set
from urllib.parse import quote_plus
import pandas as pd
from openai import OpenAI
from azure.storage.blob import BlobServiceClient
from statistics import mean
import requests
import time
import ast
import dateutil.parser
from collections import defaultdict, Counter
from pymongo import MongoClient

from models.config import Config

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("topic_analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_blob_client(blob_name: str):
    blob_service_client = BlobServiceClient.from_connection_string(Config.AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(Config.AZURE_BLOB_CONTAINER_NAME)
    return container_client.get_blob_client(blob_name)

def read_azure_file(blob_name: str) -> str:
    blob_client = get_blob_client(blob_name)
    download_stream = blob_client.download_blob()
    logger.info(f"Reading file '{blob_name}' from Azure Blob Storage.")
    return download_stream.readall().decode('utf-8')

def write_azure_file(blob_name: str, content: str) -> None:
    blob_client = get_blob_client(blob_name)
    logger.info(f"Writing file '{blob_name}' to Azure Blob Storage.")
    try:
        blob_client.delete_blob()
        logger.info(f"Deleted existing blob '{blob_name}'.")
    except Exception:
        logger.info(f"Blob '{blob_name}' does not exist, proceeding to upload.")
        pass
    blob_client.upload_blob(content.encode('utf-8'), overwrite=True)

def fetch_saved_topics(file_path: str = "distinct_topics.txt") -> Set[str]:
    try:
        content = read_azure_file(file_path)
        return {line.strip() for line in content.splitlines() if line.strip()}
    except Exception as e:
        logger.warning(f"Topic file not found or error reading file: {e}. Starting with empty taxonomy.")
        return set()

def fetch_saved_queries(file_path: str = "distinct_queries.txt") -> Set[str]:
    try:
        content = read_azure_file(file_path)
        return {line.strip() for line in content.splitlines() if line.strip()}
    except Exception as e:
        logger.warning(f"Query file not found or error reading file: {e}. Starting with empty set.")
        return set()

def process_batch(client, batch, existing_topics: set, saved_queries: set):
    from collections import defaultdict
    batch_records = batch.to_dict(orient="records")
    id_to_record = {str(rec["interaction_id"]): rec for rec in batch_records}
    # -- Prompt construction, OpenAI calls, validation, and combining results --
    # (Refer to original topic_extraction.py's process_batch for all the OpenAI prompt and validation logic)
    # Should be pasted here as-is, or further modularized if desired.
    # Make sure all referenced helper functions and config are imported.

    # [Place the full process_batch implementation here...]
    # For brevity, not fully repeated in this snippet, but you would copy your logic here.

    # Insertion into MongoDB or further consolidation can remain as in the monolith, or be further split up.
    # Return status or results as appropriate.
    pass