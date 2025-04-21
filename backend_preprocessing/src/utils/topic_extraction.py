import logging
import json
import ast
import dateutil.parser
from collections import defaultdict, Counter
from typing import Set
import re
from models.config import Config
from utils.azure_utils import read_azure_file, write_azure_file
from utils.validation import (
    validate_api_response,
    validate_api_response_rb,
    validate_api_response_int,
)
from utils.metrics import calculate_session_metrics

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

def process_batch(client, batch, existing_topics: Set[str], saved_queries: Set[str]):
    """
    Process a batch of interactions for topic extraction, risk analysis, and sentiment detection.
    """
    try:
        batch_records = batch.to_dict(orient="records")
        id_to_record = {str(rec["interaction_id"]): rec for rec in batch_records}

        # Step 1: Call OpenAI API for topics, errors, and resolution
        topics_response = client.chat.completions.create(
            model=Config.MODEL_NAME,
            temperature=Config.TEMPERATURE,
            messages=[{"role": "system", "content": "SYSTEM_MESSAGE"}, {"role": "user", "content": "PROMPT"}]
        )
        topics_data = parse_response(topics_response, validate_api_response, id_to_record)

        # Step 2: Call OpenAI API for risky behavior and queries
        risky_behavior_response = client.chat.completions.create(
            model=Config.MODEL_NAME,
            temperature=Config.TEMPERATURE,
            messages=[{"role": "system", "content": "SYSTEM_MESSAGE_QUERIES"}, {"role": "user", "content": "PROMPT"}]
        )
        risky_behavior_data = parse_response(risky_behavior_response, validate_api_response_rb, id_to_record)

        # Step 3: Call OpenAI API for intent/sentiment/emotion
        intent_response = client.chat.completions.create(
            model=Config.MODEL_NAME,
            temperature=Config.TEMPERATURE,
            messages=[{"role": "system", "content": "SYSTEM_MESSAGE_INTENT"}, {"role": "user", "content": "PROMPT"}]
        )
        intent_data = parse_response(intent_response, validate_api_response_int, id_to_record)

        # Step 4: Combine responses
        combined_response = combine_responses(topics_data, risky_behavior_data, intent_data, id_to_record)

        # Step 5: Group by session and calculate metrics
        session_metrics_dict = calculate_session_metrics(combined_response, id_to_record)

        # Step 6: Merge session metrics into responses
        final_records = merge_session_metrics(combined_response, session_metrics_dict)

        # Step 7: Save results to Azure or MongoDB
        save_results_to_azure(final_records)

        return final_records
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        raise

def parse_response(response, validation_func, id_to_record):
    """
    Parses and validates the API response.
    """
    content = response.choices[0].message.content.strip()
    match = re.search(r'\[.*\]', content, re.DOTALL)
    if not match:
        raise ValueError("No JSON array found in the response.")
    json_str = match.group(0)
    response_data = json.loads(json_str)
    if not validation_func(response_data, {str(rec["interaction_id"]) for rec in id_to_record.values()}):
        raise ValueError("Invalid API response structure.")
    return response_data

def combine_responses(topics_data, risky_behavior_data, intent_data, id_to_record):
    """
    Combines the API responses into a unified structure.
    """
    combined_response = []
    for topic_entry in topics_data:
        interaction_id = topic_entry["interaction_id"]
        rb_entry = next((rb for rb in risky_behavior_data if rb["interaction_id"] == interaction_id), None)
        int_entry = next((intent for intent in intent_data if intent["interaction_id"] == interaction_id), None)
        if rb_entry and int_entry:
            original_record = id_to_record.get(interaction_id, {})
            combined_entry = {
                **topic_entry,
                **rb_entry,
                **int_entry,
                "session_id": original_record.get("session_id"),
                "timestamp": original_record.get("timestamp"),
                "interactions": original_record.get("interactions", "[]"),
            }
            combined_response.append(combined_entry)
    return combined_response

def merge_session_metrics(combined_response, session_metrics_dict):
    """
    Merges session-level metrics into each interaction record.
    """
    final_records = []
    for entry in combined_response:
        sess_metrics = session_metrics_dict.get(entry["session_id"], {})
        merged_entry = {**entry, **sess_metrics}
        final_records.append(merged_entry)
    return final_records

def save_results_to_azure(final_records):
    """
    Saves the final records to Azure Blob Storage.
    """
    output_blob_name = "processed_interactions.json"
    output_content = json.dumps(final_records, indent=2)
    write_azure_file(output_blob_name, output_content)
    logger.info(f"Saved processed interactions to {output_blob_name}.")