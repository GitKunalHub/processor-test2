import logging
from typing import Set, Dict, List

logger = logging.getLogger(__name__)

def validate_api_response(response_data: List[Dict], valid_interaction_ids: Set[str]) -> bool:
    """
    Validates the API response for topics/errors.
    Ensures all required fields are present and interaction IDs match the expected set.
    """
    if not isinstance(response_data, list):
        logger.error("API response is not a list.")
        return False

    for record in response_data:
        if "interaction_id" not in record:
            logger.error(f"Missing 'interaction_id' in record: {record}")
            return False
        if record["interaction_id"] not in valid_interaction_ids:
            logger.error(f"Invalid 'interaction_id': {record['interaction_id']}")
            return False

    return True

def validate_api_response_rb(response_data: List[Dict], valid_interaction_ids: Set[str]) -> bool:
    """
    Validates the API response for risky behavior and queries.
    """
    return validate_api_response(response_data, valid_interaction_ids)

def validate_api_response_int(response_data: List[Dict], valid_interaction_ids: Set[str]) -> bool:
    """
    Validates the API response for intent/sentiment/emotion.
    """
    return validate_api_response(response_data, valid_interaction_ids)