from pydantic import BaseModel
from typing import Optional

class Interaction(BaseModel):
    interaction_id: str
    session_id: str
    user_id: Optional[str]
    interactions: str
    timestamp: Optional[str]
    # Add any other fields as needed