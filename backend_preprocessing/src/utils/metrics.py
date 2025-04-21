import dateutil.parser
from collections import defaultdict, Counter
from typing import Dict, List

def calculate_session_metrics(combined_response: List[Dict], id_to_record: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Calculates session-level metrics such as engagement level, sentiment, and duration.
    """
    session_metrics_dict = {}
    sessions = defaultdict(list)

    for entry in combined_response:
        sessions[entry["session_id"]].append(entry)

    for session_id, entries in sessions.items():
        for entry in entries:
            entry["parsed_timestamp"] = dateutil.parser.parse(id_to_record[str(entry["interaction_id"])]["timestamp"])
        sorted_entries = sorted(entries, key=lambda x: x["parsed_timestamp"])
        timestamps = [entry["parsed_timestamp"] for entry in sorted_entries]
        earliest_entry = sorted_entries[0]
        latest_entry = sorted_entries[-1]

        # Calculate session-level metrics
        num_interactions = len(sorted_entries)
        engagement_level = "Low" if num_interactions <= 3 else "Medium" if num_interactions <= 7 else "High"
        sentiments = [entry["Sentiment"] for entry in sorted_entries]
        average_sentiment = Counter(sentiments).most_common(1)[0][0] if sentiments else "None"
        topics = [entry["Topic"] for entry in sorted_entries]
        dominant_topic = Counter(topics).most_common(1)[0][0] if topics else "None"
        drop_off_sentiment = latest_entry["Sentiment"]
        total_dialog_turns = sum(entry["dialog_turns"] for entry in sorted_entries)
        duration_minutes = (max(timestamps) - min(timestamps)).total_seconds() / 60.0

        session_metrics = {
            "engagement_level": engagement_level,
            "average_user_sentiment": average_sentiment,
            "drop_off_sentiment": drop_off_sentiment,
            "dominant_topic": dominant_topic,
            "session_dialog_turns": total_dialog_turns,
            "duration_minutes": duration_minutes,
        }
        session_metrics_dict[session_id] = session_metrics

    return session_metrics_dict