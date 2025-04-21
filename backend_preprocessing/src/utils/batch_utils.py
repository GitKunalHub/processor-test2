import tiktoken

def count_tokens(text, encoding="", default_encoder="cl100k_base"):
    if not encoding:
        encoding = tiktoken.get_encoding(default_encoder)
    return len(encoding.encode(text))

def find_max_records_within_limit_custom(data, encoding, max_tokens, reserved_model_response_tokens):
    static_tokens = reserved_model_response_tokens
    running_token_count = static_tokens
    valid_ids = []
    for record in data:
        record_tokens = count_tokens(record["interactions"], encoding)
        if record_tokens > max_tokens - static_tokens:
            print(f"Skipping record {record['id']}: Exceeds token limit")
            continue
        if running_token_count + record_tokens > max_tokens:
            break
        running_token_count += record_tokens
        valid_ids.append(record["interaction_id"])
    return valid_ids

def process_records_in_batches_by_session(records, max_tokens=1024, reserved_model_response_tokens=200, encoding_name="cl100k_base"):
    encoding = tiktoken.get_encoding(encoding_name)
    batches = []
    current_batch = []
    running_token_count = reserved_model_response_tokens

    sessions = {}
    for record in records:
        session = record["session_id"]
        sessions.setdefault(session, []).append(record)
    sessions = sessions.items()
    for session_id, group in sessions:
        session_token_count = sum(count_tokens(rec["interactions"], encoding) for rec in group)
        if session_token_count > max_tokens - reserved_model_response_tokens:
            for rec in group:
                rec_tokens = count_tokens(rec["interactions"], encoding)
                if running_token_count + rec_tokens > max_tokens:
                    if current_batch:
                        batches.append([r["interaction_id"] for r in current_batch])
                    current_batch = [rec]
                    running_token_count = reserved_model_response_tokens + rec_tokens
                else:
                    current_batch.append(rec)
                    running_token_count += rec_tokens
        else:
            if running_token_count + session_token_count > max_tokens:
                if current_batch:
                    batches.append([r["interaction_id"] for r in current_batch])
                current_batch = group.copy()
                running_token_count = reserved_model_response_tokens + session_token_count
            else:
                current_batch.extend(group)
                running_token_count += session_token_count
    if current_batch:
        batches.append([r["interaction_id"] for r in current_batch])
    return batches