import re
import json
import os

def extract_token_usage(output):
    """Extract token usage from OpenAI API response or print output."""
    # Example: '..."usage": {"prompt_tokens": 123, "completion_tokens": 456, "total_tokens": 579}...'
    match = re.search(r'"total_tokens"\s*:\s*(\d+)', output)
    if match:
        return int(match.group(1))
    return 0


def update_pipeline_status(step, status, tokens, status_path="pipeline_status.json"):
    if os.path.exists(status_path):
        with open(status_path, "r") as f:
            data = json.load(f)
    else:
        data = {"steps": [], "total_tokens": 0}

    data["steps"].append({"step": step, "status": status, "tokens": tokens})
    data["total_tokens"] += tokens

    with open(status_path, "w") as f:
        json.dump(data, f, indent=2)


def reset_pipeline_status(status_path="pipeline_status.json"):
    with open(status_path, "w") as f:
        json.dump({"steps": [], "total_tokens": 0}, f, indent=2)
