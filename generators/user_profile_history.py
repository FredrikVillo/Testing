import requests
import json
from faker import Faker
import random
import pandas as pd
import datetime as dt
import time

fake = Faker("no_NO")

# Set the LLM model name here to switch models easily
LLM_MODEL_NAME = "google/gemma-3-12b"  # e.g., "phi-2", "google/gemma-3-12b", etc.

def query_LLM(prompt):
    """Send a prompt to the Lmstudio API and return the response text. Includes debug output and fallback."""
    url = "http://localhost:1234/v1/chat/completions"  # Update if your Lmstudio runs elsewhere
    headers = {"Content-Type": "application/json"}
    data = {
        "model": LLM_MODEL_NAME,  # Use the global model name variable
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 128,
        "temperature": 0.5
    }
    print(f"\n🔗 [DEBUG] Sending POST to {url}\nPayload: {json.dumps(data, ensure_ascii=False)}")
    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        print(f"🔄 [DEBUG] Status code: {response.status_code}")
        print(f"🔄 [DEBUG] Raw response: {response.text}")
        response.raise_for_status()
        try:
            result = response.json()
            print(f"🟢 [DEBUG] Parsed JSON: {json.dumps(result, ensure_ascii=False)}")
            content = result["choices"][0]["message"]["content"]
            print(f"🟢 [DEBUG] API content: {content}")
            # --- Fix: Remove markdown code fences if present ---
            content = content.strip()
            if content.startswith("```"):
                # Remove all code fences and 'json' markers
                content = content.replace('```json', '').replace('```', '').strip()
            return content
        except Exception as parse_err:
            print(f"❌ [DEBUG] Failed to parse JSON or extract content: {parse_err}")
            print(f"❌ [DEBUG] Full response text: {response.text}")
            return '{"PositionTitle": "API_ERROR", "Department": "API_ERROR"}'
    except Exception as e:
        print(f"❌ [DEBUG] API call failed: {e}")
        return '{"PositionTitle": "API_ERROR", "Department": "API_ERROR"}'


def make_user_profile_history(n_profiles=10, seed=42):
    random.seed(seed)
    Faker.seed(seed)
    rows = []
    today = dt.date.today()
    fields = [
        "ACCESSCATALYST", "EMPLOYEE", "USERNAME", "PASSWORD", "POLICY", "DESCRIPTION", "FAILEDLOGON", "SESSIONID", "ACCESSDISABLED", "LASTLOGON", "USERPROTECT", "LANGUAGE", "DEFAULTDETAILS", "DN", "ORGANIZATION", "EDITOR", "DATEFORMAT", "PWDCHANGE", "MODIFIED", "CREATED", "ISFIRSTTIMELOGON", "EMPLOYEE_ID", "KEYBASED_SSO_KEY", "IMPORT_MODIFIED", "LOCKEDTIME", "DELETED", "PRIMARY_PROFILE", "ISPRIMARY", "PROFILE_ID", "SECRET_KEY"
    ]
    for _ in range(n_profiles):
        prompt = (
            "Generate a realistic synthetic user access profile for a Norwegian application. "
            "Respond with a single-line JSON object with the following fields only, no markdown or explanation. "
            f"Fields: {fields}. "
            "Use plausible values for each field. Dates should be in ISO format. Boolean fields as 0/1. "
            "USERNAME must not contain # or '. "
            "EMPLOYEE_ID must match EMPLOYEE. "
            "Example: {\"ACCESSCATALYST\":\"...\",\"EMPLOYEE\":\"...\",...}"
        )
        try:
            llm_response = query_LLM(prompt)
            llm_response = llm_response.splitlines()[0].strip()
            row = json.loads(llm_response)
            if isinstance(row, list):
                row = row[0] if row else {}
        except Exception:
            # Fallback: fill with fake/random data
            row = {f: "" for f in fields}
        # Ensure all fields are present
        for f in fields:
            if f not in row:
                row[f] = ""
        rows.append(row)
    return pd.DataFrame(rows)


if __name__ == "__main__":
    import pandas as pd
    import time
    start_time = time.time()
    n_profiles = 1  # Set how many user profiles you want
    df = make_user_profile_history(n_profiles)
    print(df)
    df.to_csv("out/user_profile_history.csv", index=False)
    print("Saved out/user_profile_history.csv")
    elapsed = time.time() - start_time
    print(f"⏱️ Time used: {elapsed:.2f} seconds")



