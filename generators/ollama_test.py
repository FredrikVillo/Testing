import requests
import json
import time

def run_ollama_test():
    access_catalyst_prompt = """
You are generating a synthetic user record for testing a secure HR database. Follow this schema:

{
  \"POLICY\": null,
  \"EMPLOYEE\": \"Name\",
  \"USERNAME\": \"UniqueUser\",
  \"PASSWORD\": \"SHA256-hash\",
  \"DESCRIPTION\": null or \"Job title\",
  \"FAILEDLOGON\": Integer,
  \"SESSIONID\": null,
  \"ACCESSDISABLED\": 0 or 1,
  \"LASTLOGON\": \"yyyy-mm-dd hh:mm:ss.fff\" or null,
  \"USERPROTECT\": null or Integer,
  \"LANGUAGE\": Integer (1 = en, 6 = no, etc),
  \"DEFAULTDETAILS\": null,
  \"DN\": Integer ID,
  \"ORGANIZATION\": null,
  \"EDITOR\": null,
  \"DATEFORMAT\": 4,
  \"PWDCHANGE\": \"yyyy-mm-dd hh:mm:ss.fff\",
  \"MODIFIED\": \"yyyy-mm-dd hh:mm:ss.fff\",
  \"CREATED\": \"yyyy-mm-dd hh:mm:ss.fff\",
  \"ISFIRSTTIMELOGON\": null,
  \"EMPLOYEE_ID\": int (starting from 10000),
  \"KEYBASED_SSO_KEY\": null,
  \"IMPORT_MODIFIED\": null,
  \"LOCKEDTIME\": null,
  \"PRIMARY_PROFILE\": null,
  \"PROFILE_ID\": null,
  \"SECRET_KEY\": \"Random string\",
  \"GUID\": UUID,
  \"DEACTIVATED_ON\": null or date,
  \"ACCOUNT_GUID\": UUID,
  \"USED_INTEGRATION_IDS\": null,
  \"IS_DELETED\": 0 or 1,
  \"IS_ANONYMIZED\": 0 or 1
}

Give the response as a valid JSON object, 1 entry only. Respond with only the JSON object, no explanation, no code block.
"""
    start_time = time.time()
    res = requests.post(
        "http://localhost:11434/api/generate",
        json={
            "model": "gemma3:1b",
            "prompt": access_catalyst_prompt,
            "stream": False
        }
    )
    elapsed = time.time() - start_time
    data = res.json()
    if "response" in data:
        print(data["response"])
    else:
        print("[ERROR] Ollama did not return a 'response' key. Full response:")
        print(json.dumps(data, indent=2))
    print(f"\n⏱️ Model response time: {elapsed:.2f} seconds")

def list_ollama_models():
    res = requests.get("http://localhost:11434/api/tags")
    data = res.json()
    print("Available models:")
    for model in data.get("models", []):
        print("-", model.get("name"))

if __name__ == "__main__":
    print("Listing available Ollama models:")
    list_ollama_models()
    print("\n---\n")
    run_ollama_test()
