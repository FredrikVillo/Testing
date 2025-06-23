import requests
import pyodbc
import json
import uuid
import re
import time
import random
import string

prompt = """
You are generating a synthetic user record for testing a secure HR database. Use realistic, diverse, and plausible names (e.g., Emma Johansson, Lars Pettersson, Sofia Berg, Erik Hansen, Anna Lindqvist, etc). Usernames should look like real corporate usernames (e.g., emma.johansson, lars.p, sofia.berg, erik.hansen, anna.lindqvist, etc). Follow this schema:
{
  "POLICY": null,
  "EMPLOYEE": "Name",
  "USERNAME": "UniqueUser",
  "PASSWORD": "SHA256-hash",
  "DESCRIPTION": null or "Job title",
  "FAILEDLOGON": Integer,
  "SESSIONID": null,
  "ACCESSDISABLED": 0 or 1,
  "LASTLOGON": "yyyy-mm-dd hh:mm:ss.fff" or null,
  "USERPROTECT": null or Integer,
  "LANGUAGE": Integer (1 = en, 6 = no, etc),
  "DEFAULTDETAILS": null,
  "DN": Integer ID,
  "ORGANIZATION": null,
  "EDITOR": null,
  "DATEFORMAT": 4,
  "PWDCHANGE": "yyyy-mm-dd hh:mm:ss.fff",
  "MODIFIED": "yyyy-mm-dd hh:mm:ss.fff",
  "CREATED": "yyyy-mm-dd hh:mm:ss.fff",
  "ISFIRSTTIMELOGON": null,
  "EMPLOYEE_ID": int (starting from 10000),
  "KEYBASED_SSO_KEY": null,
  "IMPORT_MODIFIED": null,
  "LOCKEDTIME": null,
  "PRIMARY_PROFILE": null,
  "PROFILE_ID": null,
  "SECRET_KEY": "Random string",
  "GUID": UUID,
  "DEACTIVATED_ON": null or date,
  "ACCOUNT_GUID": UUID,
  "USED_INTEGRATION_IDS": null,
  "IS_DELETED": 0 or 1,
  "IS_ANONYMIZED": 0 or 1
}
Give the response as a valid JSON object, 1 entry only.
"""

def query_ollama(prompt):
    url = "http://localhost:11434/api/generate"
    headers = {"Content-Type": "application/json"}
    data = {"model": "gemma3:1b", "prompt": prompt, "stream": False}
    response = requests.post(url, headers=headers, json=data)
    result = response.json()
    return result.get("response", "").strip()

def extract_json(text):
    text = text.strip()
    if text.startswith('```'):
        text = text.replace('```json', '').replace('```', '').strip()
    match = re.search(r'\{[\s\S]*\}', text)
    return match.group(0) if match else text

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=DESKTOP-R9S4CFK;"
    "DATABASE=HR_Synthetic;"
    "UID=sa;"
    "PWD=(catalystone123);"
    "TrustServerCertificate=yes;"
    "Encrypt=no;"
)

def is_valid_datetime(val):
    if not isinstance(val, str):
        return False
    # Accepts 'YYYY-MM-DD HH:MM:SS.fff' or 'YYYY-MM-DD' (SQL Server compatible)
    import re
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2}(\.\d{1,3})?)?$", val))

def insert_to_db(records):
    db_fields = [
        "POLICY", "EMPLOYEE", "USERNAME", "PASSWORD", "DESCRIPTION", "FAILEDLOGON", "SESSIONID", "ACCESSDISABLED", "LASTLOGON",
        "USERPROTECT", "LANGUAGE", "DEFAULTDETAILS", "DN", "ORGANIZATION", "EDITOR", "DATEFORMAT", "PWDCHANGE", "MODIFIED", "CREATED",
        "ISFIRSTTIMELOGON", "EMPLOYEE_ID", "KEYBASED_SSO_KEY", "IMPORT_MODIFIED", "LOCKEDTIME", "PRIMARY_PROFILE", "PROFILE_ID", "SECRET_KEY", "GUID",
        "DEACTIVATED_ON", "ACCOUNT_GUID", "USED_INTEGRATION_IDS", "IS_DELETED", "IS_ANONYMIZED"
    ]
    date_fields = [
        "LASTLOGON", "PWDCHANGE", "MODIFIED", "CREATED", "LOCKEDTIME", "DEACTIVATED_ON", "IMPORT_MODIFIED"
    ]
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        for rec in records:
            # UUID fields
            for key in ["GUID", "ACCOUNT_GUID", "SESSIONID", "PROFILE_ID"]:
                val = rec.get(key)
                try:
                    rec[key] = str(uuid.UUID(str(val))) if val else str(uuid.uuid4())
                except Exception:
                    rec[key] = str(uuid.uuid4())
            # Int/bit fields
            for key in ["IS_DELETED", "IS_ANONYMIZED", "ACCESSDISABLED", "FAILEDLOGON", "USERPROTECT", "LANGUAGE", "DATEFORMAT"]:
                val = rec.get(key)
                try:
                    rec[key] = int(val) if val is not None else 0
                except Exception:
                    rec[key] = 0
            # Clean date/time fields
            for key in date_fields:
                val = rec.get(key)
                if not is_valid_datetime(val):
                    rec[key] = None
            # Ensure USED_INTEGRATION_IDS is always a string
            if isinstance(rec.get("USED_INTEGRATION_IDS"), list) or rec.get("USED_INTEGRATION_IDS") is None:
                rec["USED_INTEGRATION_IDS"] = ""
            # Ensure SECRET_KEY is always a non-empty string
            if not rec.get("SECRET_KEY"):
                rec["SECRET_KEY"] = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
            # Fill missing fields with None
            for key in db_fields:
                if key not in rec:
                    rec[key] = None
            values = tuple(rec.get(field) for field in db_fields)
            # Debug print all field types and values before insert
            print("[DEBUG] Field types and values for insert:")
            for field, value in zip(db_fields, values):
                print(f"  {field}: type={type(value)}, value={value}")
            cursor.execute(f"""
                INSERT INTO AccessCatalystUsers (
                    {', '.join(db_fields)}
                ) VALUES ({', '.join(['?']*len(db_fields))})
            """, values)
        conn.commit()
        print(f"Inserted {len(records)} record(s) into AccessCatalystUsers table.")
    except Exception as db_err:
        print(f"Database insert error: {db_err}")
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass

def get_next_employee_id():
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT ISNULL(MAX(EMPLOYEE_ID), 9999) FROM AccessCatalystUsers")
        max_id = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return max_id + 1
    except Exception:
        return 10000

def main():
    start_time = time.time()
    try:
        num_entries = int(input("How many synthetic user records to generate? "))
    except Exception:
        num_entries = 1
    all_results = []
    used_names = set()
    used_usernames = set()
    next_id = get_next_employee_id()
    for i in range(num_entries):
        # Add uniqueness constraint to prompt
        current_prompt = prompt
        if used_names or used_usernames:
            current_prompt += (
                "\nPreviously used names: " + ", ".join(sorted(used_names)) +
                ". Previously used usernames: " + ", ".join(sorted(used_usernames)) +
                ". Generate a new, unique user not in these lists."
            )
        print(f"\n=== Generating entry {i+1} of {num_entries} ===")
        raw_output = query_ollama(current_prompt)
        try:
            json_candidate = extract_json(raw_output)
            if json_candidate.count('{') > 0 and json_candidate.count('}') > 0:
                last_brace = json_candidate.rfind('}')
                json_candidate = json_candidate[:last_brace+1]
            result = json.loads(json_candidate)
            # Ensure unique EMPLOYEE_ID for each record in this batch
            result['EMPLOYEE_ID'] = next_id + i
            print(json.dumps(result, indent=2))
            all_results.append(result)
            # Track used names and usernames
            if result.get('EMPLOYEE'):
                used_names.add(result['EMPLOYEE'])
            if result.get('USERNAME'):
                used_usernames.add(result['USERNAME'])
        except Exception as e:
            print("Model output error:", e)
            print(json_candidate if 'json_candidate' in locals() else raw_output)
    if all_results:
        insert_to_db(all_results)
    elapsed = time.time() - start_time
    print(f"\n⏱️ Total time elapsed: {elapsed:.2f} seconds")

if __name__ == "__main__":
    main()
