import json
import requests

def read_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def query_lmstudio(prompt, model="gemma-3", max_tokens=800):
    url = "http://localhost:1234/v1/completions"
    headers = {"Content-Type": "application/json"}
    data = {
        "model": model,
        "prompt": prompt,
        "max_tokens": max_tokens,
        "temperature": 0.2,
        "stream": False
    }
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        return result.get("choices", [{}])[0].get("text", "").strip()
    except Exception as e:
        print(f"[ERROR] Failed to query LLM: {e}")
        return ""

def main():
    schema = read_json("metaData.json")
    data = read_json("cleanData.json")
    # Get expected fields from schema
    if isinstance(schema, dict) and 'tables' in schema and schema['tables']:
        columns = schema['tables'][0]['columns']
        expected_fields = [col['name'] for col in columns]
    else:
        print("[ERROR] Could not parse expected fields from metaData.json")
        return
    # Get actual fields from generated data
    if isinstance(data, list) and data:
        actual_fields = list(data[0].keys())
    else:
        print("[ERROR] cleanData.json does not contain a valid data array")
        return
    # Compose prompt for LLM
    prompt = f"""
You are a data validation expert. Compare the expected fields from the schema and the actual fields in the generated data. 

Expected fields (from schema):\n{expected_fields}\n\nActual fields (from generated data):\n{actual_fields}\n\nList any missing fields, extra fields, or mismatches. Respond ONLY with a JSON object like this: {{'missing': [...], 'extra': [...], 'match': true/false, 'explanation': '...'}}. If all fields match exactly, set 'match' to true and leave 'missing' and 'extra' empty.
"""
    result = query_lmstudio(prompt)
    print("\n=== LLM Validation Result ===\n")
    print(result)
    # Optionally, save result to file
    with open("field_validation_result.json", "w", encoding="utf-8") as f:
        f.write(result)
    # If validation is true, save a copy of cleanData.json as cleanDataQcChecked.json
    try:
        result_json = json.loads(result.replace("'", '"'))  # handle single quotes if LLM returns them
        if result_json.get('match') is True:
            with open("cleanData.json", "r", encoding="utf-8") as src, open("cleanDataQcChecked.json", "w", encoding="utf-8") as dst:
                dst.write(src.read())
            print("[✓] Validation passed. cleanDataQcChecked.json created.")
        else:
            print("[!] Validation failed. Not creating cleanDataQcChecked.json.")
    except Exception as e:
        print(f"[ERROR] Could not parse LLM result as JSON: {e}")

if __name__ == "__main__":
    main()
