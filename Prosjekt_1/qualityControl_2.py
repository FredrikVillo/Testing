import json
import requests
import os
import re

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
    # Clean LLM output for JSON parsing
    def extract_json(text):
        text = re.sub(r"```(?:json)?", "", text, flags=re.IGNORECASE).strip()
        match = re.search(r"{[\s\S]*}", text)
        return match.group(0) if match else text
    try:
        clean_result = extract_json(result)
        result_json = json.loads(clean_result.replace("'", '"'))
        if result_json.get('match') is True:
            with open("cleanData.json", "r", encoding="utf-8") as src, open("cleanDataQcChecked.json", "w", encoding="utf-8") as dst:
                dst.write(src.read())
            print("[✓] Validation passed. cleanDataQcChecked.json created.")
            if os.path.exists("qc_feedback.json"):
                os.remove("qc_feedback.json")
        else:
            print("[!] Validation failed. Not creating cleanDataQcChecked.json.")
            with open("qc_feedback.json", "w", encoding="utf-8") as fb:
                json.dump(result_json, fb, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[ERROR] Could not parse LLM result as JSON: {e}")
        with open("qc_feedback.json", "w", encoding="utf-8") as fb:
            json.dump({"error": str(e), "raw_llm_output": result}, fb, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
