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

def query_openai_gpt(prompt, ***REMOVED***, model="gpt-4o", max_tokens=800):
    import openai
    client = openai.OpenAI(***REMOVED***=***REMOVED***)
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a data validation expert. Return only a valid JSON object as described in the prompt."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=max_tokens,
            temperature=0.2
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"[ERROR] Failed to query OpenAI GPT: {e}")
        return ""

def main():
    schema = read_json("metaData.json")
    data = read_json("cleanData.json")
    
    # Build a mapping from table_name to expected fields
    table_fields = {}
    if isinstance(schema, dict) and 'tables' in schema:
        for table in schema['tables']:
            table_fields[table['table_name']] = [col['name'] for col in table['columns']]
    else:
        print("[ERROR] Could not parse expected fields from metaData.json")
        return

    # Build a mapping from table_name to records
    data_tables = {}
    if isinstance(data, list):
        for entry in data:
            if isinstance(entry, dict) and 'table_name' in entry and 'records' in entry:
                data_tables[entry['table_name']] = entry['records']
    else:
        print("[ERROR] cleanData.json does not contain a valid data array")
        return

    use_openai = True
    openai_***REMOVED*** = API_KEY

    all_results = {}
    for table_name, expected_fields in table_fields.items():
        records = data_tables.get(table_name, [])
        actual_fields = list(records[0].keys()) if records else []
        prompt = f"""
You are a data validation expert. Compare the expected fields from the schema and the actual fields in the generated data for table '{table_name}'.\n\nExpected fields (from schema):\n{expected_fields}\n\nActual fields (from generated data):\n{actual_fields}\n\nList any missing fields, extra fields, or mismatches. Respond ONLY with a JSON object like this: {{'missing': [...], 'extra': [...], 'match': true/false, 'explanation': '...'}}. If all fields match exactly, set 'match' to true and leave 'missing' and 'extra' empty."""
        if use_openai:
            result = query_openai_gpt(prompt, openai_***REMOVED***)
        else:
            result = query_lmstudio(prompt)
        print(f"\n=== LLM Validation Result for {table_name} ===\n")
        print(result)
        # Clean LLM output for JSON parsing
        def extract_json(text):
            text = re.sub(r"```(?:json)?", "", text, flags=re.IGNORECASE).strip()
            match = re.search(r"{[\s\S]*}", text)
            return match.group(0) if match else text
        try:
            clean_result = extract_json(result)
            result_json = json.loads(clean_result.replace("'", '"'))
            all_results[table_name] = result_json
        except Exception as e:
            print(f"[ERROR] Could not parse LLM result as JSON for {table_name}: {e}")
            all_results[table_name] = {"error": str(e), "raw_llm_output": result}

    # Save all results
    with open("field_validation_result.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    # If all tables match, create cleanDataQcChecked.json
    if all(r.get('match') is True for r in all_results.values()):
        with open("cleanData.json", "r", encoding="utf-8") as src, open("cleanDataQcChecked.json", "w", encoding="utf-8") as dst:
            dst.write(src.read())
        print("[✓] Validation passed for all tables. cleanDataQcChecked.json created.")
        if os.path.exists("qc_feedback.json"):
            os.remove("qc_feedback.json")
    else:
        print("[!] Validation failed for one or more tables. Not creating cleanDataQcChecked.json.")
        with open("qc_feedback.json", "w", encoding="utf-8") as fb:
            json.dump(all_results, fb, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
