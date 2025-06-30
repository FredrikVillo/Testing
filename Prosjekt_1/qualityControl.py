import json
import os
import re
from typing import List, Dict, Any, Optional
import requests
import glob
import sys

def read_json_file(path: str) -> Any:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_json(data: Any, path: str) -> None:
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def query_lmstudio(prompt: str, model: str = "gemma-3", max_tokens: int = 1500, temperature: float = 0.5) -> str:
    url = "http://localhost:1234/v1/completions"
    headers = {"Content-Type": "application/json"}
    data = {
        "model": model,
        "prompt": prompt,
        "max_tokens": max_tokens,
        "temperature": temperature,
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

def clean_llm_json_response(text: str) -> str:
    cleaned = re.sub(r"```(?:json)?\n?|```", "", text, flags=re.IGNORECASE).strip()
    if "{" in cleaned:
        cleaned = cleaned[cleaned.find('{'):]
    if "}" in cleaned:
        cleaned = cleaned[:cleaned.rfind('}') + 1]
    return cleaned

def try_parse_llm_response(llm_response: str) -> Optional[Dict[str, Any]]:
    cleaned = clean_llm_json_response(llm_response)
    try:
        return json.loads(cleaned)
    except Exception:
        return None

def check_and_enhance_table(table: Dict[str, Any], model: str = "gemma-3") -> Dict[str, Any]:
    # Compose a prompt for the LLM to check and enhance the table schema
    prompt = (
        "You are a data privacy and schema quality expert.\n"
        "Given the following table schema in JSON, check if the PII information (is_pii, pii_reason) is correct for each column.\n"
        "If any column is missing PII information, add it.\n"
        "If the schema format is wrong, fix it.\n"
        "Output only the corrected JSON object, starting with { and ending with }.\n\n"
        f"Table schema:\n{json.dumps(table, ensure_ascii=False, indent=2)}"
    )
    llm_response = query_lmstudio(prompt, model=model)
    checked = try_parse_llm_response(llm_response)
    if checked and 'table_name' in checked and 'columns' in checked:
        return checked
    # fallback: return original if LLM fails
    return table

def quality_control(input_path: str, output_path: str, model: str = "gemma-3"):
    data = read_json_file(input_path)
    tables = data.get('tables', data if isinstance(data, list) else [])
    enhanced_tables = []
    for table in tables:
        # Try to handle both dict and raw_response
        if isinstance(table, dict) and 'columns' in table:
            enhanced = check_and_enhance_table(table, model=model)
            enhanced_tables.append(enhanced)
        else:
            enhanced_tables.append(table)
    result = {'tables': enhanced_tables}
    save_json(result, output_path)
    # Also save as metaData.json (copy)
    meta_path = "metaData.json"
    save_json(result, meta_path)
    print(f"[✓] Quality control complete. Output saved to {output_path} and {meta_path}")

if __name__ == "__main__":
    # Automatically use the latest analysis file from databaseAnalyser
    # Example: schema_TestTinyPII_analysis.json -> schema_TestTinyPII_qualitychecked.json
    # Find the most recent *_analysis.json file in the current directory

    analysis_files = sorted(glob.glob("*_analysis.json"), key=os.path.getmtime, reverse=True)
    if not analysis_files:
        print("No *_analysis.json file found in the current directory.")
        sys.exit(1)
    input_path = analysis_files[0]
    output_path = input_path.replace("_analysis.json", "_qualitychecked.json")
    print(f"[i] Using input: {input_path}\n[i] Output will be: {output_path}")
    model = "gemma-3"
    quality_control(input_path, output_path, model)