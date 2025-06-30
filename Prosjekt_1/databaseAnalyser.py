import requests
import json
import re
from typing import List, Dict, Any, Optional

filepath = "c:/Users/FredrikVillo/repos/TestDataGeneration/Prosjekt_1/schema_TestTinyPII.txt"

def read_schema_from_file(file_path: str) -> str:
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()

def query_lmstudio(prompt: str, model: str = "gemma-3", max_tokens: int = 4000, temperature: float = 0.5) -> str:
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
    # Remove any leading text before the first '{'
    if '{' in cleaned:
        cleaned = cleaned[cleaned.find('{'):]
    # Remove any trailing text after the last '}'
    if '}' in cleaned:
        cleaned = cleaned[:cleaned.rfind('}') + 1]
    try:
        parsed = json.loads(cleaned)
        # Accept both {table_name, columns} and {table: {...}}
        if 'table' in parsed and isinstance(parsed['table'], dict):
            return parsed['table']
        if 'table_name' in parsed and 'columns' in parsed:
            return parsed
        # Accept {table: [...], ...} (array style)
        if 'table' in parsed and isinstance(parsed['table'], list):
            return {'table_name': 'Unknown', 'columns': parsed['table']}
    except Exception:
        pass
    # Try to extract JSON from within the text (fallback)
    return extract_json_from_text(llm_response)

def extract_json_from_text(text: str) -> Optional[Dict[str, Any]]:
    json_start = text.find('{')
    json_end = text.rfind('}') + 1
    if json_start != -1 and json_end > json_start:
        try:
            possible_json = text[json_start:json_end]
            parsed = json.loads(possible_json)
            if 'table' in parsed and isinstance(parsed['table'], dict):
                return parsed['table']
            if 'table_name' in parsed and 'columns' in parsed:
                return parsed
            if 'table' in parsed and isinstance(parsed['table'], list):
                return {'table_name': 'Unknown', 'columns': parsed['table']}
        except Exception:
            return None
    return None

def chunk_schema_by_table(schema_text: str) -> List[str]:
    table_chunks = []
    current = []
    for line in schema_text.splitlines():
        if line.strip().startswith('Table: '):
            if current:
                table_chunks.append('\n'.join(current))
                current = []
        if line.strip() or current:
            current.append(line)
    if current:
        table_chunks.append('\n'.join(current))
    return table_chunks

def analyse_schema(schema: str, max_retries: int = 2) -> List[Dict[str, Any]]:
    table_chunks = chunk_schema_by_table(schema)
    all_tables = []


    #Refine the prompt to get better responses
    for chunk in table_chunks:
        prompt = (
        "You are a strict JSON formatter. You must output only a single JSON object — starting with '{' and ending with '}'. Do NOT include explanations, markdown, comments, or any extra text.\n\n"
        "Instructions:\n"
        "- Output a JSON object with two keys: 'table_name' (string) and 'columns' (list of column definitions).\n"
        "- Each column definition must include:\n"
        "    - name (string)\n"
        "    - type (string)\n"
        "    - key_type (string or null)\n"
        "    - is_pii (true or false)\n"
        "    - pii_reason (string or null)\n"
        "- Treat as PII: first/last/full names, emails, phone numbers, usernames, government-issued IDs (SSN, passport, etc.), birthdates, addresses, IP/MAC addresses, biometric or financial info, and any field that may uniquely identify a person.\n"
        "- Use your best judgment when unsure. If PII, explain briefly why in pii_reason. If not, set pii_reason to null.\n"
        "- Follow the format exactly. Output only the JSON object.\n\n"
        f"Schema:\n{chunk}"
    )


        llm_response = query_lmstudio(prompt)
        parsed = try_parse_llm_response(llm_response)
        retry_count = 0

        while parsed is None and retry_count < max_retries:
            retry_prompt = (
                prompt + "\n\nREMINDER: Return ONLY a valid JSON object. No markdown, comments, or extra text."
            )
            llm_response = query_lmstudio(retry_prompt)
            parsed = try_parse_llm_response(llm_response)
            retry_count += 1

        if parsed and 'table_name' in parsed and 'columns' in parsed:
            all_tables.append(parsed)
        else:
            all_tables.append({'raw_response': llm_response})

    return all_tables

def save_json(data: Any, path: str) -> None:
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    schema = read_schema_from_file(filepath)
    parsed_tables = analyse_schema(schema)

    # Save raw result
    output_analysis = filepath.replace('.txt', '_analysis.json')
    save_json({'tables': parsed_tables}, output_analysis)
    print(f"[✓] Analysis result saved to {output_analysis}")

    # Save generator-friendly schema
    output_generator = filepath.replace('.txt', '_generator_schema.json')
    save_json({'tables': parsed_tables}, output_generator)
    print(f"[✓] Generator schema saved to {output_generator}")

if __name__ == "__main__":
    main()
