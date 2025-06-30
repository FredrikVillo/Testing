from abc import ABC, abstractmethod
import json
import requests
import sys
import os
import re

# === Step 1: Core Generator Interface ===
class DataGeneratorBase(ABC):
    @abstractmethod
    def generate_record(self, metadata: dict, table_name: str) -> dict:
        pass


# === Step 2: Gemma 3 12B High-Quality Generator ===
class GemmaGenerator(DataGeneratorBase):
    def __init__(self, llm_interface):
        self.llm = llm_interface  # Should support generate(prompt) -> str

    def generate_record(self, metadata: dict, table_name: str) -> list:
        prompt = self._build_prompt(metadata, table_name)
        return self.llm.generate(prompt)

    def _build_prompt(self, metadata: dict, table_name: str) -> str:
        # Find the correct table by name
        if isinstance(metadata, dict) and 'tables' in metadata:
            table = next((t for t in metadata['tables'] if t['table_name'] == table_name), None)
            if table is None:
                raise ValueError(f"Fant ikke tabellen '{table_name}' i metadata.")
            field_list = [(col['name'], col['type']) for col in table['columns']]
        else:
            raise ValueError("Ugyldig metadataformat. Forventet 'tables' nøkkel.")

        fields_text = ", ".join([f"{name} ({type_})" for name, type_ in field_list])
        field_names_only = ", ".join([f"'{name}'" for name, _ in field_list])

        return f"""
        Du skal generere ett realistisk og GDPR-trygt testdatasett som kun inneholder følgende felter:
        {fields_text}

        Ikke legg til noen ekstra felter.
        Returner en JSON-liste med ett objekt, som har nøyaktig disse feltene: [{field_names_only}]
        Svar kun med gyldig JSON, uten kommentarer eller forklaringer.
        """


# === Step 3: LLM Interface ===
class LocalLLM:
    def __init__(self, endpoint_url: str):
        self.endpoint = endpoint_url

    def generate(self, prompt: str) -> list:
        response = requests.post(
            self.endpoint,
            json={"prompt": prompt, "max_tokens": 600}
        )

        # Lagre hele responsen til debugging
        with open("derivedData.json", "w", encoding="utf-8") as f:
            try:
                json.dump(response.json(), f, ensure_ascii=False, indent=2)
            except Exception as e:
                f.write(str(response.text))

        try:
            text = response.json()["choices"][0]["text"]
        except Exception:
            return [{"error": "Model response mangler 'choices[0][text]'"}]

        # Rens outputen
        clean_text = re.sub(r"```json|```", "", text, flags=re.IGNORECASE).strip()
        match = re.search(r"\[\s*{.*?}\s*\]", clean_text, re.DOTALL)

        if match:
            try:
                parsed = json.loads(match.group(0))
                with open("cleanData.json", "w", encoding="utf-8") as out:
                    json.dump(parsed, out, ensure_ascii=False, indent=2)
                return parsed
            except Exception as e:
                return [{"error": f"Kunne ikke parse JSON-array: {e}"}]

        return [{"error": "Fant ikke gyldig JSON-liste i modellens output."}]


# === Step 4: Main program ===
if __name__ == "__main__":
    metadata_file = "metaData.json"
    table_name = "dbo.CustomerData"  # Kan gjøres dynamisk senere

    if not os.path.isfile(metadata_file):
        print(f"Metadatafil '{metadata_file}' finnes ikke.")
        sys.exit(1)

    with open(metadata_file, 'r', encoding='utf-8') as f:
        sample_metadata = json.load(f)

    generator = GemmaGenerator(LocalLLM("http://localhost:1234/v1/completions"))
    try:
        records = generator.generate_record(sample_metadata, table_name)
    except ValueError as e:
        print(f"Feil: {e}")
        sys.exit(1)

    print("\n✅ Generert datasett:\n")
    print(json.dumps(records, indent=2, ensure_ascii=False))

    with open("derived_schema.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
