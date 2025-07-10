import random
import json
from faker import Faker
from datetime import datetime, timedelta
import pandas as pd
from openai import AzureOpenAI
import sys
import os

fake = Faker()

# Load USERPROFILE_FIELD and ACCESSCATALYST from userprofile_data.json (bruker korrekt struktur)
userprofile_path = "../aiConversions/data/output/latest/userprofile_data.json"
with open(userprofile_path, 'r', encoding='utf-8') as f:
    userprofiles = json.load(f)

profile_pairs = [(row['USERFIELD'], row['ACCESSCATALYST']) for row in userprofiles]

# Azure OpenAI setup
api_key_path = "C:/Users/FredrikVillo/repos/TestDataGeneration/api_key.txt"
with open(api_key_path, "r") as f:
    api_key = f.read().strip()

client = AzureOpenAI(
    api_key=api_key,
    api_version="2025-01-01-preview",
    azure_endpoint="https://azureopenai-sin-dev.openai.azure.com"
)

def generate_reason(field_value):
    if dry_run:
        return fake.sentence(nb_words=8)
    prompt = (
        f"A user profile field value is now '{field_value}'. "
        "Provide a concise and realistic reason for this value in an HR system. "
        "Avoid verbose explanations and focus on the context of the value. "
        "For example, consider role transitions, skill updates, or personal interest changes."
    )
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=50,
            temperature=0.7
        )
        return response.choices[0].message.content.strip()
    except:
        return f"Profile value set to '{field_value}' for HR context."

value_pool = ['Python', 'SQL', 'Excel', 'Java', 'Power BI', 'Running', 'Cycling', 'Photography', 'Hiking', 'AWS Certified', 'PMP', 'Scrum Master', 'Azure Fundamentals']

history_data = []
dry_run = '--dry-run' in sys.argv

# In dry run: use only a few entries and no AI calls
def generate_reason(field_value):
    if dry_run:
        return fake.sentence(nb_words=8)
    prompt = (
        f"A user profile field value is now '{field_value}'. "
        "Provide a concise and realistic reason for this value in an HR system. "
        "Avoid verbose explanations and focus on the context of the value. "
        "For example, consider role transitions, skill updates, or personal interest changes."
    )
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=50,
            temperature=0.7
        )
        return response.choices[0].message.content.strip()
    except:
        return f"Profile value set to '{field_value}' for HR context."

if dry_run:
    # Only a few entries for dry run
    sample_pairs = profile_pairs[:2]
else:
    sample_pairs = profile_pairs
for userfield, accesscatalyst in sample_pairs:
    num_entries = random.randint(1, 3) if not dry_run else 1
    for _ in range(num_entries):
        field_value = random.choice(value_pool)
        valid_from = datetime.now() - timedelta(days=random.randint(500, 1000))
        changed_date = valid_from + timedelta(days=random.randint(10, 100))
        valid_to = changed_date + timedelta(days=random.randint(30, 180))
        ai_reason = generate_reason(field_value)
        entry = {
            "USERPROFILE_FIELD": userfield,
            "ACCESSCATALYST": accesscatalyst,
            "FIELD_VALUE": field_value,
            "CHANGED_BY": random.randint(1, 1000),
            "CHANGED_DATE": changed_date.strftime("%Y-%m-%d %H:%M:%S"),
            "CHANGED_BY_DEPUTY": random.randint(1, 1000),
            "CHANGE_TYPE": random.randint(1, 5),
            "VALID_FROM": valid_from.strftime("%Y-%m-%d"),
            "APPROVED_BY": random.randint(1, 1000),
            "VALID_TO": valid_to.strftime("%Y-%m-%d"),
            "IS_TIMELINE": random.choice([0, 1]),
            "APPROVED_ON": (changed_date + timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d %H:%M:%S"),
            "REASON_FOR_CHANGE_IN_VALUE": ai_reason,
            "CHANGED_BY_ROLE": random.randint(1, 10),
            "PRIMARY_RECORD": random.randint(1, 1000),
            "APPROVED_BY_DEPUTY": random.randint(1, 1000),
            "CHANGE_FROM_COMMON_TYPE": random.randint(1, 10),
            "CHANGE_FROM_PROCESS_ID": random.randint(1, 100),
            "INTEGRATION_ID": 0,
            "INTEGRATION_VERSION": 0,
            "INTEGRATION_TYPE": 0,
            "INTEGRATION_MODE": 0
        }
        history_data.append(entry)

# Determine output directory (support --dry-run like loader)
dry_run = '--dry-run' in sys.argv
if dry_run:
    output_dir = os.path.join(os.path.dirname(__file__), '../data/output/dryRun')
else:
    output_dir = os.path.join(os.path.dirname(__file__), '../data/output/latest')
os.makedirs(output_dir, exist_ok=True)
json_out = os.path.join(output_dir, 'userprofile_history_data.json')

with open(json_out, "w", encoding="utf-8") as f:
    json.dump(history_data, f, indent=2)

print(f"✅ Generated {len(history_data)} USERPROFILE_HISTORY entries with AI reasons (CORRECTED STRUCTURE) to {json_out}")
