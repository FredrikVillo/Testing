import random
import json
from faker import Faker
from datetime import datetime, timedelta
import pandas as pd
from openai import AzureOpenAI

fake = Faker()

# Load USERPROFILE_IDs and ACCESSCATALYST from CSV
user_profiles_df = pd.read_csv("userprofile_id.csv", header=None, names=["USERPROFILE_ID", "ACCESSCATALYST"])
user_profiles = user_profiles_df.to_dict(orient="records")

# Azure OpenAI setup
***REMOVED***_path = "C:/Users/FredrikVillo/repos/TestDataGeneration/***REMOVED***.txt"
with open(***REMOVED***_path, "r") as f:
    ***REMOVED*** = f.read().strip()

client = AzureOpenAI(
    ***REMOVED***=***REMOVED***,
    api_version="2025-01-01-preview",
    azure_endpoint="https://azureopenai-sin-dev.openai.azure.com"
)

# Predefined value pools by type
value_pool_by_field = {
    'Skill': ['Python', 'SQL', 'Excel', 'Java', 'Power BI'],
    'Hobby': ['Running', 'Cycling', 'Photography', 'Hiking'],
    'Certification': ['AWS Certified', 'PMP', 'Scrum Master', 'Azure Fundamentals']
}

# Function to call AI for reason
def generate_reason(old_value, new_value):
    prompt = (
        f"An employee's profile value changed from '{old_value}' to '{new_value}'. "
        "Provide a concise and realistic reason for this change in an HR system. "
        "Avoid verbose explanations and focus on the context of the change. "
        "For example, consider role transitions, skill updates, or personal interest changes."
    )
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=50,
            temperature=0.7
        )
        global total_tokens_used
        total_tokens_used += response.usage.total_tokens  # Track token usage
        return response.choices[0].message.content.strip()
    except:
        return f"Profile value updated from '{old_value}' to '{new_value}' for role alignment."

history_data = []
history_id = 1

for profile in user_profiles:
    num_entries = random.randint(1, 3)

    for _ in range(num_entries):
        # Pick a field type
        field_type = random.choice(list(value_pool_by_field.keys()))
        values = value_pool_by_field[field_type]
        old_value, new_value = random.sample(values, 2)

        # Ensure correct chronological order
        valid_from = datetime.now() - timedelta(days=random.randint(500, 1000))
        modified_date = valid_from + timedelta(days=random.randint(10, 100))
        valid_to = modified_date + timedelta(days=random.randint(30, 180))

        ai_reason = generate_reason(old_value, new_value)

        entry = {
            "USERPROFILE_HISTORY_ID": history_id,
            "USERPROFILE_ID": profile["USERPROFILE_ID"],
            "ACCESSCATALYST": profile["ACCESSCATALYST"],
            "OLD_VALUE": old_value,
            "NEW_VALUE": new_value,
            "MODIFIED": modified_date.strftime("%Y-%m-%d %H:%M:%S"),
            "REASON": ai_reason,
            "MODIFIEDBY": fake.user_name(),
            "CHANGED_BY": random.randint(1, 1000),
            "CHANGED_DATE": modified_date.strftime("%Y-%m-%d %H:%M:%S"),
            "CHANGED_BY_DEPUTY": random.randint(1, 1000),
            "CHANGE_TYPE": random.randint(1, 5),
            "VALID_FROM": valid_from.strftime("%Y-%m-%d"),
            "APPROVED_BY": random.randint(1, 1000),
            "RECORD_NUMBER": history_id,
            "VALID_TO": valid_to.strftime("%Y-%m-%d"),
            "IS_TIMELINE": random.choice([0, 1]),
            "APPROVED_ON": (modified_date + timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d %H:%M:%S"),
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
        history_id += 1

# Save to JSON
with open("json/userprofile_history_data.json", "w") as f:
    json.dump(history_data, f, indent=2)

print(f"✅ Generated {len(history_data)} USERPROFILE_HISTORY entries with AI-enhanced reasons.")
