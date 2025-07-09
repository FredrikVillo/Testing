import json
import uuid
from datetime import datetime, timedelta
import random
from openai import AzureOpenAI
import sys
from faker import Faker
import os

# Azure OpenAI client setup
***REMOVED***_path = "C:/Users/FredrikVillo/repos/TestDataGeneration/***REMOVED***.txt"
with open(***REMOVED***_path, "r") as f:
    ***REMOVED*** = f.read().strip()

client = AzureOpenAI(
    ***REMOVED***=***REMOVED***,
    api_version="2025-01-01-preview",
    azure_endpoint="https://azureopenai-sin-dev.openai.azure.com"
)

fake = Faker()

# Function to call GPT to generate diverse job titles
def generate_titles(n=10):
    if is_dry_run():
        return [fake.job() for _ in range(n)]
    
    prompt = (
        f"Generate {n} unique, realistic job titles for a modern company. List them separated by commas without numbering or explanations."
    )
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500,
        temperature=0.7
    )
    titles = [title.strip() for title in response.choices[0].message.content.split(',') if title.strip()]
    return titles[:n]

def is_dry_run():
    return "--dry-run" in sys.argv

def get_output_dir():
    for arg in sys.argv[1:]:
        if not arg.startswith("-"):
            return arg
    return "."

# Generate title entries using GPT
title_names = generate_titles(5)

# Manually defined additional categories
position_levels = ["Junior", "Mid-level", "Senior", "Lead"]
employee_types = ["Permanent", "Contractor", "Intern"]
marital_statuses = ["Single", "Married"]
nationalities = ["Norwegian", "Swedish", "Danish"]
countries = ["Norway", "Sweden", "Denmark"]

scale_entries = []

# Titles (SCALETYPE 1)
for idx, title in enumerate(title_names, start=1):
    entry = {
        "ID": 1000 + idx,
        "SCALETYPE": 1,
        "NAME0": title,
        "NAME1": title,
        "NAME2": title,
        "DESCRIPTION0": f"Job Title: {title}",
        "DESCRIPTION1": None,
        "DESCRIPTION2": None,
        "SORTORDER": idx * 10,
        "DELETED": 0,
        "CREATED": (datetime.now() - timedelta(days=random.randint(200, 1000))).strftime("%Y-%m-%d %H:%M:%S"),
        "MODIFIED": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "GUID": str(uuid.uuid4())
    }
    scale_entries.append(entry)

# Gender (SCALETYPE 2)
gender_entries = ["Male", "Female"]
for idx, gender in enumerate(gender_entries, start=1):
    scale_entries.append({
        "ID": 2000 + idx,
        "SCALETYPE": 2,
        "NAME0": gender,
        "NAME1": gender,
        "NAME2": gender,
        "DESCRIPTION0": f"Gender: {gender}",
        "DESCRIPTION1": None,
        "DESCRIPTION2": None,
        "SORTORDER": (len(scale_entries) + 1) * 10,
        "DELETED": 0,
        "CREATED": (datetime.now() - timedelta(days=random.randint(200, 1000))).strftime("%Y-%m-%d %H:%M:%S"),
        "MODIFIED": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "GUID": str(uuid.uuid4())
    })

# Position Level (SCALETYPE 3)
for idx, level in enumerate(position_levels, start=1):
    scale_entries.append({
        "ID": 3000 + idx,
        "SCALETYPE": 3,
        "NAME0": level,
        "NAME1": None,
        "NAME2": None,
        "DESCRIPTION0": f"{level} level position",
        "DESCRIPTION1": None,
        "DESCRIPTION2": None,
        "SORTORDER": (len(scale_entries) + 1) * 10,
        "DELETED": 0,
        "CREATED": (datetime.now() - timedelta(days=random.randint(200, 1000))).strftime("%Y-%m-%d %H:%M:%S"),
        "MODIFIED": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "GUID": str(uuid.uuid4())
    })

# Employee Type (SCALETYPE 4)
for idx, emp_type in enumerate(employee_types, start=1):
    scale_entries.append({
        "ID": 4000 + idx,
        "SCALETYPE": 4,
        "NAME0": emp_type,
        "NAME1": None,
        "NAME2": None,
        "DESCRIPTION0": f"{emp_type} employee",
        "DESCRIPTION1": None,
        "DESCRIPTION2": None,
        "SORTORDER": (len(scale_entries) + 1) * 10,
        "DELETED": 0,
        "CREATED": (datetime.now() - timedelta(days=random.randint(200, 1000))).strftime("%Y-%m-%d %H:%M:%S"),
        "MODIFIED": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "GUID": str(uuid.uuid4())
    })

# Marital Status (SCALETYPE 5)
for idx, status in enumerate(marital_statuses, start=1):
    scale_entries.append({
        "ID": 5000 + idx,
        "SCALETYPE": 5,
        "NAME0": status,
        "NAME1": None,
        "NAME2": None,
        "DESCRIPTION0": status,
        "DESCRIPTION1": None,
        "DESCRIPTION2": None,
        "SORTORDER": (len(scale_entries) + 1) * 10,
        "DELETED": 0,
        "CREATED": (datetime.now() - timedelta(days=random.randint(200, 1000))).strftime("%Y-%m-%d %H:%M:%S"),
        "MODIFIED": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "GUID": str(uuid.uuid4())
    })

# Nationality (SCALETYPE 6)
for idx, nation in enumerate(nationalities, start=1):
    scale_entries.append({
        "ID": 6000 + idx,
        "SCALETYPE": 6,
        "NAME0": nation,
        "NAME1": None,
        "NAME2": None,
        "DESCRIPTION0": nation,
        "DESCRIPTION1": None,
        "DESCRIPTION2": None,
        "SORTORDER": (len(scale_entries) + 1) * 10,
        "DELETED": 0,
        "CREATED": (datetime.now() - timedelta(days=random.randint(200, 1000))).strftime("%Y-%m-%d %H:%M:%S"),
        "MODIFIED": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "GUID": str(uuid.uuid4())
    })

# Country (SCALETYPE 7)
for idx, country in enumerate(countries, start=1):
    scale_entries.append({
        "ID": 7000 + idx,
        "SCALETYPE": 7,
        "NAME0": country,
        "NAME1": None,
        "NAME2": None,
        "DESCRIPTION0": country,
        "DESCRIPTION1": None,
        "DESCRIPTION2": None,
        "SORTORDER": (len(scale_entries) + 1) * 10,
        "DELETED": 0,
        "CREATED": (datetime.now() - timedelta(days=random.randint(200, 1000))).strftime("%Y-%m-%d %H:%M:%S"),
        "MODIFIED": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "GUID": str(uuid.uuid4())
    })

# Save to JSON
output_dir = get_output_dir()
os.makedirs(output_dir, exist_ok=True)
with open(os.path.join(output_dir, "scale_data_full.json"), "w") as f:
    json.dump(scale_entries, f, indent=2)

print(f"✅ Generated {len(scale_entries)} SCALE entries and saved to '{os.path.join(output_dir, 'scale_data_full.json')}'")
