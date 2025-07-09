import sys
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass  # Ignore if not supported

import random
import csv
import json
import os
from faker import Faker

fake = Faker()

# Define USERFIELDs and their possible values
userprofile_fields = {
    1: ["Python", "SQL", "Excel", "Project Management"],  # Skill
    2: ["English", "Norwegian", "German", "Spanish"],     # Language
    3: ["Running", "Photography", "Chess", "Hiking"],     # Hobby
    4: ["PMP", "AWS Certified", "Scrum Master"]           # Certification
}

# Always resolve paths relative to this script's directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

def rel_path(*parts):
    return os.path.join(PROJECT_ROOT, *parts)

def load_valid_accesscatalyst_ids():
    accesscatalyst_path = rel_path('data', 'output', 'latest', 'accesscatalyst_data.json')
    if not os.path.exists(accesscatalyst_path):
        print(f"❌ accesscatalyst_data.json not found at {accesscatalyst_path}")
        return set()
    with open(accesscatalyst_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        return set(entry["ACCESSCATALYST"] for entry in data if "ACCESSCATALYST" in entry)

def generate_userprofiles(employees, valid_accesscatalyst_ids, max_profiles_per_employee=3):
    userprofiles = []

    for emp in employees:
        accesscatalyst_id = emp.get("ACCESSCATALYST")
        if accesscatalyst_id not in valid_accesscatalyst_ids:
            continue  # skip if not a valid FK
        num_profiles = random.randint(1, max_profiles_per_employee)
        chosen_fields = random.sample(list(userprofile_fields.keys()), num_profiles)

        for userfield in chosen_fields:
            field_value = random.choice(userprofile_fields[userfield])

            profile = {
                "USERFIELD": userfield,
                "ACCESSCATALYST": accesscatalyst_id,
                "FIELD_VALUE": field_value
            }

            userprofiles.append(profile)

    return userprofiles

def is_dry_run():
    return "--dry-run" in sys.argv

def get_output_dir():
    for arg in sys.argv[1:]:
        if not arg.startswith("-"):
            # If absolute, return as is; else, resolve relative to project root
            return arg if os.path.isabs(arg) else rel_path(arg)
    return rel_path('data', 'output', 'latest')

# Load combined EMPLOYEE + ACCESSCATALYST data from CSV
employee_data = []
employee_csv_path = rel_path('data', 'input', 'csv', 'employee_data_with_accesscatalyst.csv')
with open(employee_csv_path, mode="r", encoding="utf-8-sig") as csv_file:  # Handle BOM
    csv_reader = csv.reader(csv_file)
    for row in csv_reader:
        employee_data.append({"EMPLOYEE": int(row[0]), "ACCESSCATALYST": int(row[1])})

# Load valid ACCESSCATALYST IDs from latest output
valid_accesscatalyst_ids = load_valid_accesscatalyst_ids()
if not valid_accesscatalyst_ids:
    print("❌ No valid ACCESSCATALYST IDs found in accesscatalyst_data.json. Aborting.")
    sys.exit(1)

# Generate USERPROFILE entries
userprofile_data = generate_userprofiles(employee_data, valid_accesscatalyst_ids)

# Use faker for any AI fields if is_dry_run()
if is_dry_run():
    # Only use valid ACCESSCATALYST IDs from accesscatalyst_data.json
    userprofile_data = [{
        "USERFIELD": fake.random_int(min=1, max=4),  # always int 1-4
        "ACCESSCATALYST": random.choice(list(valid_accesscatalyst_ids)),
        "FIELD_VALUE": fake.word()
    } for _ in range(len(userprofile_data))]

output_dir = get_output_dir()
os.makedirs(output_dir, exist_ok=True)
with open(os.path.join(output_dir, "userprofile_data.json"), "w") as f:
    json.dump(userprofile_data, f, indent=2)
print(f"✅ Generated {len(userprofile_data)} USERPROFILE entries and saved to '{os.path.join(output_dir, 'userprofile_data.json')}'")
