import random
import json
import uuid
from faker import Faker
from datetime import datetime, timedelta
import sys
import os

fake = Faker()

def generate_accesscatalyst_for_employees(employees):
    access_entries = []
    access_id = 1

    for emp in employees:
        employee_id = emp["EMPLOYEE"]
        first_name = emp["GIVENNAME"].lower()
        last_name = emp["SURNAME"].lower()
        username = f"{first_name}.{last_name}@example.com"
        password = fake.password(length=12)

        # Random last logon: 70% chance
        lastlogon = (datetime.now() - timedelta(days=random.randint(1, 730))).strftime("%Y-%m-%d %H:%M:%S") if random.random() < 0.7 else None

        # Random modified date
        modified_date = datetime.now() - timedelta(days=random.randint(0, 730))

        # Random created date (before modified)
        created_date = modified_date - timedelta(days=random.randint(1, 365))

        # Random pwd change date (after created)
        pwd_change_date = created_date + timedelta(days=random.randint(1, 180))

        # Disabled account (10%)
        disabled = 1 if random.random() < 0.1 else 0

        # Random deactivated date if disabled
        deactivated_on = (modified_date + timedelta(days=random.randint(1, 60))).strftime("%Y-%m-%d %H:%M:%S") if disabled else None

        access_entry = {
            "ACCESSCATALYST": access_id,
            "POLICY": random.randint(1, 5),
            "EMPLOYEE": employee_id,
            "USERNAME": username,
            "PASSWORD": password,
            "DESCRIPTION": fake.sentence(nb_words=6),
            "FAILEDLOGON": random.randint(0, 5),
            "SESSIONID": str(uuid.uuid4())[:20],
            "ACCESSDISABLED": random.choice([0, 1]),
            "LASTLOGON": lastlogon,
            "USERPROTECT": random.choice([0, 1]),
            "LANGUAGE": random.randint(1, 10),
            "DEFAULTDETAILS": random.choice([0, 1]),
            "DN": f"CN={first_name}.{last_name},OU=Users,DC=example,DC=com",
            "ORGANIZATION": fake.company(),
            "EDITOR": random.randint(1, 1000),
            "DATEFORMAT": random.randint(1, 3),
            "PWDCHANGE": pwd_change_date.strftime("%Y-%m-%d %H:%M:%S"),
            "MODIFIED": modified_date.strftime("%Y-%m-%d %H:%M:%S"),
            "CREATED": created_date.strftime("%Y-%m-%d %H:%M:%S"),
            "ISFIRSTTIMELOGON": random.choice([0, 1]),
            "EMPLOYEE_ID": f"EMP-{employee_id}",
            "KEYBASED_SSO_KEY": uuid.uuid4().hex,
            "IMPORT_MODIFIED": fake.text(max_nb_chars=50),
            "LOCKEDTIME": (modified_date + timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d %H:%M:%S") if random.random() < 0.2 else None,
            "PRIMARY_PROFILE": random.randint(1, 100),
            "PROFILE_ID": f"PROF-{random.randint(100, 999)}",
            "SECRET_KEY": fake.lexify(text='??????????'),
            "GUID": str(uuid.uuid4()),
            "DEACTIVATED_ON": deactivated_on,
            "ACCOUNT_GUID": str(uuid.uuid4()),
            "USED_INTEGRATION_IDS": ",".join([str(uuid.uuid4()) for _ in range(random.randint(1, 3))]),
            "IS_DELETED": random.choice([0, 1]),
            "IS_ANONYMIZED": random.choice([0, 1]),
            "DISABLED": disabled
        }

        access_entries.append(access_entry)
        access_id += 1

    return access_entries

def is_dry_run():
    return "--dry-run" in sys.argv

def get_output_dir():
    for arg in sys.argv[1:]:
        if not arg.startswith("-"):
            return arg
    return "."

# Load employees
with open(os.path.join('data', 'output', 'latest', 'employee_data_full.json'), "r") as f:
    employee_data = json.load(f)

accesscatalyst_data = generate_accesscatalyst_for_employees(employee_data)

output_dir = get_output_dir()
os.makedirs(output_dir, exist_ok=True)
with open(os.path.join(output_dir, "accesscatalyst_data.json"), "w") as f:
    json.dump(accesscatalyst_data, f, indent=2)
print(f"✅ Generated {len(accesscatalyst_data)} ACCESSCATALYST entries and saved to '{os.path.join(output_dir, 'accesscatalyst_data.json')}'")
