import random
import json
import uuid
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

def generate_organization_table(num_orgs=10):
    organizations = []
    org_ids = list(range(1, num_orgs + 1))
    
    dept_names = set()
    while len(dept_names) < num_orgs:
        dept_names.add(fake.bs().title())

    dept_names = list(dept_names)

    for i in range(num_orgs):
        org_id = org_ids[i]
        name = dept_names[i]
        
        motherorg = random.choice(org_ids[:i]) if i > 0 and random.random() < 0.3 else None
        motherorg = int(motherorg) if motherorg is not None else None

        created_date = fake.date_time_between(start_date='-5y', end_date='-1y')
        modified_date = fake.date_time_between(start_date=created_date, end_date='now')
        directory_modified = fake.date_time_between(start_date=modified_date - timedelta(days=90), end_date=modified_date)

        organization = {
            "ORGANIZATION": org_id,
            "NAME": name,
            "MOTHERORG": motherorg,
            "DISABLED": 1 if random.random() < 0.1 else 0,
            "CREATED": created_date.strftime("%Y-%m-%d %H:%M:%S"),
            "MODIFIED": modified_date.strftime("%Y-%m-%d %H:%M:%S"),
            "DIRECTORYDN": f"CN={name},OU=Departments,DC=example,DC=com",
            "DIRECTORYMODIFIED": directory_modified.strftime("%Y-%m-%d %H:%M:%S"),
            "SORTORDER": (i + 1) * 10,
            "UNIQUE_IMPORT_ID": uuid.uuid4().hex[:8],
            "IMPORT_MODIFIED": fake.word(),
            "ORGANIZATION_LEVEL": random.randint(1, 5),
            "GUID": str(uuid.uuid4())
        }
        
        organizations.append(organization)
    
    return organizations

# Generate and save
org_data = generate_organization_table(10)

with open("json/organization_data.json", "w") as f:
    json.dump(org_data, f, indent=2)
