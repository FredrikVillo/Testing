import random
import json
from faker import Faker
from datetime import datetime, timedelta
import pandas as pd

fake = Faker()

# Load USERPROFILE_IDs and ACCESSCATALYST from CSV
user_profiles_df = pd.read_csv("csv/userprofile_id.csv", header=None, names=["USERPROFILE_ID", "ACCESSCATALYST"])
user_profiles = user_profiles_df.to_dict(orient="records")

# Predefined value pools and reasons
value_pool = ['Python', 'SQL', 'Excel', 'Running', 'Cycling', 'Blue', 'Green', 'AWS Certified', 'Scrum Master']
reasons = [
    "User updated field",
    "Admin correction",
    "Automated system update",
    "Compliance requirement",
    "User profile refresh"
]

history_data = []
history_id = 1

for profile in user_profiles:
    num_entries = random.randint(1, 3)

    for _ in range(num_entries):
        old_value, new_value = random.sample(value_pool, 2)
        modified_date = datetime.now() - timedelta(days=random.randint(0, 1000))

        entry = {
            "USERPROFILE_HISTORY_ID": history_id,
            "USERPROFILE_ID": profile["USERPROFILE_ID"],
            "ACCESSCATALYST": profile["ACCESSCATALYST"],
            "OLD_VALUE": old_value,
            "NEW_VALUE": new_value,
            "MODIFIED": modified_date.strftime("%Y-%m-%d %H:%M:%S"),
            "REASON": random.choice(reasons),
            "MODIFIEDBY": fake.user_name(),
            "CHANGED_BY": random.randint(1, 1000),
            "CHANGED_DATE": modified_date.strftime("%Y-%m-%d %H:%M:%S"),
            "CHANGED_BY_DEPUTY": random.randint(1, 1000),
            "CHANGE_TYPE": random.randint(1, 5),
            "VALID_FROM": (modified_date - timedelta(days=random.randint(10, 100))).strftime("%Y-%m-%d"),
            "APPROVED_BY": random.randint(1, 1000),
            "RECORD_NUMBER": history_id,
            "VALID_TO": (modified_date + timedelta(days=random.randint(10, 100))).strftime("%Y-%m-%d"),
            "IS_TIMELINE": random.choice([0, 1]),
            "APPROVED_ON": (modified_date + timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d %H:%M:%S"),
            "REASON_FOR_CHANGE_IN_VALUE": random.choice(reasons),
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

print(f"Generated {len(history_data)} USERPROFILE_HISTORY entries.")
