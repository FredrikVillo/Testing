import json
import uuid
from datetime import datetime, timedelta
import random

scale_entries = [
    # Titles
    {"ID": 1001, "SCALETYPE": 1, "NAME0": "Software Engineer", "NAME1": "Software Engineer", "NAME2": "Software Engineer", "DESCRIPTION0": "Technical role"},
    {"ID": 1002, "SCALETYPE": 1, "NAME0": "Project Manager", "NAME1": "Project Manager", "NAME2": "Project Manager", "DESCRIPTION0": "Manages projects"},
    {"ID": 1003, "SCALETYPE": 1, "NAME0": "HR Specialist", "NAME1": "HR Specialist", "NAME2": "HR Specialist", "DESCRIPTION0": "Human Resources"},
    {"ID": 1004, "SCALETYPE": 1, "NAME0": "Marketing Lead", "NAME1": "Marketing Lead", "NAME2": "Marketing Lead", "DESCRIPTION0": "Marketing role"},
    {"ID": 1005, "SCALETYPE": 1, "NAME0": "Data Analyst", "NAME1": "Data Analyst", "NAME2": "Data Analyst", "DESCRIPTION0": "Analytics role"},

    # Genders
    {"ID": 2001, "SCALETYPE": 2, "NAME0": "Male", "NAME1": "Male", "NAME2": "Male", "DESCRIPTION0": "Gender: Male"},
    {"ID": 2002, "SCALETYPE": 2, "NAME0": "Female", "NAME1": "Female", "NAME2": "Female", "DESCRIPTION0": "Gender: Female"}
]

for idx, entry in enumerate(scale_entries):
    entry["SORTORDER"] = (idx + 1) * 10
    entry["DELETED"] = 0
    days_ago_created = random.randint(200, 1000)
    entry["CREATED"] = (datetime.now() - timedelta(days=days_ago_created)).strftime("%Y-%m-%d %H:%M:%S")
    entry["MODIFIED"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry["GUID"] = str(uuid.uuid4())

with open("scale_data_full.json", "w") as f:
    json.dump(scale_entries, f, indent=2)
