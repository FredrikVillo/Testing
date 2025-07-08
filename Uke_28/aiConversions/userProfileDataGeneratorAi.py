import random
import csv
import json
from faker import Faker

fake = Faker()

# Define USERFIELDs and their possible values
userprofile_fields = {
    1: ["Python", "SQL", "Excel", "Project Management"],  # Skill
    2: ["English", "Norwegian", "German", "Spanish"],     # Language
    3: ["Running", "Photography", "Chess", "Hiking"],     # Hobby
    4: ["PMP", "AWS Certified", "Scrum Master"]           # Certification
}

def generate_userprofiles(employees, max_profiles_per_employee=3):
    userprofiles = []

    for emp in employees:
        accesscatalyst_id = emp.get("ACCESSCATALYST")  # This must exist in ACCESSCATALYST table
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

# Load combined EMPLOYEE + ACCESSCATALYST data from CSV
employee_data = []
with open("csv/employee_data_with_accesscatalyst.csv", mode="r", encoding="utf-8-sig") as csv_file:  # Handle BOM
    csv_reader = csv.reader(csv_file)
    for row in csv_reader:
        employee_data.append({"EMPLOYEE": int(row[0]), "ACCESSCATALYST": int(row[1])})

# Generate USERPROFILE entries
userprofile_data = generate_userprofiles(employee_data)

# Save result to JSON
with open("json/userprofile_data.json", "w") as f:
    json.dump(userprofile_data, f, indent=2)
