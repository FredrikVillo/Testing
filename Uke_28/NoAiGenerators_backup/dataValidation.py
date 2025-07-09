import json
from datetime import datetime

# Load data
with open("json/organization_data.json", "r") as f:
    organizations = json.load(f)

with open("json/employee_data.json", "r") as f:
    employees = json.load(f)

# Build a set of valid ORGANIZATION IDs
organization_ids = {org["ORGANIZATION"] for org in organizations}

# Prepare for validation
seen_employee_numbers = set()
seen_emails = set()

errors = []

for emp in employees:
    emp_id = emp["EMPLOYEE"]
    org_id = emp["ORGANIZATION"]
    empno = emp["EMPLOYEENO"]
    email = emp["MAIL"]
    birthdate = datetime.strptime(emp["BIRTHDATE"], "%Y-%m-%d").date()
    employment_date = datetime.strptime(emp["EMPLOYMENTDATE"], "%Y-%m-%d").date()

    # 1. Foreign key check
    if org_id not in organization_ids:
        errors.append(f"Employee {emp_id} has invalid ORGANIZATION ID: {org_id}")

    # 2. Date logic check
    if birthdate > employment_date:
        errors.append(f"Employee {emp_id} has BIRTHDATE after EMPLOYMENTDATE.")

    # 3. Employee number uniqueness
    if empno in seen_employee_numbers:
        errors.append(f"Duplicate EMPLOYEENO found: {empno}")
    else:
        seen_employee_numbers.add(empno)

    # 4. Email uniqueness (optional)
    if email in seen_emails:
        errors.append(f"Duplicate email found: {email}")
    else:
        seen_emails.add(email)

# Final report
if not errors:
    print("✅ All validations passed!")
else:
    print(f"❌ Found {len(errors)} validation issues:")
    for error in errors:
        print(" -", error)
