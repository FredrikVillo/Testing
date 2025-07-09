import random
import json
from faker import Faker
from datetime import datetime, timedelta
from openai import AzureOpenAI
import sys
import os

fake = Faker()

# Optional: AI client for richer fields
***REMOVED***_path = "C:/Users/FredrikVillo/repos/TestDataGeneration/***REMOVED***.txt"
with open(***REMOVED***_path, "r") as f:
    ***REMOVED*** = f.read().strip()

client = AzureOpenAI(
    ***REMOVED***=***REMOVED***,
    api_version="2025-01-01-preview",
    azure_endpoint="https://azureopenai-sin-dev.openai.azure.com"
)

# Define SCALE references (matching real SCALE IDs)
scale_title_ids = [1001, 1002, 1003, 1004, 1005]
scale_gender_ids = [2001, 2002]
scale_position_level_ids = [3001, 3002, 3003, 3004]
scale_employeetype_ids = [4001, 4002, 4003]
marital_status_ids = [5001, 5002]  # Correct SCALE IDs
nationality_ids = [6001, 6002, 6003]  # Correct SCALE IDs
country_ids = [7001, 7002, 7003]  # Correct SCALE IDs

# Function to optionally enrich fields using AI
def generate_career_topic():
    if is_dry_run():
        return fake.job()
    
    prompt = "Suggest a realistic career planning discussion topic for an employee."
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=50,
        temperature=0.7
    )
    return response.choices[0].message.content.strip()

def is_dry_run():
    return "--dry-run" in sys.argv

def get_output_dir():
    for arg in sys.argv[1:]:
        if not arg.startswith("-"):
            return arg
    return "."

# Generate employee records
def generate_employee_table(num_employees=100, organizations=None):
    employees = []
    organization_ids = [org["ORGANIZATION"] for org in organizations] if organizations else [1]

    for emp_id in range(1, num_employees + 1):
        birth_date = fake.date_of_birth(minimum_age=22, maximum_age=65)
        birth_date_str = birth_date.strftime("%Y-%m-%d")
        min_employment_date = birth_date + timedelta(days=365 * 18)
        employment_date = fake.date_between_dates(date_start=min_employment_date, date_end=datetime.today().date())
        employment_date_str = employment_date.strftime("%Y-%m-%d")

        # Ensure manager is not self
        manager_id = random.randint(1, num_employees)
        while manager_id == emp_id:
            manager_id = random.randint(1, num_employees)

        # AI-generated career topic
        career_topic = generate_career_topic()

        employee = {
            "EMPLOYEE": emp_id,
            "ORGANIZATION": random.choice(organization_ids),
            "EMPLOYEENO": f"EMP{emp_id:05d}",
            "SURNAME": fake.last_name(),
            "GIVENNAME": fake.first_name(),
            "MIDDLENAME": fake.first_name(),
            "COMMONNAME": fake.first_name(),
            "MAIL": fake.email(),
            "PRIVATEMAIL": fake.email(),
            "BIRTHDATE": birth_date_str,
            "EMPLOYMENTDATE": employment_date_str,
            "EMPLOYMENTEND": None,
            "ADDRESS1": fake.street_address(),
            "ADDRESS2": fake.secondary_address(),
            "ADDRESS3": None,
            "ADDRESS4": None,
            "ZIPCODE": fake.postcode(),
            "NICKNAME": fake.first_name(),
            "USERID": f"user{emp_id}",
            "PASSWORD": fake.password(),
            "BUILDINGNAME": fake.word(),
            "EMPLOYEETYPE": random.choice(scale_employeetype_ids),
            "PHOTOURL": fake.image_url(),
            "TITLE": random.choice(scale_title_ids),
            "PROFILE_TITLE": fake.job(),
            "MANAGER": manager_id,
            "TEMPMANAGER": None,
            "ONVACATION": random.randint(0,1),
            "LASTLOGON": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "LASTUSEDOBJECT": None,
            "LASTUSEDACTIVITY": None,
            "MODIFIED": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "MODIFIEDBY": fake.user_name(),
            "MODIFICATIONNO": random.randint(1, 10),
            "SECRETARY": None,
            "SUPERVISOR": None,
            "SECONDARYORG": None,
            "PRIMARYMANAGER": None,
            "SECONDARYMANAGER": None,
            "HRMANAGER": None,
            "DIRECTORYDN": f"CN={fake.name()},OU=People,DC=example,DC=com",
            "DIRECTORYMODIFIED": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "PHOTO_ID": None,
            "JOB_DESCRIPTION": fake.job(),
            "POSITION_LEVEL": random.choice(scale_position_level_ids),
            "GENDER": random.choice(scale_gender_ids),
            "HOMEADDRESS": fake.address(),
            "DATEOFBIRTH": birth_date_str,
            "SOCIALSECURITYNUMBER": fake.ssn(),
            "NATIONALITY": random.choice(nationality_ids),
            "COUNTRY": random.choice(country_ids),
            "MARITALSTATUS": random.choice(marital_status_ids),
            "POSITION": random.randint(1, 5),
            "MOBILITY": random.randint(0, 1),
            "FLIGHT_RISK": random.randint(0, 1),
            "CAREER_AMBITIONS": random.randint(1, 3),
            "CAREER_PLANNING_TOPICS": career_topic,
            "DEPARTMENTMANAGER": None,
            "BASIC_SALARY": str(fake.random_number(digits=5)),
            "POSITION_CODE": fake.lexify(text='POS????'),
            "MARKET_VALUE": str(fake.random_number(digits=5)),
            "HIRING_DATE": employment_date_str,
            "TERMINATION_DATE": None,
            "WORKING_HOURS": str(random.randint(20, 40)),
            "STATUS": 1,
            "TYPE_OF_PAY": 1,
            "WEEKLY_WORK_HOURS": random.randint(20, 40),
            "GUID": str(fake.uuid4())
        }

        # Use faker for fields if dry run
        if is_dry_run():
            employee["EMPLOYEENO"] = fake.lexify(text='EMP????')
            employee["SURNAME"] = fake.last_name()
            employee["GIVENNAME"] = fake.first_name()
            employee["MIDDLENAME"] = fake.first_name()
            employee["COMMONNAME"] = fake.first_name()
            employee["MAIL"] = fake.email()
            employee["PRIVATEMAIL"] = fake.email()
            employee["ADDRESS1"] = fake.street_address()
            employee["ADDRESS2"] = fake.secondary_address()
            employee["ZIPCODE"] = fake.postcode()
            employee["NICKNAME"] = fake.first_name()
            employee["USERID"] = f"user{emp_id}"
            employee["PASSWORD"] = fake.password()
            employee["BUILDINGNAME"] = fake.word()
            employee["PHOTOURL"] = fake.image_url()
            employee["PROFILE_TITLE"] = fake.job()
            employee["LASTLOGON"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            employee["MODIFIED"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            employee["MODIFIEDBY"] = fake.user_name()
            employee["DIRECTORYDN"] = f"CN={fake.name()},OU=People,DC=example,DC=com"
            employee["DIRECTORYMODIFIED"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            employee["JOB_DESCRIPTION"] = fake.job()
            employee["HOMEADDRESS"] = fake.address()
            employee["DATEOFBIRTH"] = birth_date_str
            employee["SOCIALSECURITYNUMBER"] = fake.ssn()
            employee["GUID"] = str(fake.uuid4())

        employees.append(employee)

    return employees

# Load organizations
with open("json/organization_data_with_gpt.json", "r") as f:
    organization_data = json.load(f)

# Generate and save
employee_data = generate_employee_table(num_employees=100, organizations=organization_data)

output_dir = get_output_dir()
os.makedirs(output_dir, exist_ok=True)
with open(os.path.join(output_dir, "employee_data_full.json"), "w") as f:
    json.dump(employee_data, f, indent=2)

print(f"✅ Generated {len(employee_data)} EMPLOYEE records and saved to '{os.path.join(output_dir, 'employee_data_full.json')}'")
