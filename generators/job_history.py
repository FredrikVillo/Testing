import requests
import json
from faker import Faker
import random
import pandas as pd
import datetime as dt
import time

fake = Faker("no_NO")

def query_LLM(prompt):
    """Send a prompt to the Lmstudio API and return the response text. Includes debug output and fallback."""
    url = "http://localhost:1234/v1/chat/completions"  # Update if your Lmstudio runs elsewhere
    headers = {"Content-Type": "application/json"}
    data = {
        "model": "google/gemma-3-12b",  # Replace with your model name if needed
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 128,
        "temperature": 0.5
    }
    print(f"\n🔗 [DEBUG] Sending POST to {url}\nPayload: {json.dumps(data, ensure_ascii=False)}")
    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        print(f"🔄 [DEBUG] Status code: {response.status_code}")
        print(f"🔄 [DEBUG] Raw response: {response.text}")
        response.raise_for_status()
        try:
            result = response.json()
            print(f"🟢 [DEBUG] Parsed JSON: {json.dumps(result, ensure_ascii=False)}")
            content = result["choices"][0]["message"]["content"]
            print(f"🟢 [DEBUG] API content: {content}")
            # --- Fix: Remove markdown code fences if present ---
            content = content.strip()
            if content.startswith("```"):
                # Remove all code fences and 'json' markers
                content = content.replace('```json', '').replace('```', '').strip()
            return content
        except Exception as parse_err:
            print(f"❌ [DEBUG] Failed to parse JSON or extract content: {parse_err}")
            print(f"❌ [DEBUG] Full response text: {response.text}")
            return '{"PositionTitle": "API_ERROR", "Department": "API_ERROR"}'
    except Exception as e:
        print(f"❌ [DEBUG] API call failed: {e}")
        return '{"PositionTitle": "API_ERROR", "Department": "API_ERROR"}'

def make_job_history(employees_df, seed=42):
    random.seed(seed)
    Faker.seed(seed)
    rows, today = [], dt.date.today()
    manager_pool = employees_df["EmployeeID"].tolist()

    # --- New: Let the model decide the company and its business ---
    company_prompt = (
        "Invent a realistic Norwegian company. Respond with a single-line JSON object only, no markdown or explanation. "
        "Example: {\"CompanyName\":\"...\", \"Industry\":\"...\", \"Description\":\"...\"}"
    )
    try:
        company_response = query_LLM(company_prompt)
        # Only use the first line, strip whitespace
        company_response = company_response.splitlines()[0].strip()
        company_data = json.loads(company_response)
        if isinstance(company_data, list):
            company_data = company_data[0] if company_data else {}
    except Exception as e:
        print("⚠️  AI error (company):", e)
        # Fallback: use Faker and a static industry list
        industries = [
            "Technology", "Finance", "Retail", "Manufacturing", "Healthcare", "Education", "Logistics", "Hospitality", "Construction", "Energy", "Consulting", "Media", "Transportation", "Real Estate", "Public Sector"
        ]
        company_data = {
            "CompanyName": fake.company(),
            "Industry": random.choice(industries),
            "Description": fake.catch_phrase()
        }
    print(f"🏢 Company: {company_data}")

    for _, emp in employees_df.iterrows():
        emp_id  = emp["EmployeeID"]
        hire_dt = pd.to_datetime(emp["DOB"]) + pd.DateOffset(years=22)
        total_years = 10
        start = hire_dt
        end_of_history = hire_dt + pd.DateOffset(years=total_years)
        rows_for_person = 0
        prev_title = None
        prev_dept = None
        prev_end = None
        while start < end_of_history:
            months = random.randint(12, 36)  # Shorter jobs for more variety
            end = start + pd.DateOffset(months=months)
            if end > end_of_history:
                end = end_of_history
            if end.date() >= today:
                end = pd.NaT

            # --- New: Prompt for diverse roles based on company ---
            prompt = (
                f"The company is {company_data.get('CompanyName', 'a Norwegian company')} in the {company_data.get('Industry', 'industry')} sector. "
                f"It does: {company_data.get('Description', '')}. "
                "Generate a realistic job history entry for an employee as JSON. "
                "The job must start the day after the previous job ended, and jobs must not overlap. "
                "If this is not the first job, make the new job a plausible next step after the previous one, with increasing seniority or logical progression. "
                f"Previous job title: {prev_title if prev_title else 'None'}. "
                f"Previous department: {prev_dept if prev_dept else 'None'}. "
                f"Previous job ended: {prev_end.date() if prev_end else 'None'}. "
                f"This job must start: {start.date()}. "
                f"This job must end: {'' if pd.isna(end) else end.date()}. "
                "FTE should be 100, 80, or 60. "
                "The role should be chosen from a wide range of departments and job types relevant for the company, such as sales, marketing, IT, janitorial, HR, finance, logistics, customer service, production, legal, R&D, management, operations, and more. "
                "Do not generate only IT or data roles. "
                "Return only JSON like:\n"
                '{"PositionTitle":"...","Department":"...",'
                '"FTE":100,"ManagerID":"E00010",'
                '"EffectiveFrom":"YYYY-MM-DD","EffectiveTo":"YYYY-MM-DD"}'
            )

            try:
                job_data = json.loads(query_LLM(prompt))
                if isinstance(job_data, list):
                    job_data = job_data[0] if job_data else {}
            except Exception as e:
                print("⚠️  AI error:", e)
                break  # skip this employee

            # safety: never self-manager
            if str(job_data.get("ManagerID", "")) == str(emp_id):
                job_data["ManagerID"] = random.choice([m for m in manager_pool if m != emp_id])

            # Enforce correct EffectiveFrom/EffectiveTo
            job_data["EffectiveFrom"] = start.date().isoformat()
            job_data["EffectiveTo"] = ("" if pd.isna(end) else end.date().isoformat())

            rows.append({
                "EmployeeID":   emp_id,
                "CompanyName":  company_data.get("CompanyName", "API_ERROR"),
                "Industry":     company_data.get("Industry", "API_ERROR"),
                "EffectiveFrom": job_data.get("EffectiveFrom", start.date().isoformat()),
                "EffectiveTo":   job_data.get("EffectiveTo", ("" if pd.isna(end) else end.date().isoformat())),
                "PositionTitle": job_data.get("PositionTitle", "API_ERROR"),
                "Department":    job_data.get("Department", "API_ERROR"),
                "FTE":           job_data.get("FTE", random.choice([100, 80, 60])),
                "ManagerID":     job_data.get("ManagerID", random.choice([m for m in manager_pool if m != emp_id]))
            })

            # Update previous job info for next iteration
            prev_title = job_data.get("PositionTitle", None)
            prev_dept = job_data.get("Department", None)
            prev_end = end

            rows_for_person += 1
            if pd.isna(end) or end >= end_of_history:
                break
            start = end + pd.DateOffset(days=1)
    return pd.DataFrame(rows)


if __name__ == "__main__":
    import pandas as pd
    import time
    start_time = time.time()
    # List of employees as dicts (only EmployeeID and DOB needed)
    employees = [
        {"EmployeeID": f"E{i+1:05d}", "DOB": dob}
        for i, dob in enumerate([
            "1961-03-02",
            "1966-06-10",
            "1965-11-03",
            "1972-12-30",
            "1959-11-28",
            "1970-01-17",
            "1983-11-23",
            "1985-03-23",
            "1972-07-20",
            "1964-11-23",
            "1984-04-16",
            "2007-03-23",
            "1985-04-15",
            "1963-04-10"
        ])
    ]
    random.shuffle(employees)
    employees_df = pd.DataFrame(employees)
    df = make_job_history(employees_df)
    print(df)
    # Save to CSV in 'out' folder
    df.to_csv("out/job_history.csv", index=False)
    print("Saved out/job_history.csv")
    elapsed = time.time() - start_time
    print(f"⏱️ Time used: {elapsed:.2f} seconds")



