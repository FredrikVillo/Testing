import requests
import json
from faker import Faker
import random
import pandas as pd
import datetime as dt

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

    for _, emp in employees_df.iterrows():
        emp_id  = emp["EmployeeID"]
        hire_dt = pd.to_datetime(emp["DOB"]) + pd.DateOffset(years=22)
        # Set a fixed job history span, e.g. 10 years
        total_years = 10
        start = hire_dt
        end_of_history = hire_dt + pd.DateOffset(years=total_years)
        rows_for_person = 0
        prev_title = None
        prev_dept = None
        prev_end = None
        while start < end_of_history:
            # Each job lasts 2-4 years (24-48 months)
            months = random.randint(24, 48)
            end = start + pd.DateOffset(months=months)
            if end > end_of_history:
                end = end_of_history
            if end.date() >= today:
                end = pd.NaT

            # Build a more realistic prompt
            prompt = (
                "Generate a realistic job history entry for a Norwegian employee as JSON. "
                "The job must start the day after the previous job ended, and jobs must not overlap. "
                "If this is not the first job, make the new job a plausible next step after the previous one, with increasing seniority or logical progression. "
                f"Previous job title: {prev_title if prev_title else 'None'}. "
                f"Previous department: {prev_dept if prev_dept else 'None'}. "
                f"Previous job ended: {prev_end.date() if prev_end else 'None'}. "
                f"This job must start: {start.date()}. "
                f"This job must end: {'' if pd.isna(end) else end.date()}. "
                "FTE should be 100, 80, or 60. "
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
    # Example: create a dummy DataFrame to test
    import pandas as pd
    employees_df = pd.DataFrame([
        {"EmployeeID": 1, "DOB": "1990-01-01"},
        {"EmployeeID": 2, "DOB": "1985-05-15"}
    ])
    df = make_job_history(employees_df)
    print(df)

    # Save to CSV in 'out' folder
    df.to_csv("out/job_history.csv", index=False)
    print("Saved out/job_history.csv")



