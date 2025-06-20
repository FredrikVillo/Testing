from faker import Faker
import random
import pandas as pd
import datetime as dt

fake = Faker("no_NO")

def make_comp_history(employees_df, seed=42):
    random.seed(seed)
    Faker.seed(seed)

    salary_min = 480_000
    salary_max = 1_000_000
    grades = ["A1", "A2", "B1", "B2", "C1", "C2", "D1", "E1"]
    currencies = ["NOK"]

    comp_rows = []
    today = dt.date.today()

    for _, row in employees_df.iterrows():
        emp_id = row["EmployeeID"]
        dob = pd.to_datetime(row["DOB"])
        hire_date = dob + pd.DateOffset(years=22)

        periods = random.randint(1, 4)
        start = hire_date

        for i in range(periods):
            effective = start.date()
            salary = random.randrange(salary_min, salary_max, 5000)
            grade = random.choice(grades)

            comp_rows.append({
                "EmployeeID": emp_id,
                "EffectiveFrom": effective.isoformat(),
                "SalaryNOK": salary,
                "Grade": grade,
                "Currency": random.choice(currencies)
            })

            # next change in 1–3 years
            start += pd.DateOffset(years=random.randint(1, 3))
            if start.date() >= today:
                break

    return pd.DataFrame(comp_rows)

if __name__ == "__main__":
    from employees_generator import make_employees

    employees_df = make_employees(5)
    comp_df = make_comp_history(employees_df)
    print(comp_df.head())

    # Optional: write to CSV to confirm output manually
    comp_df.to_csv("out/comp_history_test.csv", index=False)
    print("✅ Wrote test CSV → out/comp_history_test.csv")
