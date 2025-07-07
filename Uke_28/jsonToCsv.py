import pandas as pd
import json

# Load JSON
with open("organization_data.json", "r") as f:
    org_data = json.load(f)

with open("employee_data.json", "r") as f:
    emp_data = json.load(f)

# Convert to DataFrames
org_df = pd.DataFrame(org_data)
emp_df = pd.DataFrame(emp_data)

# Save to CSV
org_df.to_csv("organization_data.csv", index=False)
emp_df.to_csv("employee_data.csv", index=False)
