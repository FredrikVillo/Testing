import pandas as pd
import json

# Load JSON
with open("employee_data_full.json", "r") as f:
    emp_data = json.load(f)

# Convert to DataFrame
emp_df = pd.DataFrame(emp_data)

# Save to CSV
emp_df.to_csv("employee_data.csv", index=False)

print("Employee data successfully converted to CSV.")
