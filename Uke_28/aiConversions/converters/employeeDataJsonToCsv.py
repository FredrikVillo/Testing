import pandas as pd
import json

# Load JSON
input_path = "json/employee_data_full.json"
output_path = "csv/employee_data.csv"

with open(input_path, "r", encoding="utf-8") as f:
    employee_data = json.load(f)

# Convert to DataFrame
employee_df = pd.DataFrame(employee_data)

# Save to CSV
employee_df.to_csv(output_path, index=False)

print(f"✅ Employee data successfully converted to CSV and saved to '{output_path}'.")
