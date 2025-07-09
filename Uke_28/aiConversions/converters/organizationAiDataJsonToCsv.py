import pandas as pd
import json

# Load JSON
input_path = "json/organization_data_with_gpt.json"
output_path = "csv/organization_data_clean.csv"

with open(input_path, "r", encoding="utf-8") as f:
    org_data = json.load(f)

# Ensure MOTHERORG is always int or None
for org in org_data:
    if org.get("MOTHERORG") is not None:
        org["MOTHERORG"] = int(org["MOTHERORG"])

# Convert to DataFrame
org_df = pd.DataFrame(org_data)

# Save to CSV WITHOUT index
org_df.to_csv(output_path, index=False, float_format='%.0f', na_rep='')

print(f"✅ Organization data successfully cleaned and converted to CSV and saved to '{output_path}'.")
