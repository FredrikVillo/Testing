import pandas as pd
import json

# Load JSON
with open("json/organization_data.json", "r") as f:
    org_data = json.load(f)

# Ensure MOTHERORG is always int or None
for org in org_data:
    if org["MOTHERORG"] is not None:
        org["MOTHERORG"] = int(org["MOTHERORG"])

# Convert to DataFrame
org_df = pd.DataFrame(org_data)

# Write to CSV WITHOUT index
org_df.to_csv("csv/organization_data_clean.csv", index=False, float_format='%.0f', na_rep='')
