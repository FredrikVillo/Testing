import pandas as pd
import json

# Load JSON
with open("json/organization_data.json", "r") as f:
    org_data = json.load(f)

# Convert to DataFrame
org_df = pd.DataFrame(org_data)

# Save to CSV
org_df.to_csv("csv/organization_data.csv", index=False)

print("Organization data successfully converted to CSV.")
