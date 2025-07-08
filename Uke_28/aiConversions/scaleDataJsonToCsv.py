import pandas as pd
import json

# Load JSON
input_path = "json/scale_data_full.json"
output_path = "csv/scale_data.csv"

with open(input_path, "r", encoding="utf-8") as f:
    scale_data = json.load(f)

# Convert to DataFrame
scale_df = pd.DataFrame(scale_data)

# Save to CSV
scale_df.to_csv(output_path, index=False)

print(f"✅ Scale data successfully converted to CSV and saved to '{output_path}'.")
