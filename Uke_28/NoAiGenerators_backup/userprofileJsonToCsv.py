import json
import csv

# Input and output file paths
INPUT_FILEPATH = "json/userprofile_data.json"
OUTPUT_FILEPATH = "csv/userprofile_data.csv"

# Load JSON data
with open(INPUT_FILEPATH, 'r', encoding='utf-8') as f:
    userprofile_data = json.load(f)

# Extract keys for CSV header
keys = userprofile_data[0].keys()

# Write to CSV
with open(OUTPUT_FILEPATH, 'w', encoding='utf-8', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=keys)
    writer.writeheader()
    writer.writerows(userprofile_data)

print(f"Data successfully converted to {OUTPUT_FILEPATH}")
