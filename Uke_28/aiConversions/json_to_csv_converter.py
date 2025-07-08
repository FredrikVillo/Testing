import json
import csv
import os

def json_to_csv(json_file_path, csv_file_path):
    """
    Converts a JSON file to a CSV file.

    Args:
        json_file_path (str): Path to the input JSON file.
        csv_file_path (str): Path to the output CSV file.
    """
    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"JSON file not found: {json_file_path}")

    with open(json_file_path, mode='r', encoding='utf-8') as json_file:
        data = json.load(json_file)

    if not isinstance(data, list):
        raise ValueError("JSON file must contain a list of objects.")

    with open(csv_file_path, mode='w', encoding='utf-8', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        # Write header
        header = data[0].keys()
        csv_writer.writerow(header)

        # Write rows
        for entry in data:
            csv_writer.writerow(entry.values())

if __name__ == "__main__":
    # Example usage
    json_file = "json/userprofile_data.json"
    csv_file = "csv/userprofile_data.csv"
    json_to_csv(json_file, csv_file)
    print(f"Converted {json_file} to {csv_file}")
