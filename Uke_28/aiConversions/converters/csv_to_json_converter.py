import csv
import json
import os

def csv_to_json(csv_file_path, json_file_path):
    """
    Converts a CSV file to a JSON file.

    Args:
        csv_file_path (str): Path to the input CSV file.
        json_file_path (str): Path to the output JSON file.
    """
    try:
        # Check if the CSV file exists
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"CSV file not found: {csv_file_path}")

        # Read the CSV file
        with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file)

            # Convert CSV rows to a list of dictionaries
            json_data = []
            for row in csv_reader:
                if len(row) == 2:  # Ensure there are exactly two columns
                    json_data.append({"Column1": row[0], "Column2": row[1]})
                else:
                    raise ValueError("CSV file must have exactly two columns.")

        # Write the JSON data to the output file
        with open(json_file_path, mode='w', encoding='utf-8') as json_file:
            json.dump(json_data, json_file, indent=4)

        print(f"Successfully converted {csv_file_path} to {json_file_path}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # Example usage
    csv_file = "csv/employee_data_with_accesscatalyst.csv"
    json_file = "json/employee_data_with_accesscatalyst.json"
    csv_to_json(csv_file, json_file)
    print(f"Converted {csv_file} to {json_file}")
