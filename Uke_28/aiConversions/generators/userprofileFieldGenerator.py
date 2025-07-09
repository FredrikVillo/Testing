import csv
import json
import sys
import os

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass  # Ignore if not supported

# Usage: python userprofileFieldGenerator.py <output_dir>
def main():
    if len(sys.argv) < 2:
        print("Usage: python userprofileFieldGenerator.py <output_dir>")
        sys.exit(1)
    output_dir = sys.argv[1]
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(os.path.dirname(__file__), '../data/input/csv/userprofile_field_info.csv')
    json_out = os.path.join(output_dir, 'userprofile_field_data.json')

    fields = []
    with open(csv_path, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            fields.append({
                "FIELDNAME": row[1],
                "FIELDTYPE": row[2],
                "DESCRIPTION": row[3],
                "ISACTIVE": int(row[4])
            })

    with open(json_out, 'w', encoding='utf-8') as f:
        json.dump(fields, f, indent=2)
    print(f"✅ Wrote {len(fields)} USERPROFILE_FIELD rows to {json_out}")

if __name__ == "__main__":
    main()
