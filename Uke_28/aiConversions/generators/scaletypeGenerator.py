import csv
import json
import sys
import os
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass  # Ignore if not supported

# Usage: python scaletypeGenerator.py <output_dir>
def main():
    if len(sys.argv) < 2:
        print("Usage: python scaletypeGenerator.py <output_dir>")
        sys.exit(1)
    output_dir = sys.argv[1]
    os.makedirs(output_dir, exist_ok=True)
    # Oppdatert sti til csv-mappen
    csv_path = os.path.join(os.path.dirname(__file__), '../data/input/csv/scaletype_info.csv')
    json_out = os.path.join(output_dir, 'scaletype_data.json')

    scaletypes = []
    with open(csv_path, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            scaletypes.append({
                "ID": int(row[0]),
                "NAME": row[1],
                "DESCRIPTION": row[2],
                "ISACTIVE": int(row[3]),
                "GUID": row[19]
            })

    with open(json_out, 'w', encoding='utf-8') as f:
        json.dump(scaletypes, f, indent=2)
    # Replace Unicode checkmark with ASCII
    print(f"\u2705 Wrote {len(scaletypes)} SCALETYPE rows to {json_out}")

if __name__ == "__main__":
    main()
