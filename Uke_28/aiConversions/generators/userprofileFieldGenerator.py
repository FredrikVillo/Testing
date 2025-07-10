import csv
import json
import os
import sys
import uuid
import xml.etree.ElementTree as ET
from faker import Faker

fake = Faker()

# Helper function to extract default language label from FIELD_NAME
def extract_default_language(field_name_xml):
    try:
        tree = ET.fromstring(field_name_xml)
        for child in tree:
            if 'isDefault' in child.attrib and child.attrib['isDefault'] == 'true':
                return child.text
        return tree[0].text  # Fallback if no default
    except Exception:
        return field_name_xml  # Not XML

# Generate synthetic value based on field label
def generate_value(label):
    label_lower = label.lower()
    if 'name' in label_lower:
        return fake.name()
    elif 'email' in label_lower:
        return fake.email()
    elif 'date' in label_lower:
        return fake.date_of_birth(minimum_age=18, maximum_age=65).isoformat()
    elif 'phone' in label_lower or 'mobile' in label_lower:
        return fake.phone_number()
    elif 'address' in label_lower:
        return fake.address()
    elif 'country' in label_lower:
        return fake.country()
    elif 'city' in label_lower:
        return fake.city()
    elif 'postcode' in label_lower or 'zip' in label_lower:
        return fake.postcode()
    else:
        return fake.word()

# Main function to generate enriched userprofile field data
def main():
    if len(sys.argv) < 2:
        print("Usage: python userprofileFieldGenerator.py <output_dir>")
        sys.exit(1)

    output_dir = sys.argv[1]
    os.makedirs(output_dir, exist_ok=True)

    csv_path = os.path.join(os.path.dirname(__file__), '../data/input/csv/userprofile_field_info2.csv')
    json_out = os.path.join(output_dir, 'userprofile_field_data.json')

    fields = []

    with open(csv_path, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)  # Skip header

        for idx, row in enumerate(reader, start=1):
            # Map CSV columns to SQL fields. Adjust indices if CSV changes.
            field_data = {
                "USERPROFILE_FIELD": idx,
                "FIELD_NAME": row[1],
                "SETTINGS": row[4] if len(row) > 4 and row[4] else None,
                "RESTRICTIONS": None,
                "DELETED": 0,
                "TAB": 1,
                "PURPOSE": None,
                "GUIDELINES": None,
                "IGNORE_FROM_AUDIT": 0,
                "REFERENCE_UUID": str(uuid.uuid4()),
                "STANDARD_REFERENCE_ID": None
            }
            fields.append(field_data)

    with open(json_out, 'w', encoding='utf-8') as f:
        json.dump(fields, f, indent=2, ensure_ascii=False)

    print(f"✅ Wrote {len(fields)} USERPROFILE_FIELD rows to {json_out}")

if __name__ == "__main__":
    main()
