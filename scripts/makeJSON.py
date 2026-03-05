import csv
import json
from pathlib import Path

def convert_csv_to_json(input_csv, output_json):
    location_mapping = {}

    with open(input_csv, mode='r', encoding='utf-8') as f:
        # Using reader to handle quotes and commas automatically
        reader = csv.reader(f)
        
        for row in reader:
            if not row:
                continue
            
            # Mapping based on the image provided: 
            # Column 0: ID, Column 1: Borough, Column 2: Zone
            loc_id = row[0]
            location_mapping[loc_id] = {
                "borough": row[1],
                "zone": row[2]
            }

    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(location_mapping, f, indent=4)
    
    print(f"Successfully converted {input_csv} to {output_json}")

if __name__ == "__main__":
    script_dir = Path(__file__).resolve().parent
    input_csv = script_dir / "taxi_zone_lookup.csv"
    output_json = script_dir / "taxi_zones.json"
    convert_csv_to_json(input_csv, output_json)