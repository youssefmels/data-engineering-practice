import glob
import csv
import json
import os
import pandas as pd

def main():
    data_dir = os.path.join("/app/data")
    json_files = glob.glob(os.path.join(data_dir, '**', '*.json'), recursive=True)
    print(f"Found {len(json_files)} JSON files.")
    for file in json_files:
        print(f"Processing file: {file}")
        try:
            with open(file, 'r') as f:
                json1 = json.load(f)
                print(f"Loaded JSON data from {file}: {json1}")
                normalized = pd.json_normalize(json1)
                csv_file = file.replace('.json', '.csv')
                normalized.to_csv(csv_file, index=False)
                print(f"Converted {file} to {csv_file}")

        except json.JSONDecodeError as e:
            print(f"Error loading JSON from file {file}: {e}")
        except Exception as e:
            print(f"An error occurred while processing {file}: {e}")

if __name__ == "__main__":
    main()

