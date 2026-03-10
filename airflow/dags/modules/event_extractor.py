import json
import logging
from hooks.openf1_hook import OpenF1Hook

def extract_and_save_meetings(**kwargs):
    """
    Pulls meeting (race weekend) data from OpenF1 and saves it to a local JSON file.
    """
    # 1. Initialize our custom hook
    hook = OpenF1Hook()
    
    # 2. Call the API (let's grab all 2023 meetings as a test)
    # Using the exact filtering logic from the OpenF1 docs
    records = hook.get_endpoint('/meetings', params={'year': 2023})
    
    # 3. Save the data to the local logs folder (which is synced to your Windows F: drive)
    output_path = "/opt/airflow/logs/meetings_2023.json"
    
    with open(output_path, "w") as f:
        json.dump(records, f, indent=4)
        
    logging.info(f"Saved {len(records)} meetings to {output_path}")
    return output_path