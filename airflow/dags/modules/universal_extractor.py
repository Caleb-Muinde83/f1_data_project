import json
import logging
from hooks.openf1_hook import OpenF1Hook

def extract_and_save_endpoint(endpoint: str, api_params: dict = None, **kwargs):
    """
    Universally pulls data from any OpenF1 endpoint and saves it to a local JSON file.
    """
    hook = OpenF1Hook()
    
    # Safely assign default parameters if none are provided
    if not api_params:
        api_params = {'year': 2023}
        
    logging.info(f"Extracting data from /{endpoint} with params {api_params}...")
    records = hook.get_endpoint(f'/{endpoint}', params=api_params)
    
    # Save the data dynamically
    output_path = f"/opt/airflow/logs/{endpoint}_extracted.json"
    
    with open(output_path, "w") as f:
        json.dump(records, f, indent=4)
        
    logging.info(f"Success! Saved {len(records)} records to {output_path}")
    return output_path