import requests
import logging
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException

class OpenF1Hook(BaseHook):
    """
    Interact with the OpenF1 API.
    Base URL: https://api.openf1.org/v1
    """
    def __init__(self, conn_id='openf1_default'):
        super().__init__()
        self.conn_id = conn_id
        self.base_url = "https://api.openf1.org/v1"

    def get_endpoint(self, endpoint: str, params: dict = None) -> list:
        """
        Calls an OpenF1 endpoint and returns the JSON payload.
        """
        url = f"{self.base_url}{endpoint}"
        
        # 1. Add custom headers so the API doesn't think we are a spam bot
        headers = {
            "User-Agent": "F1DataProject/1.0 (Airflow Data Pipeline)",
            "Accept": "application/json"
        }
        
        # NOTE: If you decide to sponsor OpenF1 and get an account, 
        # you would pass your token here:
        # headers["Authorization"] = "Bearer YOUR_ACCESS_TOKEN"

        logging.info(f"Making request to: {url} with params: {params}")
        
        response = requests.get(url, params=params, headers=headers)
        
        # 2. Handle the Live Session 401 Lockout gracefully
        if response.status_code == 401:
            logging.warning("401 Unauthorized: The OpenF1 API is restricting free-tier access. A live F1 session is likely active.")
            raise AirflowSkipException("Skipping task: OpenF1 API is locked down (401). Try again after the live race finishes.")
            
        # Fail the task for any other severe errors (500 Server Error, 404 Not Found, etc.)
        response.raise_for_status() 
        
        data = response.json()
        logging.info(f"Successfully retrieved {len(data)} records from {endpoint}")
        return data