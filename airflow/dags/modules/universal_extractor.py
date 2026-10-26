import os
import time
import logging
import json
import pyarrow as pa
import pyarrow.parquet as pq
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from hooks.openf1_hook import OpenF1Hook

def sanitize_mixed_and_nested_columns(records):
    """
    Dynamically detects mixed-type columns and nested arrays/dicts.
    Casts them to strings/JSON to prevent PyArrow schema crashes.
    """
    col_types = defaultdict(set)
    for row in records:
        for k, v in row.items():
            if v is not None:
                col_types[k].add(type(v).__name__)
    
    # Identify mixed columns OR columns containing lists/dicts (e.g., duration arrays in Qualifying)
    problematic_cols = {
        col for col, types in col_types.items() 
        if len(types) > 1 or 'list' in types or 'dict' in types
    }
    
    for row in records:
        for col in problematic_cols:
            if col in row and row[col] is not None:
                if isinstance(row[col], (list, dict)):
                    row[col] = json.dumps(row[col]) # Safe JSON stringification for nested F1 data
                else:
                    row[col] = str(row[col])
    return records

def build_output_path(endpoint: str, session_val) -> str:
    """Deterministic file name: re-running the same endpoint+session overwrites instead of duplicating (idempotent)."""
    base_path = os.getenv("OPENF1_OUTPUT_PATH", "/opt/airflow/data/raw")
    return f"{base_path}/{endpoint}_{session_val}_extracted.parquet"


def extract_and_save_endpoint(endpoint: str, target_session="latest", api_params: dict = None, is_global: bool = False,
                              skip_existing: bool = False, run_id: str = None):
    """
    Universally pulls data from any OpenF1 endpoint. 
    skip_existing: return the existing file untouched when it is already on disk (cheap incremental backfills).
    run_id: optional orchestrator run id, stamped into every row as _airflow_run_id for lineage/audit.
    Handles dynamic routing, 422/429 protection, strict-typing sanitization, Parquet conversion,
    and fallback Time-Window chunking for ultra-dense data.
    """
    hook = OpenF1Hook()
    params = api_params.copy() if api_params else {}
    ultra_heavy_endpoints = ["car_data", "location"]

    if not is_global:
        params["session_key"] = target_session

    session_val = params.get("session_key", "global")
    records = []

    # ==========================================
    # RESOLVE "LATEST"
    # ==========================================
    if str(session_val).lower() == "latest" and endpoint not in ["sessions", "meetings"]:
        logging.info("Resolving 'latest' to an actual session ID...")
        try:
            latest_session_data = hook.get_endpoint('/sessions', params={'session_key': 'latest'})
            if latest_session_data and isinstance(latest_session_data, list):
                session_val = latest_session_data[-1]['session_key']
                params['session_key'] = session_val
        except Exception as e:
            logging.warning(f"Could not resolve 'latest' session: {e}")

    # ==========================================
    # SKIP FILES THAT ALREADY EXIST (opt-in)
    # ==========================================
    if skip_existing and str(session_val).isdigit():
        existing = build_output_path(endpoint, session_val)
        if os.path.exists(existing):
            logging.info(f"skip_existing=True and {existing} already exists. Skipping /{endpoint} for session {session_val}.")
            return existing

    # ==========================================
    # ULTRA-HEAVY ENDPOINT PROTECTION & RETRIES
    # ==========================================
    if endpoint in ultra_heavy_endpoints and str(session_val).isdigit():
        logging.info(f"/{endpoint} is ultra-heavy. Sub-chunking session {session_val} by driver...")
        
        # 1. Fetch Drivers (Protected)
        drivers = None
        for attempt in range(3):
            try:
                drivers = hook.get_endpoint('/drivers', params={'session_key': session_val})
                break
            except Exception as e:
                error_msg = str(e)
                if "404" in error_msg:
                    logging.warning(f"No driver data for session {session_val}. Skipping /{endpoint}.")
                    return None
                elif "429" in error_msg and attempt < 2:
                    wait = 30 * (attempt + 1)
                    logging.warning(f"429 on /drivers for session {session_val}. Sleeping {wait}s (attempt {attempt+1}/3)...")
                    time.sleep(wait)
                else:
                    if attempt == 2 or "429" not in error_msg:
                        raise e
        
        if not drivers:
            logging.warning(f"Failed to retrieve drivers for session {session_val}. Skipping /{endpoint}.")
            return None
            
        # 2. Fetch Session Timestamps (Used if Time-Window Chunking is triggered)
        session_start, session_end = None, None
        try:
            session_details = hook.get_endpoint('/sessions', params={'session_key': session_val})
            if session_details and isinstance(session_details, list):
                session_start = session_details[0].get('date_start')
                session_end = session_details[0].get('date_end')
        except Exception as e:
            logging.warning(f"Could not retrieve session timestamps for time-window fallback: {e}")

        driver_numbers = list(set([d['driver_number'] for d in drivers if 'driver_number' in d]))
        
        # 3. Main Extraction Loop
        for driver in driver_numbers:
            chunk_params = params.copy()
            chunk_params["driver_number"] = driver
            success = False
            
            for attempt in range(3):
                try:
                    time.sleep(2) # Base pacing
                    driver_data = hook.get_endpoint(f'/{endpoint}', params=chunk_params)
                    if driver_data:
                        records.extend(driver_data)
                    success = True
                    break # Success, break out of retry loop
                except Exception as e:
                    error_msg = str(e)
                    if "404" in error_msg:
                        logging.warning(f"🚨 404 ERROR: Driver {driver} missing. Skipping.")
                        success = True # Acknowledge handled so fallback doesn't trigger
                        break 
                    elif "422" in error_msg:
                        logging.warning(f"🚨 422 ERROR: Driver {driver} payload too large. Triggering Time-Window Chunking...")
                        break # Break loop early to trigger the fallback below
                    elif "429" in error_msg and attempt < 2:
                        sleep_duration = 30 * (attempt + 1)
                        logging.warning(f"🚦 429 RATE LIMIT: Driver {driver}. Retrying in {sleep_duration}s...")
                        time.sleep(sleep_duration)
                    else:
                        if attempt == 2:
                            raise e

            # ==========================================
            # 4. TIME-WINDOW CHUNKING FALLBACK
            # ==========================================
            if not success and session_start and session_end:
                logging.info(f"Initiating 15-minute time chunks for Driver {driver} in session {session_val}...")
                
                try:
                    # OpenF1 returns standard ISO 8601 with trailing Z or timezone offsets
                    start_dt = datetime.fromisoformat(session_start.replace('Z', '+00:00'))
                    end_dt = datetime.fromisoformat(session_end.replace('Z', '+00:00'))
                    current_dt = start_dt
                    
                    while current_dt < end_dt:
                        next_dt = current_dt + timedelta(minutes=15)
                        
                        time_params = chunk_params.copy()
                        # F1 API range filters: date>= and date<
                        time_params['date>='] = current_dt.isoformat()
                        time_params['date<'] = next_dt.isoformat()
                        
                        for sub_attempt in range(3):
                            try:
                                time.sleep(1) # Pacing between time chunks
                                time_data = hook.get_endpoint(f'/{endpoint}', params=time_params)
                                if time_data:
                                    records.extend(time_data)
                                break
                            except Exception as e:
                                sub_error = str(e)
                                if "404" in sub_error:
                                    break # Chunk is just empty, move to next
                                elif "422" in sub_error:
                                    logging.error(f"FATAL 422: 15-min chunk STILL too large for Driver {driver} at {current_dt}. Skipping chunk.")
                                    break # Skip this specific 15 min chunk to avoid looping endlessly
                                elif "429" in sub_error and sub_attempt < 2:
                                    time.sleep(30 * (sub_attempt + 1))
                                else:
                                    if sub_attempt == 2:
                                        raise e
                                        
                        current_dt = next_dt
                except Exception as ex:
                    logging.error(f"Time-window fallback failed for Driver {driver}: {ex}")
            
            # POLISH ADDED: Explicit warning if fallback couldn't execute due to missing timestamps
            elif not success:
                logging.warning(f"⚠️ Driver {driver} on /{endpoint} had a 422 payload issue, but session timestamps were unavailable for time-window fallback. Skipping driver data.")

    # ==========================================
    # STANDARD EXTRACTION EXECUTION
    # ==========================================
    else:
        for attempt in range(3):
            try:
                logging.info(f"Extracting data from /{endpoint} with params {params} (Attempt {attempt+1}/3)...")
                records = hook.get_endpoint(f'/{endpoint}', params=params)
                break
            except Exception as e:
                error_msg = str(e)
                if "404" in error_msg or "422" in error_msg:
                    logging.warning(f"Endpoint /{endpoint} unavailable for session {session_val}. Skipping gracefully.")
                    return None
                elif "429" in error_msg and attempt < 2:
                    wait = 30 * (attempt + 1)  # 30s, 60s
                    logging.warning(f"429 on /{endpoint} session {session_val}. Sleeping {wait}s (attempt {attempt+1}/3)...")
                    time.sleep(wait)
                else:
                    if attempt == 2 or "429" not in error_msg:
                        raise e

    if not records:
        logging.info(f"No records found for /{endpoint} session {session_val}. Skipping save.")
        return None

    # ==========================================
    # SANITIZE & SAVE TO DISK AS PARQUET
    # ==========================================
    records = sanitize_mixed_and_nested_columns(records)

    # Extraction metadata: lets you audit when/why a file was produced and reconcile row counts later
    extracted_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for row in records:
        row["_extracted_at_utc"] = extracted_at
        row["_source_endpoint"] = endpoint
        row["_airflow_run_id"] = run_id

    output_path = build_output_path(endpoint, session_val)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Write to a temp file then rename, so a crash mid-write can never leave a truncated Parquet file
    # that dbt would later try to read.
    tmp_path = output_path + ".tmp"
    try:
        table = pa.Table.from_pylist(records)
        pq.write_table(table, tmp_path, compression='snappy')
        os.replace(tmp_path, output_path)
        logging.info(f"Success! Saved {len(records)} records to {output_path}")
        return output_path
    except Exception as e:
        logging.error(f"Failed to save Parquet file for {endpoint}: {str(e)}")
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise e
