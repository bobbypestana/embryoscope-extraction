"""
Main Embryoscope Data Extraction Script
Orchestrates the complete data extraction process for all embryoscope locations.
Each clinic saves to its own database and only queries new embryos/treatments from the API.
"""

import os
import sys
import logging
import time
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional
from tqdm import tqdm
import concurrent.futures
import pandas as pd

# Add parent directory to path for imports (utils is in parent directory)
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

from utils.config_manager import EmbryoscopeConfigManager
from utils.api_client import EmbryoscopeAPIClient
from utils.data_processor import EmbryoscopeDataProcessor
from utils.database_manager import EmbryoscopeDatabaseManager


class EmbryoscopeExtractor:
    """Main class for orchestrating embryoscope data extraction."""
    
    def __init__(self, config_path: str = ""):
        """
        Initialize the embryoscope extractor.
        
        Args:
            config_path: Path to configuration file (optional)
        """
        # Determine config path robustly
        if not config_path:
            # Always resolve relative to this script's parent directory
            script_dir = os.path.dirname(os.path.abspath(__file__))
            candidate = os.path.join(script_dir, "params.yml")
            if os.path.exists(candidate):
                resolved_config_path = candidate
            else:
                # Try one directory up (for running from test/ or other subfolders)
                parent_candidate = os.path.join(os.path.dirname(script_dir), "params.yml")
                if os.path.exists(parent_candidate):
                    resolved_config_path = parent_candidate
                else:
                    raise FileNotFoundError(f"Configuration file not found: {candidate} or {parent_candidate}")
        else:
            resolved_config_path = config_path
        self.config_manager = EmbryoscopeConfigManager(resolved_config_path)
        self.logger = self._setup_logging()
        
        # Validate configuration
        if not self.config_manager.validate_config():
            raise ValueError("Invalid configuration")
        
        self.logger.info("Embryoscope extractor initialized successfully")
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration for the entire process (root logger)."""
        # Create logs directory in parent embryoscope directory
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
        os.makedirs(log_dir, exist_ok=True)

        # Get log level from config
        log_level_str = self.config_manager.config['extraction'].get('log_level', 'INFO')
        log_level = getattr(logging, log_level_str.upper(), logging.INFO)

        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        root_logger.propagate = False
        # Remove any existing handlers (avoid duplicate logs on rerun)
        if root_logger.hasHandlers():
            root_logger.handlers.clear()

        # Create handlers
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        script_name = os.path.splitext(os.path.basename(__file__))[0]
        file_handler = logging.FileHandler(os.path.join(log_dir, f'{script_name}_{timestamp}.log'))
        file_handler.setLevel(logging.DEBUG)  # Log everything to file
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)  # Only show INFO+ in terminal

        # Create formatters and add it to handlers
        log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(log_format)
        console_handler.setFormatter(log_format)

        # Add handlers to the root logger
        root_logger.addHandler(file_handler)

        # Add a filter to the console handler to only show logs from 'embryoscope_extractor'
        class ExtractorOnlyFilter(logging.Filter):
            def filter(self, record):
                return record.name == 'embryoscope_extractor'
        console_handler.addFilter(ExtractorOnlyFilter())
        root_logger.addHandler(console_handler)

        # Return a named logger for the extractor for code compatibility
        return logging.getLogger('embryoscope_extractor')
    
    def _get_db_path(self, clinic_name: str) -> str:
        """Get database path for a specific clinic."""
        safe_name = clinic_name.lower().replace(' ', '_')
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(script_dir))
        return os.path.normpath(os.path.join(project_root, "database", f"embryoscope_{safe_name}.db"))
    
    def _extract_clinic_data(self, clinic_name: str, config: Dict[str, Any], patient_ids: Optional[list] = None, db_path: Optional[str] = None, backfill: bool = False) -> bool:
        """
        Extract data for a single clinic, writing to its own DuckDB file.
        Only fetch embryo data for new patient-treatment pairs.
        Optionally restrict to a subset of patient_ids and/or use a custom db_path.
        """
        self.logger.info(f"Starting extraction for clinic: {clinic_name}")
        
        # Setup clinic-specific database
        if db_path is None:
            db_path = self._get_db_path(clinic_name)
        db_manager = EmbryoscopeDatabaseManager(db_path)
        
        # Initialize API client and data processor
        rate_limit_delay = self.config_manager.get_rate_limit_delay()
        max_workers = self.config_manager.get_max_workers()  # For clinic-level parallelization
        clinic_workers = self.config_manager.get_clinic_parallel_workers()  # For internal clinic operations
        api_client = EmbryoscopeAPIClient(clinic_name, config, rate_limit_delay)
        data_processor = EmbryoscopeDataProcessor(clinic_name)
        extraction_timestamp = datetime.now()
        run_id = str(uuid.uuid4())
        try:
            # 1. Get all patients
            self.logger.info(f"[{clinic_name}] Fetching all patients from API...")
            patients_data = api_client.get_patients()
            if patients_data is None:
                self.logger.error(f"[{clinic_name}] Failed to fetch patients data (API error/unreachable). Aborting extraction for safety.")
                return False
            # Save raw patients to bronze
            if 'Patients' in patients_data:
                db_manager.save_bronze_raw('patients', patients_data['Patients'], extraction_timestamp, run_id, clinic_name)
            patients_df = data_processor.process_patients(patients_data, extraction_timestamp, run_id)
            self.logger.info(f"[{clinic_name}] Fetched {len(patients_df)} patients from API.")
            self.logger.debug(f"[{clinic_name}] Fetched {len(patients_df)} patients from API.")
            # Restrict to provided patient_ids if given
            if patient_ids is not None:
                patients_df = patients_df[patients_df['PatientIDx'].isin(patient_ids)]
            
            # 1b. Get ongoing patients
            use_ongoing_endpoint = self.config_manager.config.get('data_extraction', {}).get('use_ongoing_endpoint', False)
            ongoing_patient_idxs = set()
            ongoing_unavailable = False

            if use_ongoing_endpoint:
                self.logger.info(f"[{clinic_name}] Fetching ongoing patients from API...")
                ongoing_patients_data = api_client.get_ongoing_patients()
                if ongoing_patients_data is None:
                    self.logger.warning(f"[{clinic_name}] Ongoing patients endpoint unavailable. Operating in safe degraded mode (new pairs marked completed, existing ongoing frozen).")
                    ongoing_unavailable = True
                elif 'Patients' in ongoing_patients_data and ongoing_patients_data['Patients']:
                    ongoing_patient_list = ongoing_patients_data['Patients']
                    for patient in ongoing_patient_list:
                        idx = patient.get('PatientIDx') or patient.get('PatientIdx') or patient.get('PatientID')
                        if idx:
                            ongoing_patient_idxs.add(str(idx))
                self.logger.info(f"[{clinic_name}] Found {len(ongoing_patient_idxs)} ongoing patients from endpoint.")
            else:
                self.logger.info(f"[{clinic_name}] Ongoing patients endpoint bypassed (using 10-day active incubation window).")

            # 2. Get treatments for patients
            # Query all patients from API so that returning patients (e.g. registered in past years)
            # and runs after execution gaps are never missed.
            is_full_backfill = backfill or os.getenv("FULL_BACKFILL", "False").lower() in ("true", "1", "yes")
            lookback_days = self.config_manager.config.get('data_extraction', {}).get('recent_treatments_lookback_days', 60)

            # Load existing treatments from DB as a safety fallback in case an individual API call fails
            _, _, db_treatments_df = db_manager.get_existing_patients_and_treatments(clinic_name, lookback_days)

            all_treatments = []
            def fetch_treatments_for_patient(patient_idx):
                treatments_data = api_client.get_treatments(patient_idx)
                if treatments_data is None:
                    time.sleep(0.5)
                    treatments_data = api_client.get_treatments(patient_idx)
                if treatments_data is None:
                    self.logger.warning(f"[{clinic_name}] Failed to get treatments for patient {patient_idx}, falling back to DB cache")
                    if not db_treatments_df.empty:
                        cached = db_treatments_df[db_treatments_df['PatientIDx'] == patient_idx]
                        if not cached.empty:
                            return cached[['PatientIDx', 'TreatmentName']]
                    return pd.DataFrame()
                # Save raw treatments to bronze
                if 'TreatmentList' in treatments_data:
                    raw_treatments = [
                        {'PatientIDx': patient_idx, 'TreatmentName': t} for t in treatments_data['TreatmentList']
                    ]
                    db_manager.save_bronze_raw('treatments', raw_treatments, extraction_timestamp, run_id, clinic_name)
                return data_processor.process_treatments(treatments_data, patient_idx, extraction_timestamp, run_id)

            patient_ids_to_query = list(patients_df['PatientIDx'])
            self.logger.info(f"[{clinic_name}] Querying treatments from API for all {len(patient_ids_to_query)} patients (ensuring returning patients & non-daily runs are captured)...")

            if patient_ids_to_query:
                with concurrent.futures.ThreadPoolExecutor(max_workers=clinic_workers) as executor:
                    futures = {executor.submit(fetch_treatments_for_patient, pid): pid for pid in patient_ids_to_query}
                    for f in tqdm(concurrent.futures.as_completed(futures), total=len(futures), 
                                desc=f"Fetching treatments for {clinic_name}", unit="patient"):
                        result = f.result()
                        if not result.empty:
                            all_treatments.append(result)

            if all_treatments:
                treatments_df = pd.concat(all_treatments, ignore_index=True).drop_duplicates(subset=['PatientIDx', 'TreatmentName'])
                total_treatments = len(treatments_df)
                self.logger.info(f"[{clinic_name}] Total treatments discovered: {total_treatments} across all patients.")
            else:
                treatments_df = pd.DataFrame(columns=pd.Index(['PatientIDx', 'TreatmentName']))
            
            # 3. Compare with local DuckDB to determine pair extraction status
            self.logger.info(f"[{clinic_name}] Comparing with local DuckDB state to determine pair extraction needs...")
            try:
                existing_pairs_status = db_manager.get_existing_pairs_with_status(clinic_name)
            except Exception as db_err:
                self.logger.warning(f"[{clinic_name}] Could not fetch existing pairs status: {db_err}")
                existing_pairs_status = {}
            
            # Filter out treatments with 'Merge' in the name (administrative entries)
            treatments_filtered = treatments_df[~treatments_df['TreatmentName'].str.contains('Merge', case=False, na=False)]
            merge_count = len(treatments_df) - len(treatments_filtered)
            if merge_count > 0:
                self.logger.info(f"[{clinic_name}] Filtered out {merge_count} 'Merge' treatments from comparison.")
            
            all_pairs = set((str(row['PatientIDx']), str(row['TreatmentName'])) for _, row in treatments_filtered.iterrows())
            
            # Categorize pairs based on ongoing state machine (10-day active clinical incubation window)
            is_full_backfill = backfill or os.getenv("FULL_BACKFILL", "False").lower() in ("true", "1", "yes")
            active_window_days = self.config_manager.config.get('data_extraction', {}).get('active_treatment_window_days', 10)
            lookback_days = self.config_manager.config.get('data_extraction', {}).get('recent_treatments_lookback_days', 60)
            
            pairs_to_fetch = {}  # pair -> target_is_ongoing
            unseen_pairs_count = 0
            currently_ongoing_count = 0
            final_pull_count = 0
            missing_embryos_count = 0
            completed_skipped_count = 0
            reopened_recent_count = 0
            recent_candidate_pairs = []
            
            today = datetime.now().date()
            current_year = str(datetime.now().year)
            current_year_short = current_year[2:]

            for pair in all_pairs:
                pid, tname = pair
                status_info = existing_pairs_status.get(pair)
                
                # Support both bool (legacy) and dict format from database_manager
                if isinstance(status_info, dict):
                    was_previously_ongoing = status_info.get('is_ongoing', False)
                    has_embryos = status_info.get('has_embryos', False)
                    latest_embryo_date = status_info.get('latest_embryo_date')
                    treatment_timestamp = status_info.get('treatment_timestamp')
                    is_in_db = True
                elif isinstance(status_info, bool):
                    was_previously_ongoing = status_info
                    has_embryos = False
                    latest_embryo_date = None
                    treatment_timestamp = None
                    is_in_db = True
                else:
                    was_previously_ongoing = False
                    has_embryos = False
                    latest_embryo_date = None
                    treatment_timestamp = None
                    is_in_db = False

                # Calculate clinical age in days from latest embryo date
                age_days = None
                if latest_embryo_date is not None:
                    try:
                        if isinstance(latest_embryo_date, str):
                            latest_dt = pd.to_datetime(latest_embryo_date).date()
                        elif hasattr(latest_embryo_date, 'date'):
                            latest_dt = latest_embryo_date.date()
                        else:
                            latest_dt = latest_embryo_date
                        age_days = (today - latest_dt).days
                    except Exception:
                        age_days = None
                
                # Determine is_currently_ongoing
                if use_ongoing_endpoint and not ongoing_unavailable and ongoing_patient_idxs:
                    # Endpoint-based mode (when enabled and available)
                    is_currently_ongoing = pid in ongoing_patient_idxs
                else:
                    # Clinical incubation window mode (age <= 10 days)
                    if age_days is not None:
                        is_currently_ongoing = (age_days <= active_window_days)
                    else:
                        is_currently_ongoing = False
                        if treatment_timestamp:
                            try:
                                t_dt = pd.to_datetime(treatment_timestamp).date()
                                if (today - t_dt).days <= active_window_days:
                                    is_currently_ongoing = True
                            except Exception:
                                pass
                        if not is_currently_ongoing and (current_year in tname or f"/{current_year_short}" in tname or f"-{current_year_short}" in tname or f" {current_year_short}" in tname):
                            is_currently_ongoing = True
                
                if is_full_backfill:
                    pairs_to_fetch[pair] = is_currently_ongoing
                elif not is_in_db:
                    # Case A: Brand new pair not yet in database
                    # Always fetch to ingest. Assume active if recent name/year
                    is_suspected_active = (current_year in tname or f"/{current_year_short}" in tname or f"-{current_year_short}" in tname or f" {current_year_short}" in tname)
                    pairs_to_fetch[pair] = is_suspected_active
                    unseen_pairs_count += 1
                    if is_suspected_active:
                        currently_ongoing_count += 1
                elif is_currently_ongoing:
                    # Case B: Active ongoing cycle in incubation window (age <= 10 days)
                    pairs_to_fetch[pair] = True
                    currently_ongoing_count += 1
                elif was_previously_ongoing:
                    # Case C: Exited active incubation window (age > 10 days). Final pull to capture terminal outcomes.
                    pairs_to_fetch[pair] = False
                    final_pull_count += 1
                elif not has_embryos:
                    # Case D: In DB but 0 embryos so far. Retry if recent (within active window).
                    t_is_recent = False
                    if current_year in tname or f"/{current_year_short}" in tname or f"-{current_year_short}" in tname or f" {current_year_short}" in tname:
                        t_is_recent = True
                    elif treatment_timestamp:
                        try:
                            t_dt = pd.to_datetime(treatment_timestamp).date()
                            if (today - t_dt).days <= active_window_days:
                                t_is_recent = True
                        except Exception:
                            pass
                    
                    if t_is_recent:
                        pairs_to_fetch[pair] = True
                        missing_embryos_count += 1
                    else:
                        completed_skipped_count += 1
                else:
                    # Case E: In DB with embryos, age > 10 days, and not ongoing.
                    # Check if treatment is within lookback_days for lightweight get_embryo_id check
                    is_recent = False
                    if age_days is not None and lookback_days and lookback_days > 0:
                        if age_days <= lookback_days:
                            is_recent = True
                    
                    if is_recent:
                        recent_candidate_pairs.append(pair)
                    else:
                        completed_skipped_count += 1

            # Check recent candidate pairs using lightweight get_embryo_id
            if recent_candidate_pairs:
                self.logger.info(f"[{clinic_name}] Checking lightweight get_embryo_id for {len(recent_candidate_pairs)} recent pairs (last {lookback_days} days)...")
                def check_recent_pair(pair_item):
                    p_id, t_name = pair_item
                    embryo_id_data = api_client.get_embryo_id(p_id, t_name)
                    if embryo_id_data and 'EmbryoIDList' in embryo_id_data and embryo_id_data['EmbryoIDList']:
                        api_ids = set(str(e) for e in embryo_id_data['EmbryoIDList'])
                        db_ids = existing_pairs_status.get(pair_item, {}).get('embryo_ids', set())
                        new_ids = api_ids - db_ids
                        if new_ids:
                            return pair_item, True, len(new_ids)
                    return pair_item, False, 0

                with concurrent.futures.ThreadPoolExecutor(max_workers=clinic_workers) as executor:
                    futures = {executor.submit(check_recent_pair, p): p for p in recent_candidate_pairs}
                    for f in tqdm(concurrent.futures.as_completed(futures), total=len(futures), 
                                desc=f"Checking recent pairs for {clinic_name}", unit="pair"):
                        p_item, has_new, new_cnt = f.result()
                        if has_new:
                            self.logger.info(f"[{clinic_name}] Detected {new_cnt} new embryo(s) for recent pair {p_item}. Re-extracting.")
                            pairs_to_fetch[p_item] = False
                            reopened_recent_count += 1
                        else:
                            completed_skipped_count += 1

            self.logger.info(f"[{clinic_name}] Pairs breakdown:")
            self.logger.info(f"  - Total pairs from API: {len(all_pairs)}")
            self.logger.info(f"  - Existing pairs in DB: {len(existing_pairs_status)}")
            self.logger.info(f"  - Unseen completed pairs to process: {unseen_pairs_count}")
            self.logger.info(f"  - Active ongoing pairs to update: {currently_ongoing_count}")
            self.logger.info(f"  - Final pull pairs (just completed): {final_pull_count}")
            self.logger.info(f"  - Missing embryo records pairs to retry: {missing_embryos_count}")
            self.logger.info(f"  - Reopened recent pairs with new embryos: {reopened_recent_count}")
            self.logger.info(f"  - Completed pairs skipped: {completed_skipped_count}")
            self.logger.info(f"  - Total pairs to extract: {len(pairs_to_fetch)}")
            
            # 4. Fetch embryo data for selected pairs
            all_embryo_data = []
            updated_treatment_statuses = {}  # pair -> actual_is_ongoing
            
            def fetch_embryo_for_pair(item):
                pair, target_is_ongoing = item
                patient_idx, treatment_name = pair
                embryo_data = api_client.get_embryo_data(patient_idx, treatment_name)
                
                if embryo_data is None:
                    # No embryo data returned (e.g. treatment with no embryos or failed embryos)
                    self.logger.debug(f"[{clinic_name}] No embryo data returned for pair ({patient_idx}, {treatment_name}). Setting is_ongoing={target_is_ongoing}.")
                    return pd.DataFrame(), pair, target_is_ongoing
                
                # Save raw embryo_data to bronze
                raw_embryos = []
                if 'EmbryoDataList' in embryo_data and embryo_data['EmbryoDataList']:
                    raw_embryos = embryo_data['EmbryoDataList']
                    for rec in raw_embryos:
                        rec['PatientIDx'] = patient_idx
                        rec['TreatmentName'] = treatment_name
                    db_manager.save_bronze_raw('embryo_data', raw_embryos, extraction_timestamp, run_id, clinic_name)
                
                processed_df = data_processor.process_embryo_data(embryo_data, patient_idx, treatment_name, extraction_timestamp, run_id)
                
                # Determine actual_is_ongoing based on returned embryo dates
                actual_is_ongoing = target_is_ongoing
                if not processed_df.empty and 'EmbryoID' in processed_df.columns:
                    import re
                    latest_embryo_dt = None
                    for eid in processed_df['EmbryoID'].dropna():
                        m = re.search(r'D(20\d{2})\.(\d{2})\.(\d{2})', str(eid))
                        if m:
                            try:
                                edt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()
                                if latest_embryo_dt is None or edt > latest_embryo_dt:
                                    latest_embryo_dt = edt
                            except Exception:
                                pass
                    if latest_embryo_dt:
                        embryo_age = (datetime.now().date() - latest_embryo_dt).days
                        actual_is_ongoing = (embryo_age <= active_window_days)
                
                return processed_df, pair, actual_is_ongoing

            pairs_to_fetch_list = list(pairs_to_fetch.items())
            if pairs_to_fetch_list:
                self.logger.info(f"[{clinic_name}] Starting embryo data extraction for {len(pairs_to_fetch_list)} pairs...")
                with concurrent.futures.ThreadPoolExecutor(max_workers=clinic_workers) as executor:
                    futures = {executor.submit(fetch_embryo_for_pair, item): item[0] for item in pairs_to_fetch_list}
                    for f in tqdm(concurrent.futures.as_completed(futures), total=len(futures), 
                                desc=f"Fetching embryo data for {clinic_name}", unit="pair"):
                        result_df, pair, actual_is_ongoing = f.result()
                        # If no embryos were returned and treatment is not ongoing, we still update status,
                        # but if embryos are returned, we record them
                        updated_treatment_statuses[pair] = actual_is_ongoing
                        if not result_df.empty:
                            all_embryo_data.append(result_df)
                
                total_embryos_fetched = sum(len(df) for df in all_embryo_data)
                pairs_with_embryos = len(all_embryo_data)
                pairs_without_embryos = len(pairs_to_fetch_list) - pairs_with_embryos
                
                self.logger.info(f"[{clinic_name}] Embryo data extraction complete:")
                self.logger.info(f"  - Pairs processed: {len(pairs_to_fetch_list)}")
                self.logger.info(f"  - Pairs with embryo data: {pairs_with_embryos}")
                self.logger.info(f"  - Pairs without embryo data: {pairs_without_embryos}")
                self.logger.info(f"  - Total embryos fetched: {total_embryos_fetched}")
                
                if all_embryo_data:
                    embryo_data_df = pd.concat(all_embryo_data, ignore_index=True)
                else:
                    embryo_data_df = pd.DataFrame()
            else:
                embryo_data_df = pd.DataFrame()
                self.logger.info(f"[{clinic_name}] No embryo data to fetch.")

            # 4b. Fetch IDA score data for the clinic
            self.logger.info(f"[{clinic_name}] Fetching IDA score data from API...")
            idascore_data = api_client.get_idascore()
            if idascore_data is not None:
                if 'Scores' in idascore_data:
                    db_manager.save_bronze_raw('idascore', idascore_data['Scores'], extraction_timestamp, run_id, clinic_name)
                idascore_df = data_processor.process_idascore(idascore_data, extraction_timestamp, run_id)
            else:
                idascore_df = pd.DataFrame()
            self.logger.info(f"[{clinic_name}] Fetched {len(idascore_df)} IDA score records from API.")

            # 5. Build treatments to save with updated is_ongoing status
            if updated_treatment_statuses:
                import hashlib
                # Filter treatments_df to pairs that were updated
                treatments_to_save_list = []
                for _, row in treatments_df.iterrows():
                    pair = (str(row['PatientIDx']), str(row['TreatmentName']))
                    if pair in updated_treatment_statuses:
                        row_dict = row.to_dict()
                        is_ong = updated_treatment_statuses[pair]
                        row_dict['is_ongoing'] = is_ong
                        row_dict['_location'] = clinic_name
                        row_dict['_extraction_timestamp'] = extraction_timestamp
                        row_dict['_run_id'] = run_id
                        hash_str = f"{row_dict.get('PatientIDx')}_{row_dict.get('TreatmentName')}_{is_ong}_{clinic_name}"
                        row_dict['_row_hash'] = hashlib.md5(hash_str.encode()).hexdigest()
                        treatments_to_save_list.append(row_dict)
                treatments_to_save = pd.DataFrame(treatments_to_save_list)
            else:
                treatments_to_save = pd.DataFrame()
            
            self.logger.info(f"[{clinic_name}] Saving data to DuckDB...")
            data_to_save = {
                'patients': patients_df,
                'treatments': treatments_to_save,
                'embryo_data': embryo_data_df,
                'idascore': idascore_df
            }
            row_counts = db_manager.save_data(data_to_save, clinic_name, run_id, extraction_timestamp)
            self.logger.info(f"[{clinic_name}] Saved data to {db_path}: {row_counts}")
            
            # Final summary
            self.logger.info(f"[{clinic_name}] Extraction summary:")
            self.logger.info(f"  - Pairs extracted/updated: {len(updated_treatment_statuses)}")
            self.logger.info(f"  - Embryo records added: {row_counts.get('embryo_data', 0)}")
            self.logger.info(f"  - Total records processed: {sum(row_counts.values())}")
            
            self.logger.info(f"[{clinic_name}] Extraction complete.")
            return True
        except Exception as e:
            self.logger.error(f"[{clinic_name}] Error in extraction: {e}")
            return False
    
    def extract_single_location(self, location: str, backfill: bool = False) -> bool:
        """
        Extract data from a single embryoscope location.
        
        Args:
            location: Location name
            backfill: Whether to run full backfill
            
        Returns:
            True if extraction successful, False otherwise
        """
        try:
            # Get location configuration
            config = self.config_manager.get_embryoscope_config(location)
            if not config or not config.get('enabled', False):
                self.logger.warning(f"Location {location} is not enabled or not found")
                return False
            
            return self._extract_clinic_data(location, config, backfill=backfill)
            
        except Exception as e:
            self.logger.error(f"Error in extraction for {location}: {e}")
            return False
    
    def extract_all_locations(self, parallel: bool = True, backfill: bool = False) -> Dict[str, bool]:
        """
        Extract data from all enabled embryoscope locations.
        
        Args:
            parallel: Whether to run extractions in parallel
            backfill: Whether to run full backfill
            
        Returns:
            Dictionary with extraction results for each location
        """
        enabled_embryoscopes = self.config_manager.get_enabled_embryoscopes()
        
        if not enabled_embryoscopes:
            self.logger.error("No enabled embryoscopes found")
            return {}
        
        self.logger.info(f"Starting extraction for {len(enabled_embryoscopes)} locations")
        
        results = {}
        
        if parallel and self.config_manager.is_parallel_processing_enabled():
            # Parallel extraction
            max_workers = self.config_manager.get_max_workers()
            self.logger.info(f"Running parallel extraction with {max_workers} workers")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all extraction tasks
                future_to_location = {
                    executor.submit(self.extract_single_location, location, backfill=backfill): location
                    for location in enabled_embryoscopes.keys()
                }
                
                # Collect results - simple approach that handles failures gracefully
                for future in concurrent.futures.as_completed(future_to_location):
                    location = future_to_location[future]
                    try:
                        result = future.result()
                        results[location] = result
                        self.logger.info(f"Completed extraction for {location}: {'SUCCESS' if result else 'FAILED'}")
                    except Exception as e:
                        self.logger.error(f"Exception in extraction for {location}: {e}")
                        results[location] = False
        else:
            # Sequential extraction
            self.logger.info("Running sequential extraction")
            for location in enabled_embryoscopes.keys():
                result = self.extract_single_location(location, backfill=backfill)
                results[location] = result
                self.logger.info(f"Completed extraction for {location}: {'SUCCESS' if result else 'FAILED'}")
        
        # Log summary
        successful = sum(1 for result in results.values() if result)
        total = len(results)
        self.logger.info(f"Extraction completed. {successful}/{total} locations successful")
        
        return results
    
    def get_extraction_summary(self) -> Dict[str, Any]:
        """
        Get summary of all extractions.
        
        Returns:
            Dictionary with extraction summary
        """
        summary = {
            'enabled_locations': list(self.config_manager.get_enabled_embryoscopes().keys()),
            'data_summary': {},
            'extraction_history': {}
        }
        
        # Get data summary for each location
        for location in self.config_manager.get_enabled_embryoscopes().keys():
            db_path = self._get_db_path(location)
            try:
                db_manager = EmbryoscopeDatabaseManager(db_path)
                summary['data_summary'][location] = db_manager.get_data_summary(location)
                summary['extraction_history'][location] = db_manager.get_extraction_history(location, limit=5)
            except Exception as e:
                self.logger.warning(f"Could not get summary for {location}: {e}")
                summary['data_summary'][location] = {}
                summary['extraction_history'][location] = pd.DataFrame()
        
        return summary

    def extract_for_patients(self, clinic_name: str, patient_ids: list, db_path: Optional[str] = None, backfill: bool = False) -> bool:
        """
        Public method to extract data for a subset of patients for a given clinic, optionally to a custom DB path.
        """
        config = self.config_manager.get_embryoscope_config(clinic_name)
        if not config or not config.get('enabled', False):
            self.logger.warning(f"Location {clinic_name} is not enabled or not found")
            return False
        return self._extract_clinic_data(clinic_name, config, patient_ids=patient_ids, db_path=db_path, backfill=backfill)


def main():
    """Main function to run the embryoscope extraction."""
    import argparse
    parser = argparse.ArgumentParser(description="Embryoscope Data Extraction")
    parser.add_argument("--backfill", action="store_true", help="Perform a full backfill/read all")
    parser.add_argument("--clinic", type=str, default=None, help="Extract only for a specific clinic")
    parser.add_argument("--patients", nargs="+", default=None, help="Extract only for specific patient IDs")
    args = parser.parse_args()
    
    try:
        # Initialize extractor
        extractor = EmbryoscopeExtractor()
        
        # Print configuration summary
        extractor.config_manager.print_config_summary()
        
        # Run extraction
        extractor.logger.info("Starting embryoscope data extraction...")
        if args.clinic:
            if args.patients:
                success = extractor.extract_for_patients(args.clinic, args.patients, backfill=args.backfill)
            else:
                success = extractor.extract_single_location(args.clinic, backfill=args.backfill)
            results = {args.clinic: success}
        else:
            results = extractor.extract_all_locations(parallel=True, backfill=args.backfill)
        
        # Print results
        extractor.logger.info("\nExtraction Results:")
        for location, success in results.items():
            status = "SUCCESS" if success else "FAILED"
            extractor.logger.info(f"  {location}: {status}")
        
        # Print summary
        extractor.logger.info("\nExtraction Summary:")
        summary = extractor.get_extraction_summary()
        for location, data_summary in summary['data_summary'].items():
            extractor.logger.info(f"  {location}:")
            for data_type, stats in data_summary.items():
                extractor.logger.info(f"    {data_type}: {stats['count']} records (last: {stats['last_extraction']})")
        
    except Exception as e:
        import traceback
        print(f"ERROR in main execution: {e}")
        print("Full traceback:")
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 