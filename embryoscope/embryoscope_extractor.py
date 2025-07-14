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

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config_manager import EmbryoscopeConfigManager
from api_client import EmbryoscopeAPIClient
from data_processor import EmbryoscopeDataProcessor
from database_manager import EmbryoscopeDatabaseManager


class EmbryoscopeExtractor:
    """Main class for orchestrating embryoscope data extraction."""
    
    def __init__(self, config_path: str = "params.yml"):
        """
        Initialize the embryoscope extractor.
        
        Args:
            config_path: Path to configuration file
        """
        self.config_manager = EmbryoscopeConfigManager(config_path)
        self.logger = self._setup_logging()
        
        # Validate configuration
        if not self.config_manager.validate_config():
            raise ValueError("Invalid configuration")
        
        self.logger.info("Embryoscope extractor initialized successfully")
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        # Create logs directory
        os.makedirs('logs', exist_ok=True)
        
        # Setup logging
        logger = logging.getLogger('embryoscope_extractor')
        
        # Get log level from config
        log_level_str = self.config_manager.config['extraction'].get('log_level', 'INFO')
        log_level = getattr(logging, log_level_str.upper(), logging.INFO)
        logger.setLevel(log_level)
        
        # Create handlers
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_handler = logging.FileHandler(f'logs/embryoscope_extraction_{timestamp}.log')
        console_handler = logging.StreamHandler()
        
        # Create formatters and add it to handlers
        log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(log_format)
        console_handler.setFormatter(log_format)
        
        # Add handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def _get_db_path(self, clinic_name: str) -> str:
        """Get database path for a specific clinic."""
        safe_name = clinic_name.lower().replace(' ', '_')
        return f"../database/embryoscope_{safe_name}.db"
    
    def _extract_clinic_data(self, clinic_name: str, config: Dict[str, Any]) -> bool:
        """
        Extract data for a single clinic, writing to its own DuckDB file.
        Only fetch embryo data for new patient-treatment pairs.
        
        Args:
            clinic_name: Clinic name
            config: Clinic configuration
            
        Returns:
            True if extraction successful, False otherwise
        """
        self.logger.info(f"Starting extraction for clinic: {clinic_name}")
        
        # Setup clinic-specific database
        db_path = self._get_db_path(clinic_name)
        db_manager = EmbryoscopeDatabaseManager(db_path)
        
        # Initialize API client and data processor
        rate_limit_delay = self.config_manager.get_rate_limit_delay()
        api_client = EmbryoscopeAPIClient(clinic_name, config, rate_limit_delay)
        data_processor = EmbryoscopeDataProcessor(clinic_name)
        
        extraction_timestamp = datetime.now()
        run_id = str(uuid.uuid4())
        
        try:
            # 1. Get all patients
            self.logger.info(f"[{clinic_name}] Fetching all patients from API...")
            patients_data = api_client.get_patients()
            if patients_data is None:
                self.logger.error(f"[{clinic_name}] Failed to fetch patients data")
                return False
            patients_df = data_processor.process_patients(patients_data, extraction_timestamp, run_id)
            self.logger.info(f"[{clinic_name}] Fetched {len(patients_df)} patients from API.")
            print(f"[{clinic_name}] Fetched {len(patients_df)} patients from API.")

            # 1b. Get ongoing patients
            self.logger.info(f"[{clinic_name}] Fetching ongoing patients from API...")
            ongoing_patients_data = api_client.get_ongoing_patients()
            ongoing_patient_idxs = set()
            if ongoing_patients_data and 'Patients' in ongoing_patients_data:
                ongoing_patient_list = ongoing_patients_data['Patients']
                for patient in ongoing_patient_list:
                    idx = patient.get('PatientIDx') or patient.get('PatientIdx') or patient.get('PatientID')
                    if idx:
                        ongoing_patient_idxs.add(str(idx))
            self.logger.info(f"[{clinic_name}] Found {len(ongoing_patient_idxs)} ongoing patients.")
            
            # 2. Get all treatments for each patient (sequential, progress bar)
            self.logger.info(f"[{clinic_name}] Fetching all treatments from API (sequential)...")
            all_treatments = []
            
            def fetch_treatments_for_patient(patient_idx):
                treatments_data = api_client.get_treatments(patient_idx)
                if treatments_data is None:
                    return pd.DataFrame()
                return data_processor.process_treatments(treatments_data, patient_idx, extraction_timestamp, run_id)
            
            patient_ids = list(patients_df['PatientIDx'])
            if patient_ids:
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    futures = {executor.submit(fetch_treatments_for_patient, pid): pid for pid in patient_ids}
                    for f in tqdm(concurrent.futures.as_completed(futures), total=len(futures), 
                                desc=f"Fetching treatments for {clinic_name}", unit="patient"):
                        result = f.result()
                        if not result.empty:
                            all_treatments.append(result)
                
                if all_treatments:
                    treatments_df = pd.concat(all_treatments, ignore_index=True)
                else:
                    treatments_df = pd.DataFrame(columns=pd.Index(['PatientIDx', 'TreatmentName']))
            else:
                treatments_df = pd.DataFrame(columns=pd.Index(['PatientIDx', 'TreatmentName']))
            
            self.logger.info(f"[{clinic_name}] Fetched {len(treatments_df)} treatments from API.")
            print(f"[{clinic_name}] Fetched {len(treatments_df)} treatments from API.")
            
            # 3. Compare with local DB to find new patient-treatment pairs
            self.logger.info(f"[{clinic_name}] Comparing with local DuckDB to find new patient-treatment pairs...")
            try:
                existing_treatments = db_manager.get_latest_data('treatments', clinic_name)
                existing_pairs = set(zip(existing_treatments['PatientIDx'], existing_treatments['TreatmentName']))
            except Exception:
                existing_pairs = set()
            
            all_pairs = set(zip(treatments_df['PatientIDx'], treatments_df['TreatmentName']))
            new_pairs = all_pairs - existing_pairs
            self.logger.info(f"[{clinic_name}] Found {len(new_pairs)} new patient-treatment pairs needing embryo data.")
            
            # 4. Fetch embryo data only for new pairs (sequential, progress bar)
            all_embryo_data = []
            
            def fetch_embryo_for_pair(pair):
                patient_idx, treatment_name = pair
                embryo_data = api_client.get_embryo_data(patient_idx, treatment_name)
                if embryo_data is None:
                    # Check if patient is ongoing; if so, skip logging as missing
                    if str(patient_idx) in ongoing_patient_idxs:
                        self.logger.info(f"[{clinic_name}] Pair (PatientIDx={patient_idx}, TreatmentName={treatment_name}) is ongoing, will retry in future runs.")
                        print(f"[{clinic_name}] Pair (PatientIDx={patient_idx}, TreatmentName={treatment_name}) is ongoing, will retry in future runs.")
                        return pd.DataFrame()  # Do not treat as missing
                    else:
                        self.logger.warning(f"[{clinic_name}] No embryo data for pair (PatientIDx={patient_idx}, TreatmentName={treatment_name}) and patient is NOT ongoing.")
                        print(f"[{clinic_name}] No embryo data for pair (PatientIDx={patient_idx}, TreatmentName={treatment_name}) and patient is NOT ongoing.")
                        return pd.DataFrame()
                return data_processor.process_embryo_data(embryo_data, patient_idx, treatment_name, extraction_timestamp, run_id)
            
            new_pairs_list = list(new_pairs)
            if new_pairs_list:
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    futures = {executor.submit(fetch_embryo_for_pair, pair): pair for pair in new_pairs_list}
                    for f in tqdm(concurrent.futures.as_completed(futures), total=len(futures), 
                                desc=f"Fetching embryo data for {clinic_name}", unit="pair"):
                        result = f.result()
                        if not result.empty:
                            all_embryo_data.append(result)
                
                if all_embryo_data:
                    embryo_data_df = pd.concat(all_embryo_data, ignore_index=True)
                else:
                    embryo_data_df = pd.DataFrame()
            else:
                embryo_data_df = pd.DataFrame()
            
            self.logger.info(f"[{clinic_name}] Fetched {len(embryo_data_df)} embryo data records from API.")
            print(f"[{clinic_name}] Fetched {len(embryo_data_df)} embryo data records from API.")
            
            # 4b. Fetch IDA score data for the clinic
            self.logger.info(f"[{clinic_name}] Fetching IDA score data from API...")
            idascore_data = api_client.get_idascore()
            if idascore_data is not None:
                idascore_df = data_processor.process_idascore(idascore_data, extraction_timestamp, run_id)
            else:
                idascore_df = pd.DataFrame()
            self.logger.info(f"[{clinic_name}] Fetched {len(idascore_df)} IDA score records from API.")
            print(f"[{clinic_name}] Fetched {len(idascore_df)} IDA score records from API.")
            
            # 5. Save all data incrementally
            self.logger.info(f"[{clinic_name}] Saving data to DuckDB...")
            data_to_save = {
                'patients': patients_df,
                'treatments': treatments_df,
                'embryo_data': embryo_data_df,
                'idascore': idascore_df
            }
            row_counts = db_manager.save_data(data_to_save, clinic_name, run_id, extraction_timestamp)
            self.logger.info(f"[{clinic_name}] Saved data to {db_path}: {row_counts}")
            print(f"[{clinic_name}] Saved data to {db_path}: {row_counts}")
            self.logger.info(f"[{clinic_name}] Extraction complete.")
            
            return True
            
        except Exception as e:
            self.logger.error(f"[{clinic_name}] Error in extraction: {e}")
            return False
    
    def extract_single_location(self, location: str) -> bool:
        """
        Extract data from a single embryoscope location.
        
        Args:
            location: Location name
            
        Returns:
            True if extraction successful, False otherwise
        """
        try:
            # Get location configuration
            config = self.config_manager.get_embryoscope_config(location)
            if not config or not config.get('enabled', False):
                self.logger.warning(f"Location {location} is not enabled or not found")
                return False
            
            return self._extract_clinic_data(location, config)
            
        except Exception as e:
            self.logger.error(f"Error in extraction for {location}: {e}")
            return False
    
    def extract_all_locations(self, parallel: bool = True) -> Dict[str, bool]:
        """
        Extract data from all enabled embryoscope locations.
        
        Args:
            parallel: Whether to run extractions in parallel
            
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
                    executor.submit(self.extract_single_location, location): location
                    for location in enabled_embryoscopes.keys()
                }
                
                # Collect results
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
                result = self.extract_single_location(location)
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


def main():
    """Main function to run the embryoscope extraction."""
    try:
        # Initialize extractor
        extractor = EmbryoscopeExtractor()
        
        # Print configuration summary
        extractor.config_manager.print_config_summary()
        
        # Run extraction
        print("\nStarting embryoscope data extraction...")
        results = extractor.extract_all_locations(parallel=True)
        
        # Print results
        print("\nExtraction Results:")
        for location, success in results.items():
            status = "SUCCESS" if success else "FAILED"
            print(f"  {location}: {status}")
        
        # Print summary
        print("\nExtraction Summary:")
        summary = extractor.get_extraction_summary()
        for location, data_summary in summary['data_summary'].items():
            print(f"  {location}:")
            for data_type, stats in data_summary.items():
                print(f"    {data_type}: {stats['count']} records (last: {stats['last_extraction']})")
        
    except Exception as e:
        print(f"Error in main execution: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 