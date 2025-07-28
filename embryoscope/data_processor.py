"""
Data Processor for Embryoscope Data Extraction
Handles flattening JSON structures and preparing data for database storage.
"""

import pandas as pd
import json
import hashlib
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
from schema_config import get_column_mapping, get_api_structure, validate_data_type

# Metadata columns to exclude from hash
METADATA_COLUMNS = {'_location', '_extraction_timestamp', '_run_id', '_row_hash'}


class EmbryoscopeDataProcessor:
    """Processes and flattens embryoscope data for database storage."""
    
    def __init__(self, location: str):
        """
        Initialize the data processor.
        
        Args:
            location: Embryoscope location name
        """
        self.location = location
        self.logger = logging.getLogger(f"embryoscope_processor_{location}")
    
    def _generate_row_hash(self, data: Dict[str, Any]) -> str:
        """
        Generate MD5 hash for a row of data.
        
        Args:
            data: Dictionary containing row data
            
        Returns:
            MD5 hash string
        """
        # Convert to sorted string for consistent hashing
        data_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.md5(data_str.encode()).hexdigest()
    
    def _add_metadata_columns(self, df: pd.DataFrame, extraction_timestamp: datetime, run_id: str, data_type: str = '') -> pd.DataFrame:
        """
        Add metadata columns to the dataframe and generate _row_hash from business columns only.
        Args:
            df: Input dataframe
            extraction_timestamp: Timestamp of extraction
            run_id: Unique run identifier
            data_type: Type of data (patients, treatments, etc.)
        Returns:
            Dataframe with metadata columns added
        """
        df = df.copy()
        df['_location'] = self.location
        df['_extraction_timestamp'] = extraction_timestamp
        df['_run_id'] = run_id
        # Determine business columns for hashing
        dtype = data_type if isinstance(data_type, str) else ''
        if dtype:
            db_columns = get_column_mapping(dtype).get('db_columns', [])
            business_columns = [col for col in db_columns if col not in METADATA_COLUMNS]
        else:
            business_columns = [col for col in df.columns if col not in METADATA_COLUMNS]
        def business_row_hash(row):
            # row is a Series
            data = {col: row[col] if col in row else None for col in business_columns}
            return self._generate_row_hash(data)
        df['_row_hash'] = df.apply(business_row_hash, axis=1)
        return df
    
    def process_data_generic(self, data: Dict[str, Any], data_type: str, extraction_timestamp: datetime, 
                           run_id: str, patient_idx: str = None, treatment_name: str = None) -> pd.DataFrame:
        """
        Generic data processing method using schema configuration.
        
        Args:
            data: Raw data from API
            data_type: Type of data (patients, treatments, embryo_data, idascore)
            extraction_timestamp: Timestamp of extraction
            run_id: Unique run identifier
            patient_idx: Patient identifier (for context-dependent data)
            treatment_name: Treatment name (for context-dependent data)
            
        Returns:
            Processed dataframe with flattened structure
        """
        if not validate_data_type(data_type):
            self.logger.error(f"Unsupported data type: {data_type}")
            return pd.DataFrame()
        
        # Get API structure and column mapping
        api_structure = get_api_structure(data_type)
        column_mapping = get_column_mapping(data_type)
        
        if not api_structure or not column_mapping:
            self.logger.error(f"No configuration found for data type: {data_type}")
            return pd.DataFrame()
        
        # Extract data from API response
        root_key = api_structure['root_key']
        is_list = api_structure['is_list']
        
        if not data or root_key not in data:
            self.logger.warning(f"No {data_type} data found for {self.location}")
            return pd.DataFrame()
        
        raw_data = data[root_key]
        if not raw_data:
            self.logger.warning(f"Empty {data_type} list for {self.location}")
            return pd.DataFrame()
        
        # Process data based on structure
        if is_list:
            # Handle list-based data (patients, treatments, embryo_data, idascore)
            if data_type == 'treatments':
                # Treatments come as a list of strings
                processed_records = []
                for treatment_name_item in raw_data:
                    record = {'TreatmentName': treatment_name_item}
                    if patient_idx:
                        record['PatientIDx'] = patient_idx
                    processed_records.append(record)
                df = pd.DataFrame(processed_records)
            else:
                # Other data types come as list of objects
                df = pd.DataFrame(raw_data)
        else:
            # Handle single object data (if any)
            df = pd.DataFrame([raw_data])
        
        if df.empty:
            return df
        
        # Apply transformations based on schema configuration
        transformations = column_mapping.get('transformations', {})
        db_columns = column_mapping.get('db_columns', [])
        
        # Apply transformations
        for db_col, transform_func in transformations.items():
            if db_col in db_columns:
                if data_type == 'treatments' and db_col == 'PatientIDx':
                    df[db_col] = patient_idx
                elif data_type == 'treatments' and db_col == 'PatientID':
                    # For treatments, we need to get PatientID from the patient context
                    # This will be handled during consolidation
                    df[db_col] = None
                elif data_type == 'treatments' and db_col == 'TreatmentName':
                    df[db_col] = df['TreatmentName']  # Already set above
                elif data_type == 'embryo_data' and db_col in ['PatientIDx', 'TreatmentName']:
                    if db_col == 'PatientIDx':
                        df[db_col] = patient_idx
                    elif db_col == 'TreatmentName':
                        df[db_col] = treatment_name
                elif data_type == 'embryo_data' and db_col == 'PatientID':
                    # For embryo_data, we need to get PatientID from the patient context
                    # This will be handled during consolidation
                    df[db_col] = None
                else:
                    # Apply transformation function
                    try:
                        if data_type == 'patients':
                            df[db_col] = df.apply(transform_func, axis=1)
                        else:
                            df[db_col] = df.apply(lambda row: transform_func(row), axis=1)
                    except Exception as e:
                        self.logger.warning(f"Error applying transformation for {db_col}: {e}")
                        df[db_col] = None
        
        # Keep only the columns that match the database schema
        if db_columns:
            available_columns = [col for col in db_columns if col in df.columns]
            df = df[available_columns]
        
        # Ensure data_type is always a string and never None
        safe_data_type = str(data_type or '')
        # Ensure df is a DataFrame
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)
        df = self._add_metadata_columns(df, extraction_timestamp, run_id, safe_data_type)

        # Commented out: logging now handled in extractor for treatments
        # if len(df) >= 100 and len(df) % 100 == 0:
        #     self.logger.info(f"Processed {len(df)} {data_type} records for {self.location}")
        # elif len(df) < 100 and len(df) > 0:
        #     self.logger.info(f"Processed {len(df)} {data_type} records for {self.location}")
        return df
    
    def process_patients(self, patients_data: Dict[str, Any], extraction_timestamp: datetime, run_id: str) -> pd.DataFrame:
        """
        Process patients data and flatten JSON structure.
        
        Args:
            patients_data: Raw patients data from API
            extraction_timestamp: Timestamp of extraction
            run_id: Unique run identifier
            
        Returns:
            Processed dataframe with flattened structure
        """
        return self.process_data_generic(patients_data, 'patients', extraction_timestamp, run_id)
    
    def process_treatments(self, treatments_data: Dict[str, Any], patient_idx: str, 
                          extraction_timestamp: datetime, run_id: str) -> pd.DataFrame:
        """
        Process treatments data and flatten JSON structure.
        
        Args:
            treatments_data: Raw treatments data from API
            patient_idx: Patient identifier
            extraction_timestamp: Timestamp of extraction
            run_id: Unique run identifier
            
        Returns:
            Processed dataframe with flattened structure
        """
        return self.process_data_generic(treatments_data, 'treatments', extraction_timestamp, run_id, patient_idx)
    
    def process_embryo_data(self, embryo_data: Dict[str, Any], patient_idx: str, treatment_name: str,
                           extraction_timestamp: datetime, run_id: str) -> pd.DataFrame:
        """
        Process embryo data and flatten nested JSON structures.
        
        Args:
            embryo_data: Raw embryo data from API
            patient_idx: Patient identifier
            treatment_name: Treatment name
            extraction_timestamp: Timestamp of extraction
            run_id: Unique run identifier
            
        Returns:
            Processed dataframe with flattened structure
        """
        return self.process_data_generic(embryo_data, 'embryo_data', extraction_timestamp, run_id, patient_idx, treatment_name)
    
    def process_idascore(self, idascore_data: Dict[str, Any], extraction_timestamp: datetime, run_id: str) -> pd.DataFrame:
        """
        Process IDA score data and flatten JSON structure.
        
        Args:
            idascore_data: Raw IDA score data from API
            extraction_timestamp: Timestamp of extraction
            run_id: Unique run identifier
            
        Returns:
            Processed dataframe with flattened structure
        """
        return self.process_data_generic(idascore_data, 'idascore', extraction_timestamp, run_id)
    
    def process_all_data(self, api_data: Dict[str, Any], extraction_timestamp: datetime, run_id: str) -> Dict[str, pd.DataFrame]:
        """
        Process all data types from API response.
        
        Args:
            api_data: Dictionary containing all API data
            extraction_timestamp: Timestamp of extraction
            run_id: Unique run identifier
            
        Returns:
            Dictionary with processed dataframes for each data type
        """
        processed_data = {}
        
        # Process patients
        if 'patients' in api_data:
            processed_data['patients'] = self.process_patients(
                api_data['patients'], extraction_timestamp, run_id
            )
        
        # Process treatments
        if 'treatments' in api_data:
            all_treatments = []
            for patient_idx, treatments_data in api_data['treatments'].items():
                df_treatments = self.process_treatments(
                    treatments_data, patient_idx, extraction_timestamp, run_id
                )
                if not df_treatments.empty:
                    all_treatments.append(df_treatments)
            
            if all_treatments:
                processed_data['treatments'] = pd.concat(all_treatments, ignore_index=True)
            else:
                processed_data['treatments'] = pd.DataFrame()
        
        # Process embryo data
        if 'embryo_data' in api_data:
            all_embryos = []
            for (patient_idx, treatment_name), embryo_data in api_data['embryo_data'].items():
                df_embryos = self.process_embryo_data(
                    embryo_data, patient_idx, treatment_name, extraction_timestamp, run_id
                )
                if not df_embryos.empty:
                    all_embryos.append(df_embryos)
            
            if all_embryos:
                processed_data['embryo_data'] = pd.concat(all_embryos, ignore_index=True)
            else:
                processed_data['embryo_data'] = pd.DataFrame()
        
        # Process IDA scores
        if 'idascore' in api_data:
            processed_data['idascore'] = self.process_idascore(
                api_data['idascore'], extraction_timestamp, run_id
            )
        
        return processed_data
    
    def get_data_summary(self, processed_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Get summary statistics for processed data.
        
        Args:
            processed_data: Dictionary with processed dataframes
            
        Returns:
            Dictionary with summary statistics
        """
        summary = {
            'location': self.location,
            'patients_count': 0,
            'treatments_count': 0,
            'embryos_count': 0,
            'idascore_count': 0,
            'total_rows': 0
        }
        
        if 'patients' in processed_data and not processed_data['patients'].empty:
            summary['patients_count'] = len(processed_data['patients'])
            summary['total_rows'] += summary['patients_count']
        
        if 'treatments' in processed_data and not processed_data['treatments'].empty:
            summary['treatments_count'] = len(processed_data['treatments'])
            summary['total_rows'] += summary['treatments_count']
        
        if 'embryo_data' in processed_data and not processed_data['embryo_data'].empty:
            summary['embryos_count'] = len(processed_data['embryo_data'])
            summary['total_rows'] += summary['embryos_count']
        
        if 'idascore' in processed_data and not processed_data['idascore'].empty:
            summary['idascore_count'] = len(processed_data['idascore'])
            summary['total_rows'] += summary['idascore_count']
        
        return summary


if __name__ == "__main__":
    # Test the data processor
    processor = EmbryoscopeDataProcessor("Test Location")
    
    # Test data
    test_patients = {
        "Patients": [
            {"PatientIDx": "TEST001", "Name": "Test Patient 1"},
            {"PatientIDx": "TEST002", "Name": "Test Patient 2"}
        ]
    }
    
    test_treatments = {
        "TreatmentList": ["Treatment 1", "Treatment 2"]
    }
    
    extraction_timestamp = datetime.now()
    run_id = "test_run_001"
    
    # Process test data
    df_patients = processor.process_patients(test_patients, extraction_timestamp, run_id)
    df_treatments = processor.process_treatments(test_treatments, "TEST001", extraction_timestamp, run_id)
    
    print("Processed patients:")
    print(df_patients)
    print("\nProcessed treatments:")
    print(df_treatments) 