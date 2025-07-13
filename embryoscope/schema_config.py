"""
Schema Configuration for Embryoscope Data
Defines mappings between API responses and database schemas.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
import json

# Database table schemas for each data type
TABLE_SCHEMAS = {
    'patients': {
        'columns': [
            'PatientIDx VARCHAR',
            'Name VARCHAR', 
            'BirthDate DATE',
            '_location VARCHAR',
            '_extraction_timestamp TIMESTAMP',
            '_run_id VARCHAR',
            '_row_hash VARCHAR'
        ],
        'primary_key': ['PatientIDx', '_location', '_extraction_timestamp']
    },
    'treatments': {
        'columns': [
            'PatientIDx VARCHAR',
            'TreatmentName VARCHAR',
            '_location VARCHAR',
            '_extraction_timestamp TIMESTAMP',
            '_run_id VARCHAR',
            '_row_hash VARCHAR'
        ],
        'primary_key': ['PatientIDx', 'TreatmentName', '_location', '_extraction_timestamp']
    },
    'embryo_data': {
        'columns': [
            'EmbryoID VARCHAR',
            'PatientIDx VARCHAR',
            'TreatmentName VARCHAR',
            'AnnotationList TEXT',
            'InstrumentNumber VARCHAR',
            'Position VARCHAR',
            'WellNumber VARCHAR',
            'FertilizationTime VARCHAR',
            'EmbryoFate VARCHAR',
            'Description VARCHAR',
            'EmbryoDescriptionID VARCHAR',
            'EvaluationModel VARCHAR',
            'EvaluationScore VARCHAR',
            'EvaluationUser VARCHAR',
            'EvaluationDate VARCHAR',
            '_location VARCHAR',
            '_extraction_timestamp TIMESTAMP',
            '_run_id VARCHAR',
            '_row_hash VARCHAR'
        ],
        'primary_key': ['EmbryoID', '_location', '_extraction_timestamp']
    },
    'idascore': {
        'columns': [
            'EmbryoID VARCHAR',
            'Score REAL',
            'Viability VARCHAR',
            '_location VARCHAR',
            '_extraction_timestamp TIMESTAMP',
            '_run_id VARCHAR',
            '_row_hash VARCHAR'
        ],
        'primary_key': ['EmbryoID', '_location', '_extraction_timestamp']
    }
}

# API to Database column mappings for each data type
COLUMN_MAPPINGS = {
    'patients': {
        'api_fields': ['PatientIDx', 'PatientID', 'FirstName', 'LastName', 'DateOfBirth'],
        'db_columns': ['PatientIDx', 'Name', 'BirthDate'],
        'transformations': {
            'Name': lambda row: f"{row.get('FirstName', '')} {row.get('LastName', '')}".strip(),
            'BirthDate': lambda row: row.get('DateOfBirth', '').replace('.', '-') if row.get('DateOfBirth') else None
        }
    },
    'treatments': {
        'api_fields': ['TreatmentList'],  # This is a list, not individual fields
        'db_columns': ['PatientIDx', 'TreatmentName'],
        'transformations': {
            'PatientIDx': lambda row, patient_idx: patient_idx,
            'TreatmentName': lambda row: row  # row is the treatment name string
        }
    },
    'embryo_data': {
        'api_fields': ['EmbryoID', 'AnnotationList', 'EmbryoDetails', 'Evaluation'],
        'db_columns': [
            'EmbryoID', 'PatientIDx', 'TreatmentName', 'AnnotationList',
            'InstrumentNumber', 'Position', 'WellNumber', 'FertilizationTime',
            'EmbryoFate', 'Description', 'EmbryoDescriptionID',
            'EvaluationModel', 'EvaluationScore', 'EvaluationUser', 'EvaluationDate'
        ],
        'transformations': {
            'PatientIDx': lambda row, patient_idx: patient_idx,
            'TreatmentName': lambda row, treatment_name: treatment_name,
            'AnnotationList': lambda row: json.dumps(row.get('AnnotationList', [])) if row.get('AnnotationList') else None,
            'InstrumentNumber': lambda row: row.get('EmbryoDetails', {}).get('InstrumentNumber'),
            'Position': lambda row: row.get('EmbryoDetails', {}).get('Position'),
            'WellNumber': lambda row: row.get('EmbryoDetails', {}).get('WellNumber'),
            'FertilizationTime': lambda row: row.get('EmbryoDetails', {}).get('FertilizationTime'),
            'EmbryoFate': lambda row: row.get('EmbryoDetails', {}).get('EmbryoFate'),
            'Description': lambda row: row.get('EmbryoDetails', {}).get('Description'),
            'EmbryoDescriptionID': lambda row: row.get('EmbryoDetails', {}).get('EmbryoDescriptionID'),
            'EvaluationModel': lambda row: row.get('Evaluation', {}).get('Model'),
            'EvaluationScore': lambda row: row.get('Evaluation', {}).get('Evaluation'),
            'EvaluationUser': lambda row: row.get('Evaluation', {}).get('User'),
            'EvaluationDate': lambda row: row.get('Evaluation', {}).get('EvaluationDate')
        }
    },
    'idascore': {
        'api_fields': ['EmbryoID', 'Score', 'Viability'],
        'db_columns': ['EmbryoID', 'Score', 'Viability'],
        'transformations': {
            'Score': lambda row: float(row.get('Score', 0)) if row.get('Score') else None,
            'Viability': lambda row: row.get('Viability')
        }
    }
}

# API response structure definitions
API_STRUCTURES = {
    'patients': {
        'root_key': 'Patients',
        'is_list': True,
        'requires_patient_context': False
    },
    'treatments': {
        'root_key': 'TreatmentList',
        'is_list': True,
        'requires_patient_context': True
    },
    'embryo_data': {
        'root_key': 'EmbryoDataList',
        'is_list': True,
        'requires_patient_context': True
    },
    'idascore': {
        'root_key': 'Scores',
        'is_list': True,
        'requires_patient_context': False
    }
}

def get_table_schema(data_type: str) -> Dict[str, Any]:
    """Get table schema for a data type."""
    return TABLE_SCHEMAS.get(data_type, {})

def get_column_mapping(data_type: str) -> Dict[str, Any]:
    """Get column mapping for a data type."""
    return COLUMN_MAPPINGS.get(data_type, {})

def get_api_structure(data_type: str) -> Dict[str, Any]:
    """Get API structure definition for a data type."""
    return API_STRUCTURES.get(data_type, {})

def get_supported_data_types() -> List[str]:
    """Get list of supported data types."""
    return list(TABLE_SCHEMAS.keys())

def validate_data_type(data_type: str) -> bool:
    """Validate if a data type is supported."""
    return data_type in TABLE_SCHEMAS 