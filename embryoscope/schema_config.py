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
            'BirthDate': lambda row: row.get('DateOfBirth', '').replace('.', '-') if isinstance(row.get('DateOfBirth', ''), str) else None
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
            'AnnotationList': lambda row: json.dumps(row.get('AnnotationList', [])) if isinstance(row.get('AnnotationList', []), (list, dict)) else None,
            'InstrumentNumber': lambda row: row.get('EmbryoDetails', {}).get('InstrumentNumber') if isinstance(row.get('EmbryoDetails', {}), dict) else None,
            'Position': lambda row: row.get('EmbryoDetails', {}).get('Position') if isinstance(row.get('EmbryoDetails', {}), dict) else None,
            'WellNumber': lambda row: row.get('EmbryoDetails', {}).get('WellNumber') if isinstance(row.get('EmbryoDetails', {}), dict) else None,
            'FertilizationTime': lambda row: row.get('EmbryoDetails', {}).get('FertilizationTime') if isinstance(row.get('EmbryoDetails', {}), dict) else None,
            'EmbryoFate': lambda row: row.get('EmbryoDetails', {}).get('EmbryoFate') if isinstance(row.get('EmbryoDetails', {}), dict) else None,
            'Description': lambda row: row.get('EmbryoDetails', {}).get('Description') if isinstance(row.get('EmbryoDetails', {}), dict) else None,
            'EmbryoDescriptionID': lambda row: row.get('EmbryoDetails', {}).get('EmbryoDescriptionID') if isinstance(row.get('EmbryoDetails', {}), dict) else None,
            'EvaluationModel': lambda row: row.get('Evaluation', {}).get('Model') if isinstance(row.get('Evaluation', {}), dict) else None,
            'EvaluationScore': lambda row: row.get('Evaluation', {}).get('Evaluation') if isinstance(row.get('Evaluation', {}), dict) else None,
            'EvaluationUser': lambda row: row.get('Evaluation', {}).get('User') if isinstance(row.get('Evaluation', {}), dict) else None,
            'EvaluationDate': lambda row: row.get('Evaluation', {}).get('EvaluationDate') if isinstance(row.get('Evaluation', {}), dict) else None
        }
    },
    'idascore': {
        'api_fields': ['EmbryoID', 'Score', 'Viability'],
        'db_columns': ['EmbryoID', 'Score', 'Viability'],
        'transformations': {
            'Score': lambda row: float(row.get('Score', 0)) if isinstance(row.get('Score', 0), (int, float, str)) and row.get('Score') not in [None, ''] else None,
            'Viability': lambda row: row.get('Viability') if isinstance(row.get('Viability', None), str) else None
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