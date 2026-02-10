import json
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def flatten_patients_json(raw_json_str):
    """
    Flatten patient JSON data.
    """
    try:
        data = json.loads(str(raw_json_str))
        # If the data is a dict and not a list, treat as a single patient record
        if isinstance(data, dict):
            return [data]
        elif isinstance(data, list):
            return data
        else:
            return []
    except Exception as e:
        logger.error(f"Error parsing JSON: {e}")
        return []

def flatten_embryo_json(raw_json_str, annotation_names_set=None, log_errors=True):
    """
    Flatten embryo JSON data, handling dynamic annotation fields.
    """
    try:
        data = json.loads(str(raw_json_str))
        flat = {}
        # Flatten top-level fields
        for k, v in data.items():
            if isinstance(v, dict):
                for subk, subv in v.items():
                    flat[f"{k}_{subk}"] = subv
            elif k == 'AnnotationList' and isinstance(v, list):
                # We'll handle this below
                continue
            else:
                flat[k] = v
        # Pivot AnnotationList
        annotation_list = data.get('AnnotationList', [])
        if annotation_names_set is not None:
            # Use the provided set to ensure all columns are present
            for ann_name in annotation_names_set:
                ann = next((a for a in annotation_list if a.get('Name') == ann_name), None)
                if ann:
                    for annk, annv in ann.items():
                        flat[f"{annk}_{ann_name}"] = annv
                else:
                    # Fill with None for missing annotation
                    flat[f"Name_{ann_name}"] = None
                    flat[f"Time_{ann_name}"] = None
                    flat[f"Value_{ann_name}"] = None
                    flat[f"Timestamp_{ann_name}"] = None
        else:
            # If no set provided, just flatten what is present
            for ann in annotation_list:
                ann_name = ann.get('Name')
                for annk, annv in ann.items():
                    flat[f"{annk}_{ann_name}"] = annv
        return flat
    except Exception as e:
        if log_errors:
            logger.error(f"Error flattening embryo JSON: {e}")
        return {}

def flatten_idascore_json(raw_json_str, meta_cols, row):
    """
    Flatten IDAScore JSON data.
    """
    try:
        record = json.loads(str(raw_json_str))
        # Map fields as requested
        mapped_record = {}
        for k, v in record.items():
            if k == 'Viability':
                mapped_record['IDAScore'] = v
            elif k == 'Time':
                mapped_record['IDATime'] = v
            elif k == 'Version':
                mapped_record['IDAVersion'] = v
            elif k == 'Timestamp':
                mapped_record['IDATimestamp'] = v
            else:
                mapped_record[k] = v
        
        # Add meta columns
        for col in meta_cols:
             mapped_record[col] = row[col]
             
        return mapped_record
    except Exception as e:
        # Caller handles logging context if needed, or we can log here
        logger.error(f"Error parsing idascore JSON: {e}")
        return None
