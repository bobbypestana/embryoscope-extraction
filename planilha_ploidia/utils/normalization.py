"""Value normalization utilities for data comparison"""
import re
import math
import pandas as pd
from config.language_maps import LANGUAGE_MAP


def normalize_spacing(text):
    """
    Normalize spacing around commas and slashes.
    
    Args:
        text: String to normalize
    
    Returns:
        str: Normalized string
    """
    text = re.sub(r'\s*,\s*', ', ', text)
    text = re.sub(r'\s*/\s*', '/', text)
    return text


def normalize_patient_id(value):
    """
    Normalize Patient ID format (handles dots, commas, decimals).
    
    Examples:
        '825,960' -> 825960
        107.805 -> 107805
        '154.562' -> 154562
    
    Args:
        value: Patient ID value (string, float, or int)
    
    Returns:
        int: Normalized patient ID
    """
    if pd.isna(value):
        return value
    
    try:
        if isinstance(value, str):
            # Remove both '.' and ',' which can be used as thousands separators
            clean_value = value.replace('.', '').replace(',', '').strip()
            return int(clean_value)
        elif isinstance(value, float):
            # For floats like 107.805, multiply by 1000 to get 107805
            return int(value * 1000)
        else:
            return int(value)
    except (ValueError, TypeError):
        return value


def normalize_numeric(value):
    """
    Truncate numeric value to 1 decimal place.
    
    Args:
        value: Numeric value
    
    Returns:
        float: Truncated value
    """
    try:
        numeric_value = float(value)
        # Truncate to 1 decimal place: multiply by 10, floor, divide by 10
        return math.floor(numeric_value * 10) / 10
    except (ValueError, TypeError):
        return value


def numbers_match(val1, val2):
    """
    Check if two numbers match when either truncated or rounded to 1 decimal place.
    
    Args:
        val1: First value
        val2: Second value
    
    Returns:
        bool: True if values match via truncation or rounding
    """
    try:
        num1 = float(val1)
        num2 = float(val2)
        
        # Truncate to 1 decimal
        trunc1 = math.floor(num1 * 10) / 10
        trunc2 = math.floor(num2 * 10) / 10
        
        # Round to 1 decimal
        round1 = round(num1, 1)
        round2 = round(num2, 1)
        
        # Match if either truncated or rounded values are equal
        return (trunc1 == trunc2) or (round1 == round2)
    except (ValueError, TypeError):
        return False


def normalize_value(value, column_name=None):
    """
    Normalize values to ignore language differences, rounding, and type differences.
    
    Handles:
    - All numeric values: truncate to 1 decimal place
    - Language differences for Diagnosis and Oocyte Source
    - Case-insensitive string comparison
    - Patient ID: Remove dots and convert to integer
    - Video ID/Slide ID: Remove well number suffix
    
    Args:
        value: Value to normalize
        column_name: Name of the column (for special handling)
    
    Returns:
        Normalized value
    """
    if pd.isna(value):
        return value
    
    # Special handling for Patient ID
    if column_name == "Patient ID":
        return normalize_patient_id(value)
    
    # Special handling for Video ID/Slide ID: remove well number suffix
    if column_name in ["Video ID", "Slide ID"]:
        if isinstance(value, str):
            # Remove the well number suffix (e.g., -10, -2)
            normalized = re.sub(r'-\d+$', '', value)
            return normalized
        return value
    
    # Try to convert to numeric and truncate
    try:
        return normalize_numeric(value)
    except (ValueError, TypeError):
        pass
    
    # String normalization
    if isinstance(value, str):
        normalized = value.lower().strip()
        # Normalize spacing
        normalized = normalize_spacing(normalized)
        
        # Apply language mappings
        return LANGUAGE_MAP.get(normalized, normalized)
    
    return value
