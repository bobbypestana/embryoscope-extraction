#!/usr/bin/env python3
"""
Test Patient Timeline Logic
Validates the timeline creation logic against hardcoded expected results.
"""

import pandas as pd
import duckdb as db
import os
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# Import the timeline functions from the utils script
from utils.create_patient_timeline import (
    get_database_connection,
    extract_timeline_data_for_patient,
    create_unified_timeline
)

# Hardcoded expected results for each test patient
EXPECTED_RESULTS = {
    876950: {
        'description': 'Standard case - no missing treatments',
        'expected_treatments_found': 0,
        'expected_1us_entries': 1,
        'expected_insertions': 0,
        'expected_timeline_events': 11,
        'expected_tables': ['congelamentos_embrioes', 'congelamentos_ovulos', 'extrato_atendimentos', 'tratamentos']
    },
    825890: {
        'description': 'Standard case - no missing treatments',
        'expected_treatments_found': 0,
        'expected_1us_entries': 1,
        'expected_insertions': 0,
        'expected_timeline_events': 17,
        'expected_tables': ['congelamentos_embrioes', 'extrato_atendimentos', 'tratamentos']
    },
    175583: {
        'description': 'Missing treatments case - should insert 2 treatments with estimated dates',
        'expected_treatments_found': 2,
        'expected_1us_entries': 6,
        'expected_insertions': 2,
        'expected_timeline_events': 26,
        'expected_tables': ['extrato_atendimentos', 'tratamentos'],
        'expected_inserted_tentativas': ['Ciclo a Fresco FIV | 2', 'Ciclo a Fresco FIV | 1'],
        'expected_insertion_dates': ['2024-10-03', '2024-09-05']
    }
}

def test_patient_timeline(prontuario: int) -> Dict:
    """
    Test the timeline creation for a specific patient
    Returns a dictionary with test results
    """
    print(f"\n{'='*80}")
    print(f"TESTING PATIENT: {prontuario}")
    print(f"Description: {EXPECTED_RESULTS[prontuario]['description']}")
    print(f"{'='*80}")
    
    # Get database connection
    conn = get_database_connection()
    
    try:
        # Extract timeline data
        timeline_data = extract_timeline_data_for_patient(conn, prontuario)
        
        # Create unified timeline
        timeline_df = create_unified_timeline(timeline_data, prontuario)
        
        # Count treatments without dates (regardless of tentativa values)
        tratamentos_df = timeline_data.get('tratamentos', pd.DataFrame())
        no_date_treatments = tratamentos_df[
            pd.isna(tratamentos_df['data_procedimento']) | 
            (tratamentos_df['data_procedimento'] == None)
        ]
        treatments_found = len(no_date_treatments)
        
        # Count '1° US Ciclo' entries (including all variations)
        extrato_df = timeline_data.get('extrato_atendimentos', pd.DataFrame())
        
        # Get all US cycle entries first
        us_cycle_patterns = [
            '1° US Ciclo',
            'US - Ciclo',
            '1º US de Ciclo',
            'US Ciclo',
            '1° US de Ciclo'
        ]
        
        all_us_cycle_entries = extrato_df[
            extrato_df['procedimento_nome'].str.contains('|'.join(us_cycle_patterns), case=False, na=False)
        ]
        
        # Filter to only first US cycle entries (1st, 1º, etc.)
        first_us_patterns = [
            '1° US Ciclo',
            '1º US de Ciclo', 
            '1° US de Ciclo',
            '1º US Ciclo'
        ]
        
        first_us_entries = all_us_cycle_entries[
            all_us_cycle_entries['procedimento_nome'].str.contains('|'.join(first_us_patterns), case=False, na=False)
        ]
        us_entries_found = len(first_us_entries)
        
        # With simplified logic, all treatments are now included in the timeline
        # Count treatments with estimated dates (flag_date_estimated = True)
        estimated_treatments = timeline_df[timeline_df['flag_date_estimated'] == True]
        insertions_made = len(estimated_treatments)
        
        # Get estimated tentativas and dates
        inserted_tentativas = []
        inserted_dates = []
        if not estimated_treatments.empty:
            for _, row in estimated_treatments.iterrows():
                inserted_tentativas.append(str(row['reference_value']))  # Convert to string
                inserted_dates.append(row['event_date'].strftime('%Y-%m-%d'))
        
        # Use timeline_df directly since all treatments are now included
        timeline_df_after_insertion = timeline_df
        
        # Compile test results
        test_results = {
            'prontuario': prontuario,
            'treatments_found': treatments_found,
            'us_entries_found': us_entries_found,
            'insertions_made': insertions_made,
            'timeline_events': len(timeline_df_after_insertion),
            'tables_represented': sorted(timeline_df_after_insertion['table_name'].unique().tolist()),
            'inserted_tentativas': inserted_tentativas,
            'inserted_dates': inserted_dates,
            'success': True,
            'errors': []
        }
        
        return test_results
        
    except Exception as e:
        return {
            'prontuario': prontuario,
            'success': False,
            'errors': [str(e)]
        }
    finally:
        conn.close()

def validate_test_results(test_results: Dict, expected: Dict) -> Dict:
    """
    Validate test results against expected results
    Returns a dictionary with validation results
    """
    validation = {
        'prontuario': test_results['prontuario'],
        'all_passed': True,
        'checks': []
    }
    
    if not test_results['success']:
        validation['all_passed'] = False
        validation['checks'].append({
            'check': 'Script execution',
            'passed': False,
            'expected': 'Script should run without errors',
            'actual': f"Error: {test_results['errors']}"
        })
        return validation
    
    # Check treatments found
    expected_treatments = expected['expected_treatments_found']
    actual_treatments = test_results['treatments_found']
    treatments_check = {
        'check': 'Treatments without dates found',
        'passed': actual_treatments == expected_treatments,
        'expected': expected_treatments,
        'actual': actual_treatments
    }
    validation['checks'].append(treatments_check)
    if not treatments_check['passed']:
        validation['all_passed'] = False
    
    # Check '1° US Ciclo' entries
    expected_us_entries = expected['expected_1us_entries']
    actual_us_entries = test_results['us_entries_found']
    us_entries_check = {
        'check': "'1° US Ciclo' entries found",
        'passed': actual_us_entries == expected_us_entries,
        'expected': expected_us_entries,
        'actual': actual_us_entries
    }
    validation['checks'].append(us_entries_check)
    if not us_entries_check['passed']:
        validation['all_passed'] = False
    
    # Check insertions made
    expected_insertions = expected['expected_insertions']
    actual_insertions = test_results['insertions_made']
    insertions_check = {
        'check': 'Treatments inserted',
        'passed': actual_insertions == expected_insertions,
        'expected': expected_insertions,
        'actual': actual_insertions
    }
    validation['checks'].append(insertions_check)
    if not insertions_check['passed']:
        validation['all_passed'] = False
    
    # Check timeline events
    expected_events = expected['expected_timeline_events']
    actual_events = test_results['timeline_events']
    events_check = {
        'check': 'Total timeline events',
        'passed': actual_events == expected_events,
        'expected': expected_events,
        'actual': actual_events
    }
    validation['checks'].append(events_check)
    if not events_check['passed']:
        validation['all_passed'] = False
    
    # Check tables represented
    expected_tables = expected['expected_tables']
    actual_tables = test_results['tables_represented']
    tables_check = {
        'check': 'Tables represented',
        'passed': actual_tables == expected_tables,
        'expected': expected_tables,
        'actual': actual_tables
    }
    validation['checks'].append(tables_check)
    if not tables_check['passed']:
        validation['all_passed'] = False
    
    # Check inserted tentativas (if applicable)
    if 'expected_inserted_tentativas' in expected:
        expected_tentativas = expected['expected_inserted_tentativas']
        actual_tentativas = test_results['inserted_tentativas']
        tentativas_check = {
            'check': 'Inserted tentativas',
            'passed': actual_tentativas == expected_tentativas,
            'expected': expected_tentativas,
            'actual': actual_tentativas
        }
        validation['checks'].append(tentativas_check)
        if not tentativas_check['passed']:
            validation['all_passed'] = False
    
    # Check inserted dates (if applicable)
    if 'expected_insertion_dates' in expected:
        expected_dates = expected['expected_insertion_dates']
        actual_dates = test_results['inserted_dates']
        dates_check = {
            'check': 'Insertion dates',
            'passed': actual_dates == expected_dates,
            'expected': expected_dates,
            'actual': actual_dates
        }
        validation['checks'].append(dates_check)
        if not dates_check['passed']:
            validation['all_passed'] = False
    
    return validation

def display_validation_results(validation: Dict):
    """Display validation results in a readable format"""
    print(f"\n{'='*80}")
    print(f"VALIDATION RESULTS FOR PATIENT: {validation['prontuario']}")
    print(f"{'='*80}")
    
    if validation['all_passed']:
        print("✅ ALL TESTS PASSED!")
    else:
        print("❌ SOME TESTS FAILED!")
    
    print(f"\nDetailed Results:")
    for check in validation['checks']:
        status = "✅" if check['passed'] else "❌"
        print(f"  {status} {check['check']}")
        print(f"     Expected: {check['expected']}")
        print(f"     Actual:   {check['actual']}")
        if not check['passed']:
            print(f"     ❌ MISMATCH!")
        print()

def run_all_tests():
    """Run tests for all patients and display results"""
    print(f"\n{'='*100}")
    print(f"PATIENT TIMELINE TEST SUITE")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*100}")
    
    all_validations = []
    total_patients = len(EXPECTED_RESULTS)
    passed_patients = 0
    
    for prontuario in EXPECTED_RESULTS.keys():
        # Run test
        test_results = test_patient_timeline(prontuario)
        
        # Validate results
        validation = validate_test_results(test_results, EXPECTED_RESULTS[prontuario])
        all_validations.append(validation)
        
        # Display results
        display_validation_results(validation)
        
        if validation['all_passed']:
            passed_patients += 1
    
    # Summary
    print(f"\n{'='*100}")
    print(f"TEST SUITE SUMMARY")
    print(f"{'='*100}")
    print(f"Total patients tested: {total_patients}")
    print(f"Patients passed: {passed_patients}")
    print(f"Patients failed: {total_patients - passed_patients}")
    print(f"Success rate: {(passed_patients/total_patients)*100:.1f}%")
    
    if passed_patients == total_patients:
        print(f"\n🎉 ALL TESTS PASSED! The timeline logic is working correctly.")
    else:
        print(f"\n⚠️  Some tests failed. Please review the results above.")
    
    return all_validations

if __name__ == "__main__":
    run_all_tests()
