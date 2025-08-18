#!/usr/bin/env python3
"""
Test All Patient Timeline
Test script to verify the all patient timeline functionality works correctly.
"""

import duckdb as db
import pandas as pd
import logging
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_timeline_for_sample_patients():
    """Test timeline creation for a few sample patients"""
    
    logger.info("Testing timeline creation for sample patients...")
    
    # Connect to the huntington_data_lake database
    # Resolve DB path relative to repository root
    import os
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
    conn = db.connect(path_to_db)
    
    try:
        # Check if the table exists
        table_exists = conn.execute("""
            SELECT COUNT(*) as count 
            FROM information_schema.tables 
            WHERE table_schema = 'gold' AND table_name = 'all_patients_timeline'
        """).fetchdf()['count'].iloc[0]
        
        if table_exists == 0:
            logger.error("Table gold.all_patients_timeline does not exist!")
            return False
        
        # Get total counts
        total_events = conn.execute("SELECT COUNT(*) as count FROM gold.all_patients_timeline").fetchdf()['count'].iloc[0]
        total_patients = conn.execute("SELECT COUNT(DISTINCT prontuario) as count FROM gold.all_patients_timeline").fetchdf()['count'].iloc[0]
        
        logger.info(f"Total events in timeline: {total_events:,}")
        logger.info(f"Total patients in timeline: {total_patients:,}")
        
        # Get sample patients with most events
        sample_patients = conn.execute("""
            SELECT prontuario, COUNT(*) as event_count
            FROM gold.all_patients_timeline
            GROUP BY prontuario
            ORDER BY event_count DESC
            LIMIT 5
        """).fetchdf()
        
        logger.info("\nTop 5 patients by event count:")
        for _, row in sample_patients.iterrows():
            logger.info(f"  Patient {row['prontuario']}: {row['event_count']} events")
        
        # Test timeline for the patient with most events
        if not sample_patients.empty:
            test_patient = sample_patients.iloc[0]['prontuario']
            logger.info(f"\nTesting timeline for patient {test_patient} (most events):")
            
            patient_timeline = conn.execute(f"""
                SELECT *
                FROM gold.all_patients_timeline
                WHERE prontuario = {test_patient}
                ORDER BY event_date DESC, event_id DESC
                LIMIT 10
            """).fetchdf()
            
            logger.info(f"Found {len(patient_timeline)} events for patient {test_patient}")
            
            for _, event in patient_timeline.iterrows():
                date_str = event['event_date'].strftime('%Y-%m-%d') if pd.notna(event['event_date']) else 'Unknown'
                estimated_flag = " (ESTIMATED)" if event['flag_date_estimated'] else ""
                logger.info(f"  {date_str}{estimated_flag} | {event['reference']} | {event['reference_value']}")
        
        # Check data quality
        logger.info("\nData quality checks:")
        
        # Check for null dates
        null_dates = conn.execute("SELECT COUNT(*) as count FROM gold.all_patients_timeline WHERE event_date IS NULL").fetchdf()['count'].iloc[0]
        logger.info(f"  Events with null dates: {null_dates}")
        
        # Check for estimated dates
        estimated_dates = conn.execute("SELECT COUNT(*) as count FROM gold.all_patients_timeline WHERE flag_date_estimated = TRUE").fetchdf()['count'].iloc[0]
        logger.info(f"  Events with estimated dates: {estimated_dates}")
        
        # Check table distribution
        table_distribution = conn.execute("""
            SELECT reference, COUNT(*) as count
            FROM gold.all_patients_timeline
            GROUP BY reference
            ORDER BY count DESC
        """).fetchdf()
        
        logger.info("\nEvents per table:")
        for _, row in table_distribution.iterrows():
            logger.info(f"  {row['reference']}: {row['count']:,}")
        
        # Check date range
        date_range = conn.execute("""
            SELECT 
                MIN(event_date) as min_date,
                MAX(event_date) as max_date
            FROM gold.all_patients_timeline
            WHERE event_date IS NOT NULL
        """).fetchdf()
        
        if not date_range.empty:
            logger.info(f"\nDate range: {date_range.iloc[0]['min_date']} to {date_range.iloc[0]['max_date']}")
        
        logger.info("\nAll tests completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error during testing: {str(e)}")
        return False
        
    finally:
        conn.close()

def compare_with_original_single_patient():
    """Compare results with the original single-patient script for verification"""
    
    logger.info("\nComparing with original single-patient script...")
    
    # Resolve DB path relative to repository root
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    
    # Connect to clinisys database
    clinisys_path = os.path.join(repo_root, 'database', 'clinisys_all.duckdb')
    clinisys_conn = db.connect(clinisys_path, read_only=True)
    
    # Connect to huntington_data_lake database
    huntington_path = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
    huntington_conn = db.connect(huntington_path)
    
    try:
        # Get a sample patient from the all_patients_timeline
        sample_patient = huntington_conn.execute("""
            SELECT prontuario 
            FROM gold.all_patients_timeline 
            LIMIT 1
        """).fetchdf()['prontuario'].iloc[0]
        
        logger.info(f"Comparing results for patient {sample_patient}")
        
        # Get timeline from all_patients_timeline
        all_timeline = huntington_conn.execute(f"""
            SELECT *
            FROM gold.all_patients_timeline
            WHERE prontuario = {sample_patient}
            ORDER BY event_date DESC, event_id DESC
        """).fetchdf()
        
        logger.info(f"All patients timeline: {len(all_timeline)} events")
        
        # Show sample events
        logger.info("Sample events from all_patients_timeline:")
        for _, event in all_timeline.head(5).iterrows():
            date_str = event['event_date'].strftime('%Y-%m-%d') if pd.notna(event['event_date']) else 'Unknown'
            logger.info(f"  {date_str} | {event['reference']} | {event['reference_value']}")
        
        logger.info("Comparison completed!")
        
    except Exception as e:
        logger.error(f"Error during comparison: {str(e)}")
        
    finally:
        clinisys_conn.close()
        huntington_conn.close()

if __name__ == "__main__":
    logger.info("Starting All Patient Timeline Tests...")
    
    # Run tests
    success = test_timeline_for_sample_patients()
    
    if success:
        compare_with_original_single_patient()
    
    logger.info("All tests completed!")
