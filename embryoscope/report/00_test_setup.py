"""
Test script to verify the setup and test with a small sample.
This script tests the image availability check on a small subset of embryos.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
import pandas as pd
from datetime import datetime
import logging

from utils.api_client import EmbryoscopeAPIClient
from utils.config_manager import EmbryoscopeConfigManager


# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_connection():
    """Test connection to all enabled servers."""
    logger.info("Testing connections to all enabled servers...")
    
    config_manager = EmbryoscopeConfigManager()
    enabled_servers = config_manager.get_enabled_embryoscopes()
    
    for server_name, server_config in enabled_servers.items():
        logger.info(f"\nTesting {server_name}...")
        
        client = EmbryoscopeAPIClient(server_name, server_config, rate_limit_delay=0.1)
        
        if client.authenticate():
            logger.info(f"✓ Authentication successful for {server_name}")
            
            # Test getting patients
            patients = client.get_patients()
            if patients and 'Patients' in patients:
                logger.info(f"✓ Successfully retrieved {len(patients['Patients'])} patients")
            else:
                logger.warning(f"✗ Failed to retrieve patients")
        else:
            logger.error(f"✗ Authentication failed for {server_name}")


def test_sample_embryos(limit=5):
    """Test image availability check on a small sample."""
    logger.info(f"\nTesting image availability check on {limit} embryos per server...")
    
    db_path = 'database/huntington_data_lake.duckdb'
    config_manager = EmbryoscopeConfigManager()
    
    conn = duckdb.connect(db_path)
    
    # Get sample embryos from each server
    query = f"""
    WITH ranked AS (
        SELECT 
            prontuario,
            patient_PatientID,
            patient_PatientIDx,
            patient_unit_huntington,
            treatment_TreatmentName,
            embryo_EmbryoID,
            embryo_EmbryoDate,
            ROW_NUMBER() OVER (PARTITION BY patient_unit_huntington ORDER BY embryo_EmbryoDate DESC) as rn
        FROM gold.embryoscope_embrioes
        WHERE embryo_EmbryoID IS NOT NULL
    )
    SELECT * FROM ranked WHERE rn <= {limit}
    ORDER BY patient_unit_huntington, rn
    """
    
    df = conn.execute(query).df()
    conn.close()
    
    logger.info(f"Retrieved {len(df)} sample embryos")
    print("\nSample embryos:")
    print(df[['patient_unit_huntington', 'embryo_EmbryoID', 'embryo_EmbryoDate']])
    
    # Test each server
    for server_name in df['patient_unit_huntington'].unique():
        logger.info(f"\n{'='*60}")
        logger.info(f"Testing server: {server_name}")
        logger.info(f"{'='*60}")
        
        server_config = config_manager.get_embryoscope_config(server_name)
        if not server_config or not server_config.get('enabled', False):
            logger.warning(f"Server {server_name} is not enabled, skipping...")
            continue
        
        client = EmbryoscopeAPIClient(server_name, server_config, rate_limit_delay=0.1)
        
        if not client.authenticate():
            logger.error(f"Failed to authenticate with {server_name}")
            continue
        
        server_embryos = df[df['patient_unit_huntington'] == server_name]
        
        for idx, row in server_embryos.iterrows():
            embryo_id = row['embryo_EmbryoID']
            logger.info(f"\nChecking embryo: {embryo_id}")
            
            try:
                response = client.get_image_runs(embryo_id)
                
                if response:
                    if 'ImageRuns' in response:
                        image_runs = response['ImageRuns']
                        count = len(image_runs) if isinstance(image_runs, list) else 0
                        logger.info(f"✓ Response received: {count} image runs")
                        if count > 0:
                            logger.info(f"  First run: {image_runs[0] if isinstance(image_runs, list) else 'N/A'}")
                    else:
                        logger.warning(f"✗ No 'ImageRuns' field in response")
                        logger.debug(f"  Response keys: {response.keys()}")
                else:
                    logger.warning(f"✗ No response received")
                    
            except Exception as e:
                logger.error(f"✗ Error: {e}")


def main():
    """Main test function."""
    print("="*60)
    print("EMBRYO IMAGE AVAILABILITY - TEST SCRIPT")
    print("="*60)
    
    # Test 1: Connection test
    test_connection()
    
    # Test 2: Sample embryos test
    test_sample_embryos(limit=3)
    
    print("\n" + "="*60)
    print("TEST COMPLETE")
    print("="*60)


if __name__ == "__main__":
    main()
