"""
Query Interface for Embryoscope Data
Provides easy access to embryoscope data stored in DuckDB.
"""

import duckdb
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging
import sys
import os

# Add the parent directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_manager import EmbryoscopeConfigManager
from utils.database_manager import EmbryoscopeDatabaseManager


class EmbryoscopeQueryInterface:
    """Interface for querying embryoscope data from the database."""
    
    def __init__(self, config_path: str = "../params.yml"):
        """
        Initialize the query interface.
        
        Args:
            config_path: Path to configuration file
        """
        self.config_manager = EmbryoscopeConfigManager(config_path)
        self.db_manager = EmbryoscopeDatabaseManager(
            self.config_manager.get_database_path(),
            self.config_manager.get_database_schema()
        )
        self.logger = logging.getLogger("embryoscope_query")
    
    def get_latest_patients(self, location: str = None) -> pd.DataFrame:
        """
        Get the latest patients data.
        
        Args:
            location: Optional location filter
            
        Returns:
            Dataframe with patients data
        """
        if location:
            return self.db_manager.get_latest_data('patients', location)
        else:
            # Get data from all locations
            all_data = []
            for loc in self.config_manager.get_enabled_embryoscopes().keys():
                df = self.db_manager.get_latest_data('patients', loc)
                if not df.empty:
                    all_data.append(df)
            
            if all_data:
                return pd.concat(all_data, ignore_index=True)
            else:
                return pd.DataFrame()
    
    def get_latest_treatments(self, location: str = None) -> pd.DataFrame:
        """
        Get the latest treatments data.
        
        Args:
            location: Optional location filter
            
        Returns:
            Dataframe with treatments data
        """
        if location:
            return self.db_manager.get_latest_data('treatments', location)
        else:
            # Get data from all locations
            all_data = []
            for loc in self.config_manager.get_enabled_embryoscopes().keys():
                df = self.db_manager.get_latest_data('treatments', loc)
                if not df.empty:
                    all_data.append(df)
            
            if all_data:
                return pd.concat(all_data, ignore_index=True)
            else:
                return pd.DataFrame()
    
    def get_latest_embryo_data(self, location: str = None) -> pd.DataFrame:
        """
        Get the latest embryo data.
        
        Args:
            location: Optional location filter
            
        Returns:
            Dataframe with embryo data
        """
        if location:
            return self.db_manager.get_latest_data('embryo_data', location)
        else:
            # Get data from all locations
            all_data = []
            for loc in self.config_manager.get_enabled_embryoscopes().keys():
                df = self.db_manager.get_latest_data('embryo_data', loc)
                if not df.empty:
                    all_data.append(df)
            
            if all_data:
                return pd.concat(all_data, ignore_index=True)
            else:
                return pd.DataFrame()
    
    def get_latest_idascore(self, location: str = None) -> pd.DataFrame:
        """
        Get the latest IDA score data.
        
        Args:
            location: Optional location filter
            
        Returns:
            Dataframe with IDA score data
        """
        if location:
            return self.db_manager.get_latest_data('idascore', location)
        else:
            # Get data from all locations
            all_data = []
            for loc in self.config_manager.get_enabled_embryoscopes().keys():
                df = self.db_manager.get_latest_data('idascore', loc)
                if not df.empty:
                    all_data.append(df)
            
            if all_data:
                return pd.concat(all_data, ignore_index=True)
            else:
                return pd.DataFrame()
    
    def get_complete_embryo_data(self, location: str = None) -> pd.DataFrame:
        """
        Get complete embryo data with all related information.
        
        Args:
            location: Optional location filter
            
        Returns:
            Dataframe with complete embryo data
        """
        # Get base data
        patients_df = self.get_latest_patients(location)
        treatments_df = self.get_latest_treatments(location)
        embryo_df = self.get_latest_embryo_data(location)
        idascore_df = self.get_latest_idascore(location)
        
        if embryo_df.empty:
            return pd.DataFrame()
        
        # Merge data
        # First merge treatments with patients
        if not treatments_df.empty and not patients_df.empty:
            merged_df = pd.merge(
                treatments_df, 
                patients_df[['PatientIDx', 'Name', 'BirthDate']], 
                on='PatientIDx', 
                how='left'
            )
        else:
            merged_df = treatments_df
        
        # Then merge with embryo data
        if not merged_df.empty:
            complete_df = pd.merge(
                embryo_df,
                merged_df[['PatientIDx', 'TreatmentName', 'Name', 'BirthDate']],
                on=['PatientIDx', 'TreatmentName'],
                how='left'
            )
        else:
            complete_df = embryo_df
        
        # Finally merge with IDA scores
        if not idascore_df.empty:
            complete_df = pd.merge(
                complete_df,
                idascore_df[['EmbryoID', 'Score', 'Viability']],
                on='EmbryoID',
                how='left'
            )
        
        return complete_df
    
    def get_embryos_by_criteria(self, location: str = None, 
                               embryo_fate: str = None,
                               min_score: float = None,
                               max_score: float = None,
                               viability: str = None) -> pd.DataFrame:
        """
        Get embryos filtered by specific criteria.
        
        Args:
            location: Optional location filter
            embryo_fate: Filter by embryo fate
            min_score: Minimum IDA score
            max_score: Maximum IDA score
            viability: Filter by viability
            
        Returns:
            Filtered dataframe
        """
        df = self.get_complete_embryo_data(location)
        
        if df.empty:
            return df
        
        # Apply filters
        if embryo_fate:
            df = df[df['EmbryoFate'] == embryo_fate]
        
        if min_score is not None:
            df = df[df['Score'] >= min_score]
        
        if max_score is not None:
            df = df[df['Score'] <= max_score]
        
        if viability:
            df = df[df['Viability'] == viability]
        
        return df
    
    def get_patient_summary(self, location: str = None) -> Dict[str, Any]:
        """
        Get summary statistics for patients.
        
        Args:
            location: Optional location filter
            
        Returns:
            Dictionary with patient statistics
        """
        patients_df = self.get_latest_patients(location)
        
        if patients_df.empty:
            return {
                'total_patients': 0,
                'locations': [],
                'latest_extraction': None
            }
        
        summary = {
            'total_patients': len(patients_df),
            'locations': patients_df['_location'].unique().tolist(),
            'latest_extraction': patients_df['_extraction_timestamp'].max()
        }
        
        return summary
    
    def get_embryo_summary(self, location: str = None) -> Dict[str, Any]:
        """
        Get summary statistics for embryos.
        
        Args:
            location: Optional location filter
            
        Returns:
            Dictionary with embryo statistics
        """
        embryo_df = self.get_latest_embryo_data(location)
        
        if embryo_df.empty:
            return {
                'total_embryos': 0,
                'embryo_fates': {},
                'locations': [],
                'latest_extraction': None
            }
        
        summary = {
            'total_embryos': len(embryo_df),
            'embryo_fates': embryo_df['EmbryoFate'].value_counts().to_dict(),
            'locations': embryo_df['_location'].unique().tolist(),
            'latest_extraction': embryo_df['_extraction_timestamp'].max()
        }
        
        return summary
    
    def get_treatment_summary(self, location: str = None) -> Dict[str, Any]:
        """
        Get summary statistics for treatments.
        
        Args:
            location: Optional location filter
            
        Returns:
            Dictionary with treatment statistics
        """
        treatments_df = self.get_latest_treatments(location)
        
        if treatments_df.empty:
            return {
                'total_treatments': 0,
                'unique_patients': 0,
                'locations': [],
                'latest_extraction': None
            }
        
        summary = {
            'total_treatments': len(treatments_df),
            'unique_patients': treatments_df['PatientIDx'].nunique(),
            'locations': treatments_df['_location'].unique().tolist(),
            'latest_extraction': treatments_df['_extraction_timestamp'].max()
        }
        
        return summary
    
    def get_idascore_summary(self, location: str = None) -> Dict[str, Any]:
        """
        Get summary statistics for IDA scores.
        
        Args:
            location: Optional location filter
            
        Returns:
            Dictionary with IDA score statistics
        """
        idascore_df = self.get_latest_idascore(location)
        
        if idascore_df.empty:
            return {
                'total_scores': 0,
                'score_stats': {},
                'viability_distribution': {},
                'locations': [],
                'latest_extraction': None
            }
        
        summary = {
            'total_scores': len(idascore_df),
            'score_stats': {
                'mean': idascore_df['Score'].mean(),
                'median': idascore_df['Score'].median(),
                'min': idascore_df['Score'].min(),
                'max': idascore_df['Score'].max(),
                'std': idascore_df['Score'].std()
            },
            'viability_distribution': idascore_df['Viability'].value_counts().to_dict(),
            'locations': idascore_df['_location'].unique().tolist(),
            'latest_extraction': idascore_df['_extraction_timestamp'].max()
        }
        
        return summary
    
    def get_comprehensive_summary(self, location: str = None) -> Dict[str, Any]:
        """
        Get comprehensive summary of all data.
        
        Args:
            location: Optional location filter
            
        Returns:
            Dictionary with comprehensive summary
        """
        summary = {
            'database_info': {
                'path': self.config_manager.get_database_path(),
                'schema': self.config_manager.get_database_schema()
            },
            'patients': self.get_patient_summary(location),
            'treatments': self.get_treatment_summary(location),
            'embryos': self.get_embryo_summary(location),
            'idascore': self.get_idascore_summary(location),
            'extraction_history': self.db_manager.get_extraction_history(location, limit=5)
        }
        
        return summary
    
    def export_data_to_csv(self, output_dir: str = "data_output", location: str = None):
        """
        Export all data to CSV files.
        
        Args:
            output_dir: Output directory for CSV files
            location: Optional location filter
        """
        import os
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Export each data type
        data_types = {
            'patients': self.get_latest_patients(location),
            'treatments': self.get_latest_treatments(location),
            'embryo_data': self.get_latest_embryo_data(location),
            'idascore': self.get_latest_idascore(location),
            'complete_embryo_data': self.get_complete_embryo_data(location)
        }
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for data_type, df in data_types.items():
            if not df.empty:
                filename = f"{output_dir}/{data_type}_{timestamp}.csv"
                df.to_csv(filename, index=False)
                print(f"Exported {len(df)} rows to {filename}")
            else:
                print(f"No data to export for {data_type}")


def main():
    """Main function to demonstrate the query interface."""
    try:
        # Initialize query interface
        query_interface = EmbryoscopeQueryInterface()
        
        # Print comprehensive summary
        print("=== Embryoscope Data Summary ===")
        summary = query_interface.get_comprehensive_summary()
        
        print(f"\nDatabase: {summary['database_info']['path']}")
        print(f"Schema: {summary['database_info']['schema']}")
        
        print(f"\nPatients: {summary['patients']['total_patients']}")
        print(f"Treatments: {summary['treatments']['total_treatments']}")
        print(f"Embryos: {summary['embryos']['total_embryos']}")
        print(f"IDA Scores: {summary['idascore']['total_scores']}")
        
        # Export data
        print("\nExporting data to CSV...")
        query_interface.export_data_to_csv()
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main() 