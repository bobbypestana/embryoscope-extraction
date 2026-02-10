"""
Configuration Manager for Embryoscope Data Extraction
Handles loading and managing configuration parameters for the embryoscope extraction system.
"""

import yaml
import os
from typing import Dict, Any, Optional
from pathlib import Path


class EmbryoscopeConfigManager:
    """Manages configuration for embryoscope data extraction."""
    
    def __init__(self, config_path: str = "embryoscope/params.yml"):
        """
        Initialize the configuration manager.
        
        Args:
            config_path: Path to the configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            print(f"[DEBUG] Trying to load config from: {os.path.abspath(self.config_path)}")
            with open(self.config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
            return config
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing configuration file: {e}")
    
    def get_embryoscope_credentials(self) -> Dict[str, Dict[str, Any]]:
        """Get embryoscope credentials for all locations."""
        return self.config.get('embryoscope_credentials', {})
    
    def get_enabled_embryoscopes(self) -> Dict[str, Dict[str, Any]]:
        """Get only enabled embryoscope credentials."""
        credentials = self.get_embryoscope_credentials()
        return {name: config for name, config in credentials.items() 
                if config.get('enabled', False)}
    
    def get_embryoscope_config(self, location: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific embryoscope location."""
        credentials = self.get_embryoscope_credentials()
        return credentials.get(location)
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration."""
        return self.config.get('database', {})
    
    def get_database_path(self) -> str:
        """Get database file path."""
        db_config = self.get_database_config()
        return db_config.get('path', '../../database/embryoscope_vila_mariana.db')
    
    def get_database_schema(self) -> str:
        """Get database schema name."""
        db_config = self.get_database_config()
        return db_config.get('schema', 'embryoscope')
    
    def get_extraction_config(self, extraction_type: Optional[str] = None) -> Dict[str, Any]:
        """Get extraction configuration. If extraction_type specified, returns data_extraction or image_extraction config."""
        if extraction_type:
            specific_config = self.config.get(extraction_type, {})
            if specific_config:
                return specific_config
        return self.config.get('extraction', {})
    
    def get_rate_limit_delay(self, endpoint: Optional[str] = None, extraction_type: str = 'data_extraction') -> float:
        """Get rate limit delay between requests."""
        extraction_config = self.get_extraction_config(extraction_type)
        if endpoint and endpoint in extraction_config:
            return extraction_config[endpoint].get('rate_limit_delay', extraction_config.get('rate_limit_delay', 0.1))
        return extraction_config.get('rate_limit_delay', 0.1)
    
    def get_max_retries(self, extraction_type: str = 'data_extraction') -> int:
        """Get maximum number of retries for failed requests."""
        extraction_config = self.get_extraction_config(extraction_type)
        return extraction_config.get('max_retries', 3)
    
    def get_timeout(self, extraction_type: str = 'data_extraction') -> int:
        """Get request timeout in seconds."""
        extraction_config = self.get_extraction_config(extraction_type)
        return extraction_config.get('timeout', 30)
    
    def get_batch_size(self, extraction_type: str = 'data_extraction') -> int:
        """Get batch size for processing."""
        extraction_config = self.get_extraction_config(extraction_type)
        return extraction_config.get('batch_size', 1000)
    
    def is_parallel_processing_enabled(self, extraction_type: str = 'data_extraction') -> bool:
        """Check if parallel processing is enabled."""
        extraction_config = self.get_extraction_config(extraction_type)
        return extraction_config.get('parallel_processing', True)
    
    def get_max_workers(self, endpoint: Optional[str] = None, extraction_type: str = 'data_extraction') -> int:
        """Get maximum number of parallel workers."""
        extraction_config = self.get_extraction_config(extraction_type)
        if endpoint and endpoint in extraction_config:
            return extraction_config[endpoint].get('max_workers', extraction_config.get('max_workers', 3))
        return extraction_config.get('max_workers', 3)
    
    def get_clinic_parallel_workers(self, extraction_type: str = 'data_extraction') -> int:
        """Get number of parallel workers for internal clinic operations."""
        extraction_config = self.get_extraction_config(extraction_type)
        return extraction_config.get('clinic_parallel_workers', 1)
    
    def get_token_refresh_patients(self) -> int:
        """Get token refresh frequency for patients."""
        extraction_config = self.get_extraction_config()
        return extraction_config.get('token_refresh_patients', 50)
    
    def get_token_refresh_treatments(self) -> int:
        """Get token refresh frequency for treatments."""
        extraction_config = self.get_extraction_config()
        return extraction_config.get('token_refresh_treatments', 100)
    
    def validate_config(self) -> bool:
        """Validate the configuration."""
        try:
            # Check required sections
            required_sections = ['embryoscope_credentials', 'database', 'extraction']
            for section in required_sections:
                if section not in self.config:
                    raise ValueError(f"Missing required configuration section: {section}")
            
            # Check database path
            db_path = self.get_database_path()
            db_dir = os.path.dirname(db_path)
            if not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
            
            # Check at least one embryoscope is enabled
            enabled_embryoscopes = self.get_enabled_embryoscopes()
            if not enabled_embryoscopes:
                raise ValueError("No enabled embryoscopes found in configuration")
            
            return True
            
        except Exception as e:
            print(f"Configuration validation failed: {e}")
            return False
    
    def print_config_summary(self):
        """Print a summary of the current configuration."""
        print("=== Embryoscope Configuration Summary ===")
        
        # Database info
        db_config = self.get_database_config()
        print(f"Database: {db_config.get('path')} (schema: {db_config.get('schema')})")
        
        # Embryoscope info
        credentials = self.get_embryoscope_credentials()
        enabled_count = len(self.get_enabled_embryoscopes())
        total_count = len(credentials)
        print(f"Embryoscopes: {enabled_count}/{total_count} enabled")
        
        for name, config in credentials.items():
            status = "ENABLED" if config.get('enabled', False) else "DISABLED"
            print(f"  - {name}: {config.get('ip')}:{config.get('port')} ({status})")
        
        # Extraction settings
        extraction_config = self.get_extraction_config()
        print(f"Rate limit delay: {extraction_config.get('rate_limit_delay')}s")
        print(f"Max retries: {extraction_config.get('max_retries')}")
        print(f"Timeout: {extraction_config.get('timeout')}s")
        print(f"Batch size: {extraction_config.get('batch_size')}")
        print(f"Parallel processing: {extraction_config.get('parallel_processing')}")
        print(f"Max workers: {extraction_config.get('max_workers')}")
        print("=" * 40)


if __name__ == "__main__":
    # Test the configuration manager
    config_manager = EmbryoscopeConfigManager()
    config_manager.print_config_summary()
    
    if config_manager.validate_config():
        print("Configuration is valid!")
    else:
        print("Configuration validation failed!") 