"""
API Client for Embryoscope Data Extraction
Handles authentication, rate limiting, and API requests to embryoscope endpoints.
"""

import requests
import urllib3
import time
import hashlib
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class EmbryoscopeAPIClient:
    """Client for interacting with Embryoscope API."""
    
    def __init__(self, location: str, config: Dict[str, Any], rate_limit_delay: float = 0.1):
        """
        Initialize the API client.
        
        Args:
            location: Embryoscope location name
            config: Configuration dictionary with IP, login, password, port
            rate_limit_delay: Delay between requests in seconds
        """
        self.location = location
        self.config = config
        self.rate_limit_delay = rate_limit_delay
        self.base_url = f"https://{config['ip']}:{config['port']}"
        self.token = None
        self.session = requests.Session()
        self.session.verify = False  # Disable SSL verification
        
        # Setup logging
        self.logger = logging.getLogger(f"embryoscope_api_{location}")
        
    def authenticate(self) -> bool:
        """
        Authenticate with the embryoscope API and get token.
        
        Returns:
            True if authentication successful, False otherwise
        """
        try:
            url = f"{self.base_url}/LOGIN"
            params = {
                'username': self.config['login'],
                'password': self.config['password']
            }
            
            self.logger.info(f"Authenticating with {self.location} at {url}")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if 'Token' in data:
                self.token = data['Token']
                self.logger.info(f"Authentication successful for {self.location}")
                return True
            else:
                self.logger.error(f"No token found in response for {self.location}")
                return False
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Authentication failed for {self.location}: {e}")
            return False
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON response from {self.location}: {e}")
            return False
    
    def _rate_limited_request(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        """
        Make a rate-limited request to the API with retry logic.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            **kwargs: Additional request parameters
            
        Returns:
            Response object or None if request failed
        """
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            time.sleep(self.rate_limit_delay)
            
            try:
                response = self.session.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except Exception as e:
                if attempt < max_retries - 1:
                    self.logger.warning(f"Request failed for {self.location} - {url} (attempt {attempt + 1}/{max_retries}): {e}")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    self.logger.error(f"Request failed for {self.location} - {url} after {max_retries} attempts: {e}")
                    return None
        
        return None
    
    def _make_authenticated_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Make an authenticated request to the API.
        
        Args:
            endpoint: API endpoint (without base URL)
            params: Query parameters
            
        Returns:
            JSON response or None if request failed
        """
        # Try up to 2 times (original + 1 retry with re-authentication)
        for attempt in range(2):
            if not self.token:
                if not self.authenticate():
                    return None
            
            url = f"{self.base_url}/{endpoint}"
            headers = {'API-token': self.token}
            
            response = self._rate_limited_request('GET', url, headers=headers, params=params)
            if response is None:
                return None
            
            # Check if we got a 401 error (token expired)
            if response.status_code == 401:
                self.logger.debug(f"Token expired for {self.location}, re-authenticating...")
                self.token = None  # Clear the expired token
                if attempt == 0:  # Only retry once
                    continue
                else:
                    self.logger.error(f"Failed to re-authenticate for {self.location}")
                    return None
            
            try:
                # Check if response is empty
                if not response.text.strip():
                    self.logger.debug(f"Empty response from {self.location} - {endpoint}")
                    return None
                
                return response.json()
            except json.JSONDecodeError as e:
                self.logger.error(f"Invalid JSON response from {self.location} - {endpoint}: {e}")
                self.logger.error(f"Response content: {response.text[:200]}...")  # Log first 200 chars
                return None
        
        return None
    
    def get_patients(self) -> Optional[Dict]:
        """Get all patients from the embryoscope."""
        return self._make_authenticated_request("GET/patients")
    
    def get_treatments(self, patient_idx: str) -> Optional[Dict]:
        """
        Get treatments for a specific patient.
        
        Args:
            patient_idx: Patient identifier
        """
        params = {'patientIDx': patient_idx}
        return self._make_authenticated_request("GET/TREATMENT", params)
    
    def get_embryo_data(self, patient_idx: str, treatment_name: str) -> Optional[Dict]:
        """
        Get embryo data for a specific patient and treatment.
        
        Args:
            patient_idx: Patient identifier
            treatment_name: Treatment name
        """
        params = {
            'PatientIDx': patient_idx,
            'TreatmentName': treatment_name
        }
        return self._make_authenticated_request("GET/embryodata", params)
    
    def get_idascore(self) -> Optional[Dict]:
        """Get IDA score data for all embryos."""
        return self._make_authenticated_request("GET/IDASCORE")
    
    def get_embryo_id(self, patient_idx: str, treatment_name: str) -> Optional[Dict]:
        """
        Get embryo IDs for a specific patient and treatment.
        
        Args:
            patient_idx: Patient identifier
            treatment_name: Treatment name
        """
        params = {
            'PatientIDx': patient_idx,
            'TreatmentName': treatment_name
        }
        return self._make_authenticated_request("GET/embryoID", params)
    
    def get_fertilization_time(self, embryo_id: str) -> Optional[Dict]:
        """
        Get fertilization time for a specific embryo.
        
        Args:
            embryo_id: Embryo identifier
        """
        params = {'EmbryoID': embryo_id}
        return self._make_authenticated_request("GET/fertilizationtime", params)
    
    def get_image_runs(self, embryo_id: str) -> Optional[Dict]:
        """
        Get image runs for a specific embryo.
        
        Args:
            embryo_id: Embryo identifier
        """
        params = {'EmbryoID': embryo_id}
        return self._make_authenticated_request("GET/imageruns", params)
    
    def get_evaluation(self, embryo_id: str) -> Optional[Dict]:
        """
        Get evaluation data for a specific embryo.
        
        Args:
            embryo_id: Embryo identifier
        """
        params = {'EmbryoID': embryo_id}
        return self._make_authenticated_request("GET/evaluation", params)
    
    def get_embryo_fate(self, embryo_id: str) -> Optional[Dict]:
        """
        Get embryo fate data for a specific embryo.
        
        Args:
            embryo_id: Embryo identifier
        """
        params = {'EmbryoID': embryo_id}
        return self._make_authenticated_request("GET/embryofate", params)
    
    def get_embryo_details(self, embryo_id: str) -> Optional[Dict]:
        """
        Get detailed embryo information for a specific embryo.
        
        Args:
            embryo_id: Embryo identifier
        """
        params = {'EmbryoID': embryo_id}
        return self._make_authenticated_request("GET/embryodetails", params)
    
    def get_transfers(self, patient_idx: str) -> Optional[Dict]:
        """
        Get transfer data for a specific patient.
        
        Args:
            patient_idx: Patient identifier
        """
        params = {'PatientIDx': patient_idx}
        return self._make_authenticated_request("GET/transfers", params)
    
    def test_connection(self) -> bool:
        """
        Test the connection to the embryoscope API.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            patients = self.get_patients()
            if patients and 'Patients' in patients:
                self.logger.info(f"Connection test successful for {self.location}")
                return True
            else:
                self.logger.error(f"Connection test failed for {self.location}: Invalid response")
                return False
        except Exception as e:
            self.logger.error(f"Connection test failed for {self.location}: {e}")
            return False
    
    def check_token_validity(self) -> bool:
        """
        Check if the current token is still valid.
        
        Returns:
            True if token is valid, False otherwise
        """
        if not self.token:
            return False
        
        try:
            # Try a simple request to test token validity
            url = f"{self.base_url}/GET/patients"
            headers = {'API-token': self.token}
            
            response = self.session.get(url, headers=headers, verify=False, timeout=10)
            
            if response.status_code == 401:
                self.logger.info(f"Token expired for {self.location}")
                self.token = None
                return False
            elif response.status_code == 200:
                return True
            else:
                self.logger.warning(f"Unexpected status code {response.status_code} for {self.location}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error checking token validity for {self.location}: {e}")
            return False
    
    def refresh_token_if_needed(self) -> bool:
        """
        Refresh the token if it's expired or about to expire.
        
        Returns:
            True if token is valid (or was refreshed), False otherwise
        """
        if not self.check_token_validity():
            self.logger.info(f"Refreshing token for {self.location}")
            return self.authenticate()
        return True
    
    def get_data_summary(self) -> Dict[str, Any]:
        """
        Get a summary of available data from the embryoscope.
        
        Returns:
            Dictionary with data summary
        """
        summary = {
            'location': self.location,
            'base_url': self.base_url,
            'patients_count': 0,
            'treatments_count': 0,
            'embryos_count': 0,
            'idascore_count': 0,
            'connection_status': 'unknown'
        }
        
        try:
            # Test connection
            if self.test_connection():
                summary['connection_status'] = 'connected'
                
                # Get patients count
                patients = self.get_patients()
                if patients and 'Patients' in patients:
                    summary['patients_count'] = len(patients['Patients'])
                
                # Get IDA score count
                idascore = self.get_idascore()
                if idascore and 'Scores' in idascore:
                    summary['idascore_count'] = len(idascore['Scores'])
                
            else:
                summary['connection_status'] = 'failed'
                
        except Exception as e:
            self.logger.error(f"Error getting data summary for {self.location}: {e}")
            summary['connection_status'] = 'error'
        
        return summary


if __name__ == "__main__":
    # Test the API client
    from config_manager import EmbryoscopeConfigManager
    
    config_manager = EmbryoscopeConfigManager()
    enabled_embryoscopes = config_manager.get_enabled_embryoscopes()
    
    for location, config in enabled_embryoscopes.items():
        print(f"\nTesting connection to {location}...")
        client = EmbryoscopeAPIClient(location, config)
        summary = client.get_data_summary()
        print(f"Summary: {summary}") 