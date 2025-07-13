#!/usr/bin/env python3
"""
Simple test script to verify the API retry logic works.
This tests the retry mechanism without making actual API calls.
"""

import os
import sys
import time
import logging
from unittest.mock import Mock, patch

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api_client import EmbryoscopeAPIClient

def setup_logging():
    """Setup basic logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def test_retry_logic():
    """Test the retry logic with mocked requests."""
    logger = setup_logging()
    
    # Mock configuration
    config = {
        'ip': '192.168.1.1',
        'login': 'WEB',
        'password': 'web',
        'port': 4000
    }
    
    logger.info("Testing API retry logic...")
    
    # Create API client
    client = EmbryoscopeAPIClient('Test Location', config, rate_limit_delay=0.01)
    
    # Test 1: Successful request on first try
    logger.info("\n1. Testing successful request on first try...")
    with patch.object(client.session, 'request') as mock_request:
        # Mock successful response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response
        
        result = client._rate_limited_request('GET', 'http://test.com')
        
        if result is not None:
            logger.info("✓ Success: Request succeeded on first try")
        else:
            logger.error("✗ Failure: Request failed unexpectedly")
            return False
    
    # Test 2: Request fails twice, succeeds on third try
    logger.info("\n2. Testing request that fails twice, succeeds on third try...")
    with patch.object(client.session, 'request') as mock_request:
        # Mock response that fails twice, then succeeds
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        
        # First two calls fail, third succeeds
        mock_request.side_effect = [
            Exception("Connection error"),  # First attempt
            Exception("Timeout error"),     # Second attempt  
            mock_response                   # Third attempt
        ]
        
        result = client._rate_limited_request('GET', 'http://test.com')
        
        if result is not None:
            logger.info("✓ Success: Request succeeded after 2 failures")
        else:
            logger.error("✗ Failure: Request failed after retries")
            return False
    
    # Test 3: Request fails all 3 times
    logger.info("\n3. Testing request that fails all 3 times...")
    with patch.object(client.session, 'request') as mock_request:
        # Mock response that always fails
        mock_request.side_effect = Exception("Persistent error")
        
        result = client._rate_limited_request('GET', 'http://test.com')
        
        if result is None:
            logger.info("✓ Success: Request correctly failed after all retries")
        else:
            logger.error("✗ Failure: Request should have failed but succeeded")
            return False
    
    # Test 4: Test exponential backoff timing
    logger.info("\n4. Testing exponential backoff timing...")
    with patch.object(client.session, 'request') as mock_request:
        # Mock response that fails twice, then succeeds
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        
        mock_request.side_effect = [
            Exception("Error 1"),
            Exception("Error 2"),
            mock_response
        ]
        
        start_time = time.time()
        result = client._rate_limited_request('GET', 'http://test.com')
        end_time = time.time()
        
        if result is not None:
            elapsed_time = end_time - start_time
            logger.info(f"✓ Success: Request succeeded after {elapsed_time:.2f} seconds")
            logger.info("  (Should include rate limit delay + exponential backoff)")
        else:
            logger.error("✗ Failure: Request failed unexpectedly")
            return False
    
    return True

def test_authentication_retry():
    """Test authentication retry logic."""
    logger = setup_logging()
    
    # Mock configuration
    config = {
        'ip': '192.168.1.1',
        'login': 'WEB',
        'password': 'web',
        'port': 4000
    }
    
    logger.info("\n5. Testing authentication retry logic...")
    
    # Create API client
    client = EmbryoscopeAPIClient('Test Location', config, rate_limit_delay=0.01)
    
    with patch.object(client.session, 'request') as mock_request:
        # Mock authentication response
        auth_response = Mock()
        auth_response.json.return_value = {'Token': 'test_token_123'}
        auth_response.raise_for_status.return_value = None
        
        # Mock API response that fails with 401, then succeeds
        api_response_fail = Mock()
        api_response_fail.status_code = 401
        api_response_fail.raise_for_status.return_value = None
        
        api_response_success = Mock()
        api_response_success.status_code = 200
        api_response_success.text = '{"data": "test"}'
        api_response_success.raise_for_status.return_value = None
        api_response_success.json.return_value = {"data": "test"}
        
        # First call: authentication succeeds, API call fails with 401
        # Second call: re-authentication succeeds, API call succeeds
        mock_request.side_effect = [
            auth_response,      # First auth
            api_response_fail,  # First API call (401)
            auth_response,      # Re-auth
            api_response_success # Second API call (success)
        ]
        
        result = client._make_authenticated_request("GET/test")
        
        if result is not None and result.get("data") == "test":
            logger.info("✓ Success: Authentication retry worked correctly")
        else:
            logger.error("✗ Failure: Authentication retry failed")
            return False
    
    return True

def main():
    """Run all tests."""
    logger = setup_logging()
    
    logger.info("=" * 50)
    logger.info("API Retry Logic Test")
    logger.info("=" * 50)
    
    # Test retry logic
    test1_passed = test_retry_logic()
    
    # Test authentication retry
    test2_passed = test_authentication_retry()
    
    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("TEST SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Retry logic test: {'✓ PASSED' if test1_passed else '✗ FAILED'}")
    logger.info(f"Authentication retry test: {'✓ PASSED' if test2_passed else '✗ FAILED'}")
    
    if all([test1_passed, test2_passed]):
        logger.info("\n🎉 All tests passed! The API retry logic should work correctly.")
    else:
        logger.info("\n⚠️  Some tests failed. Check the logs above for details.")

if __name__ == "__main__":
    main() 