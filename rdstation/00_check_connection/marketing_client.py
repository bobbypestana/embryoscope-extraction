import os
import json
import requests

class RDStationMarketingClient:
    def __init__(self, tokens_filepath=None):
        if not tokens_filepath:
            tokens_filepath = os.path.join(os.path.dirname(__file__), "tokens.json")
            
        self.tokens_filepath = tokens_filepath
        self.load_tokens()
        self.base_url = "https://api.rd.services/platform"

    def load_tokens(self):
        """Loads client credentials and tokens from the JSON storage."""
        if not os.path.exists(self.tokens_filepath):
            raise FileNotFoundError(
                f"Token file not found at '{self.tokens_filepath}'. "
                "You must perform the initial OAuth bootstrap process once."
            )
            
        with open(self.tokens_filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            
        self.client_id = data.get("client_id")
        self.client_secret = data.get("client_secret")
        self.access_token = data.get("access_token")
        self.refresh_token = data.get("refresh_token")

    def save_tokens(self):
        """Saves current credentials and tokens back to the JSON storage."""
        with open(self.tokens_filepath, "w", encoding="utf-8") as f:
            json.dump({
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "access_token": self.access_token,
                "refresh_token": self.refresh_token
            }, f, indent=4)

    def refresh_access_token(self):
        """
        Refreshes the access token using the stored refresh token.
        Runs fully in the background with zero user prompts or interface.
        """
        print("[*] Refreshing access token in the background...")
        url = "https://api.rd.services/auth/token"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token"
        }
        
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            data = response.json()
            self.access_token = data.get("access_token")
            # The API usually issues a new refresh token as well; save it if returned
            if "refresh_token" in data:
                self.refresh_token = data.get("refresh_token")
            self.save_tokens()
            print("[+] Access token successfully refreshed and saved.")
        else:
            print(f"[!] Failed to refresh token: {response.text}")
            response.raise_for_status()

    def make_request(self, method, endpoint, **kwargs):
        """
        Makes an API request, automatically handling token refresh if 401 Unauthorized is returned.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        if "headers" not in kwargs:
            kwargs["headers"] = {}
        kwargs["headers"]["Authorization"] = f"Bearer {self.access_token}"
        kwargs["headers"]["Accept"] = "application/json"
        
        # Try request
        response = requests.request(method, url, **kwargs)
        
        # If token expired (401 Unauthorized), refresh token and retry once
        if response.status_code == 401:
            print("[!] Request returned 401. Access token may be expired.")
            self.refresh_access_token()
            # Update headers with new token
            kwargs["headers"]["Authorization"] = f"Bearer {self.access_token}"
            response = requests.request(method, url, **kwargs)
            
        return response

    def create_lead_event(self, email, event_name, payload_data=None):
        """
        Creates an event for a contact in the background.
        """
        endpoint = "events"
        payload = {
            "event_type": "CONVERSION",
            "event_family": "CDP",
            "payload": {
                "email": email,
                "conversion_identifier": event_name,
                **(payload_data or {})
            }
        }
        
        response = self.make_request("POST", endpoint, json=payload)
        if response.status_code in [200, 201]:
            print(f"[+] Event '{event_name}' recorded for contact '{email}' successfully.")
            return response.json()
        else:
            print(f"[!] Error creating event ({response.status_code}): {response.text}")
            response.raise_for_status()

if __name__ == "__main__":
    try:
        client = RDStationMarketingClient()
        print("[*] Marketing client initialized. Sending background event...")
        client.create_lead_event(
            email="tester@example.com",
            event_name="background_automated_event",
            payload_data={"name": "Background Tester"}
        )
    except Exception as e:
        print(f"[!] Error: {e}")
