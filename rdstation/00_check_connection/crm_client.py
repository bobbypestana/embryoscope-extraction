import os
import json
import requests

class RDStationCRMClient:
    def __init__(self, tokens_filepath=None):
        if not tokens_filepath:
            tokens_filepath = os.path.join(os.path.dirname(__file__), "tokens.json")
            
        self.tokens_filepath = tokens_filepath
        self.load_tokens()
        self.base_url = "https://api.rd.services/crm/v2"

    def load_tokens(self):
        """Loads client credentials and tokens from the JSON storage."""
        if not os.path.exists(self.tokens_filepath):
            raise FileNotFoundError(
                f"Token file not found at '{self.tokens_filepath}'. "
                "You must perform the initial OAuth bootstrap process once using auth_test.py."
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
        Refreshes the CRM v2 access token using the stored refresh token.
        Runs fully in the background with zero user prompts.
        """
        print("[*] Refreshing CRM v2 access token in the background...")
        url = "https://api.rd.services/oauth2/token"
        
        # CRM v2 requires x-www-form-urlencoded
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        }
        
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token"
        }
        
        response = requests.post(url, data=payload, headers=headers)
        if response.status_code == 200:
            data = response.json()
            self.access_token = data.get("access_token")
            # Rolling refresh token: update refresh_token if a new one is returned
            if "refresh_token" in data:
                self.refresh_token = data.get("refresh_token")
            self.save_tokens()
            print("[+] CRM v2 access token successfully refreshed and saved.")
        else:
            print(f"[!] Failed to refresh CRM token: {response.text}")
            response.raise_for_status()

    def make_request(self, method, endpoint, **kwargs):
        """
        Makes an API request to CRM v2, automatically handling token refresh if 401 Unauthorized is returned.
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
            print("[!] Request returned 401. CRM access token may be expired.")
            self.refresh_access_token()
            # Update headers with new token
            kwargs["headers"]["Authorization"] = f"Bearer {self.access_token}"
            response = requests.request(method, url, **kwargs)
            
        return response

    def get_users(self):
        """
        Retrieves users from the RD Station CRM.
        """
        response = self.make_request("GET", "users")
        if response.status_code == 200:
            return response.json().get("data", [])
        else:
            print(f"[!] Error fetching users ({response.status_code}): {response.text}")
            response.raise_for_status()

    def create_deal(self, deal_name, deal_price, user_id=None):
        """
        Creates a deal (opportunity) in RD Station CRM v2.
        """
        if not user_id:
            # Fetch users and default to the first user in the list
            users = self.get_users()
            if users:
                user_id = users[0]["id"]
                print(f"[*] Defaulting deal owner to: {users[0]['name']} (ID: {user_id})")
            else:
                raise ValueError("No users found in CRM. A deal must have an owner.")

        endpoint = "deals"
        payload = {
            "data": {
                "name": deal_name,
                "status": "ongoing",
                "one_time_price": float(deal_price),
                "owner_id": user_id
            }
        }
        
        response = self.make_request("POST", endpoint, json=payload)
        if response.status_code in [200, 201]:
            print(f"[+] Deal '{deal_name}' created in the background successfully.")
            return response.json()
        else:
            print(f"[!] Error creating deal ({response.status_code}): {response.text}")
            response.raise_for_status()

# Quick test run if executed directly
if __name__ == "__main__":
    try:
        client = RDStationCRMClient()
        print("[*] CRM Client initialized. Creating a test deal in the background...")
        client.create_deal(
            deal_name="CRM v2 Background Deal",
            deal_price=500.0
        )
    except Exception as e:
        print(f"[!] Error: {e}")
