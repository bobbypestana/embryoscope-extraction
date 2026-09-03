import os
import json
import time
import requests
import boto3

class RDStationCRMClient:
    def __init__(self, tokens_filepath=None, use_aws=True, secret_name="rdstation-api-prod", region_name="sa-east-1"):
        if not tokens_filepath:
            tokens_filepath = os.path.join(os.path.dirname(__file__), "tokens.json")
            
        self.tokens_filepath = tokens_filepath
        self.use_aws = use_aws
        self.secret_name = secret_name
        self.region_name = region_name
        self.token_expires_at = None
        self.sm_client = None

        if self.use_aws:
            try:
                self.sm_client = boto3.client('secretsmanager', region_name=self.region_name)
            except Exception as e:
                print(f"[!] Could not initialize boto3 SecretsManager: {e}. Using local file fallback.")
                self.use_aws = False

        self.load_tokens()
        self.base_url = "https://api.rd.services/crm/v2"

    def load_tokens(self):
        """Loads client credentials and tokens from AWS Secrets Manager or JSON storage."""
        if self.use_aws and self.sm_client:
            try:
                print(f"[*] Loading tokens from AWS Secrets Manager ({self.secret_name})...")
                res = self.sm_client.get_secret_value(SecretId=self.secret_name)
                data = json.loads(res.get("SecretString", "{}"))
                self.client_id = data.get("RDSTATION_CLIENT_ID") or data.get("client_id")
                self.client_secret = data.get("RDSTATION_CLIENT_SECRET") or data.get("client_secret")
                self.access_token = data.get("ACCESS_TOKEN") or data.get("access_token")
                self.refresh_token = data.get("REFRESH_TOKEN") or data.get("refresh_token")
                self.token_expires_at = data.get("TOKEN_EXPIRES_AT") or data.get("expires_at")
                print("[+] Loaded tokens from AWS Secrets Manager.")
                self._save_to_local_file()
                return
            except Exception as e:
                print(f"[!] Could not load from AWS Secrets Manager: {e}. Falling back to local file.")

        if not os.path.exists(self.tokens_filepath):
            raise FileNotFoundError(
                f"Token file not found at '{self.tokens_filepath}'. "
                "Ensure AWS secret exists or auth bootstrap has run."
            )
            
        with open(self.tokens_filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            
        self.client_id = data.get("RDSTATION_CLIENT_ID") or data.get("client_id")
        self.client_secret = data.get("RDSTATION_CLIENT_SECRET") or data.get("client_secret")
        self.access_token = data.get("ACCESS_TOKEN") or data.get("access_token")
        self.refresh_token = data.get("REFRESH_TOKEN") or data.get("refresh_token")
        self.token_expires_at = data.get("TOKEN_EXPIRES_AT") or data.get("expires_at")

    def _save_to_local_file(self):
        """Saves current credentials and tokens back to the local JSON storage."""
        try:
            with open(self.tokens_filepath, "w", encoding="utf-8") as f:
                json.dump({
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "access_token": self.access_token,
                    "refresh_token": self.refresh_token,
                    "token_expires_at": str(self.token_expires_at) if self.token_expires_at else ""
                }, f, indent=4)
        except Exception as e:
            print(f"[!] Warning: Could not write local token file: {e}")

    def save_tokens(self):
        """Saves current credentials and tokens to AWS Secrets Manager and local backup file."""
        if self.use_aws and self.sm_client:
            try:
                secret_payload = {
                    "RDSTATION_CLIENT_ID": self.client_id or "",
                    "RDSTATION_CLIENT_SECRET": self.client_secret or "",
                    "ACCESS_TOKEN": self.access_token or "",
                    "REFRESH_TOKEN": self.refresh_token or "",
                    "TOKEN_EXPIRES_AT": str(self.token_expires_at) if self.token_expires_at else ""
                }
                self.sm_client.put_secret_value(
                    SecretId=self.secret_name,
                    SecretString=json.dumps(secret_payload)
                )
                print(f"[+] Saved updated tokens to AWS Secrets Manager ({self.secret_name}).")
            except Exception as e:
                print(f"[!] Failed to update AWS Secrets Manager: {e}")
                raise

        self._save_to_local_file()

    def refresh_access_token(self):
        """
        Refreshes the CRM v2 access token using the stored refresh token.
        Runs fully in the background with zero user prompts.
        """
        print("[*] Refreshing CRM v2 access token in the background...")
        url = "https://api.rd.services/oauth2/token"
        
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
            if "refresh_token" in data:
                self.refresh_token = data.get("refresh_token")
            expires_in = data.get("expires_in", 86400)
            self.token_expires_at = str(time.time() + expires_in)
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
