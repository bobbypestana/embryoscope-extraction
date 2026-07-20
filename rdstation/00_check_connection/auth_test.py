import os
import sys
import json
import urllib.parse
import webbrowser
import requests

# Set this to the exact Redirect URI configured in your RD Station CRM Developer App settings.
REDIRECT_URI = "https://www.huntington.com.br"

def load_env():
    # Look for .env in the parent directory of this file
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ[key.strip()] = val.strip()

def main():
    print("=" * 60)
    print(" RD STATION CRM V2 OAUTH 2.0 BOOTSTRAP")
    print("=" * 60)
    
    # Load environment variables from root .env if present
    load_env()
    
    # 1. Retrieve client credentials
    client_id = os.environ.get("RDSTATION_CLIENT_ID")
    client_secret = os.environ.get("RDSTATION_CLIENT_SECRET")
    redirect_uri = os.environ.get("RDSTATION_REDIRECT_URI", REDIRECT_URI)
    
    # Fallback to hardcoded values from previous runs if not in env
    if not client_id:
        client_id = "100cb324-61e8-470f-a0dd-c61e062c9c0c"
    if not client_secret:
        client_secret = "2e94d19acd2548caa261e1356a6827e5"
        
    print(f"[*] Client ID: {client_id}")
    print(f"[*] Redirect URI: {redirect_uri}\n")
    
    # 2. Build the CRM v2 authorization URL
    auth_url = (
        f"https://accounts.rdstation.com/oauth/authorize"
        f"?response_type=code"
        f"&client_id={client_id}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri)}"
    )
    
    print("[1] Opening your browser to authorize with RD Station CRM...")
    print("If it does not open automatically, copy and paste this URL into your browser:")
    print("-" * 80)
    print(auth_url)
    print("-" * 80)
    
    webbrowser.open(auth_url)
    
    # 3. Ask the user to paste the redirect URL or the code
    print("\n[2] After authorizing in your browser, you will be redirected.")
    print("Copy the value of the 'code' parameter from the redirected page's URL address bar.")
    
    code = input("\nEnter the authorization code: ").strip()
    
    if not code:
        print("[!] Error: Authorization code cannot be empty.")
        sys.exit(1)
        
    # If the user pasted the entire URL instead of just the code, parse it
    if "code=" in code:
        try:
            parsed_url = urllib.parse.urlparse(code)
            query_params = urllib.parse.parse_qs(parsed_url.query)
            if "code" in query_params:
                code = query_params["code"][0]
                print(f"[*] Extracted code from URL: {code[:8]}...")
        except Exception:
            pass
            
    # 4. Exchange auth code for access & refresh tokens
    print("\n[3] Exchanging authorization code for CRM v2 tokens...")
    token_url = "https://api.rd.services/oauth2/token"
    
    # CRM v2 requires x-www-form-urlencoded
    token_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
    }
    
    token_payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code"
    }
    
    try:
        response = requests.post(token_url, data=token_payload, headers=token_headers)
        response.raise_for_status()
        tokens = response.json()
    except requests.exceptions.RequestException as e:
        print(f"[!] Error exchanging tokens: {e}")
        if response is not None:
            print(f"Details: {response.text}")
        sys.exit(1)
        
    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    
    print("[+] Tokens successfully received!")
    print(f"Access Token: {access_token[:15]}...")
    print(f"Refresh Token: {refresh_token[:15]}...")
    
    # 5. Save tokens to files
    tokens_filepaths = [
        os.path.join(os.path.dirname(__file__), "tokens.json"),
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "01_data_ingestion", "tokens.json")
    ]
    for filepath in tokens_filepaths:
        try:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump({
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "access_token": access_token,
                    "refresh_token": refresh_token
                }, f, indent=4)
            print(f"[+] Saved tokens securely to {filepath}")
        except IOError as e:
            print(f"[!] Error saving tokens to file {filepath}: {e}")
        
    # 6. Test connection by making a CRM v2 API request
    print("\n[4] Testing connection to RD Station CRM v2 API...")
    test_headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    crm_url = "https://api.rd.services/crm/v2/contacts"
    print(f"[*] Trying RD Station CRM v2 contacts endpoint: {crm_url}")
    try:
        test_resp = requests.get(crm_url, headers=test_headers)
        if test_resp.status_code == 200:
            print("[+] Connection verified! Successfully retrieved CRM contacts.")
            contacts_data = test_resp.json()
            contacts = contacts_data.get("data", [])
            print(f"Found {len(contacts)} contacts. First few:")
            for contact in contacts[:3]:
                print(f"  - {contact.get('name')} (ID: {contact.get('id')})")
            print("\nVerification Complete! The integration works.")
            return
        else:
            print(f"[!] Connection check returned status {test_resp.status_code}: {test_resp.text}")
    except requests.exceptions.RequestException as e:
        print(f"[!] Error calling CRM test endpoint: {e}")

if __name__ == "__main__":
    main()
