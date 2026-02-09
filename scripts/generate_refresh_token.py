#!/usr/bin/env python3
"""
Generate OAuth2 Refresh Token for Google Ads API

Run this script to get a refresh token:
    python generate_refresh_token.py

Requires our OAuth Client ID and Client Secret from Google Cloud Console.
"""

import os

from google_auth_oauthlib.flow import InstalledAppFlow

# Google Ads API scope
SCOPES = ['https://www.googleapis.com/auth/adwords']


def main():
    print("=" * 60)
    print("Google Ads API - Refresh Token Generator")
    print("=" * 60)
    print()

    client_id = input("Enter OAuth Client ID: ").strip()
    client_secret = input("Enter OAuth Client Secret: ").strip()

    # Create OAuth flow
    client_config = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    }

    flow = InstalledAppFlow.from_client_config(client_config, scopes=SCOPES)

    print()
    print("A browser window will open. Log in with the Google account")
    print("that has access to our Google Ads MCC.")
    print()

    # Run local server to handle OAuth callback
    credentials = flow.run_local_server(port=8080)

    print()
    print("=" * 60)
    print("SUCCESS! Refresh token obtained.")
    print("=" * 60)
    print()

    # Write directly to config if it exists, otherwise prompt
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    if os.path.exists(config_path):
        import yaml
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f) or {}
        config.setdefault('google_ads', {})['refresh_token'] = credentials.refresh_token
        with open(config_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)
        print(f"Refresh token written to {config_path}")
    else:
        # Fall back to clipboard/masked output
        print("config.yaml not found. Token (copy it now — it will not be shown again):")
        print()
        print(credentials.refresh_token)
        print()
        print("Add to config/config.yaml under google_ads.refresh_token")
    print()


if __name__ == '__main__':
    main()
