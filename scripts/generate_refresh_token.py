#!/usr/bin/env python3
"""
Generate OAuth2 Refresh Token for Google Ads API

Run this script to get a refresh token:
    python generate_refresh_token.py

Requires our OAuth Client ID and Client Secret from Google Cloud Console.
"""

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
    print("SUCCESS! Here's the refresh token:")
    print("=" * 60)
    print()
    print(f"Refresh Token: {credentials.refresh_token}")
    print()
    print("Add this to the config.yaml file:")
    print()
    print(f'  refresh_token: "{credentials.refresh_token}"')
    print()


if __name__ == '__main__':
    main()
