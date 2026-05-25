#!/usr/bin/env python3
"""
Telegram interactive login helper script.
Run this script locally on your host machine to authorize Telethon
and generate a TELEGRAM_STRING_SESSION string.
"""
import os
import sys

try:
    from telethon.sync import TelegramClient
    from telethon.sessions import StringSession
except ImportError:
    print("Error: 'telethon' is not installed on your local host machine.")
    print("Please install it locally by running: pip install telethon")
    sys.exit(1)

def main():
    print("=== Telegram Client StringSession Generator ===")
    print("This script will help you log in to Telegram interactively and generate")
    print("a session string to run Telethon inside your Docker ETL pipeline.\n")

    api_id_env = os.environ.get("TELEGRAM_API_ID")
    api_hash_env = os.environ.get("TELEGRAM_API_HASH")

    if api_id_env:
        print(f"Detected TELEGRAM_API_ID from env: {api_id_env}")
        api_id_input = api_id_env
    else:
        api_id_input = input("Enter your Telegram API ID: ").strip()

    if api_hash_env:
        print(f"Detected TELEGRAM_API_HASH from env: {api_hash_env}")
        api_hash_input = api_hash_env
    else:
        api_hash_input = input("Enter your Telegram API Hash: ").strip()

    if not api_id_input or not api_hash_input:
        print("Error: API ID and API Hash are required.")
        sys.exit(1)

    try:
        api_id = int(api_id_input)
    except ValueError:
        print("Error: API ID must be an integer.")
        sys.exit(1)

    api_hash = api_hash_input

    # Use StringSession to generate a portable authorization token
    print("\nStarting Telethon client...")
    try:
        with TelegramClient(StringSession(), api_id, api_hash) as client:
            session_str = client.session.save()
            print("\n" + "=" * 50)
            print("SUCCESSFULLY LOGGED IN!")
            print("=" * 50)
            print("Copy the session string below and add it to your .env file:\n")
            print(f"TELEGRAM_STRING_SESSION={session_str}")
            print("\n" + "=" * 50)
    except Exception as e:
        print(f"\nAn error occurred during authentication: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
