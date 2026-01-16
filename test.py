"""Calls the API and prints the exchange rates along with API date with time"""

import requests
from datetime import datetime, timezone, timedelta

def fetch_rates():
    url = "https://api.frankfurter.app/latest?from=USD"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception("Failed to fetch exchange rates")

    data = response.json()

    # API only gives date (YYYY-MM-DD)
    api_date_str = data["date"]  # e.g., '2026-01-16'

    # Convert to datetime in UTC (assume midnight)
    api_datetime_utc = datetime.strptime(api_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    # Convert to IST (+5:30)
    ist_offset = timedelta(hours=5, minutes=30)
    api_datetime_ist = api_datetime_utc + ist_offset

    print("Base Currency:", data["base"])
    print("API Date (UTC):", api_datetime_utc.strftime("%Y-%m-%d %H:%M:%S %Z"))
    print("API Date (IST):", api_datetime_ist.strftime("%Y-%m-%d %H:%M:%S IST"))
    print("\nExchange Rates (USD → Others):")

    for currency in ["INR", "EUR", "GBP", "JPY"]:
        rate = data["rates"].get(currency)
        print(f"USD → {currency}: {rate}")

    return data, api_datetime_utc, api_datetime_ist


if __name__ == "__main__":
    rates, api_utc, api_ist = fetch_rates()
