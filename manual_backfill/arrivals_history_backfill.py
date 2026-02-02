# manual_backfill/arrivals_history_backfill.py

import json
import time
import os

from manual_backfill.history_utils import (
    get_flights_history,
    clean_timestamp,
    calculate_flight_duration_by_iata,
    clean_iata,
    clean_icao,
)

from save_arrivals import save_arrival_flights  # Assuming you save arrivals in same table

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GERMANY_JSON_FILE = os.path.join(BASE_DIR, "data", "germany_airports.json")
WORLD_JSON_FILE = os.path.join(BASE_DIR, "data", "world_airports.json")

SLEEP = 1
MAX_RETRY_ROUNDS = 3
RETRY_SLEEP = 5

# Load valid world IATA codes to prevent FK violations
with open(WORLD_JSON_FILE, "r", encoding="utf-8") as f:
    world_airports = json.load(f)
world_iata_codes = {clean_iata(a.get("iata_code")) for a in world_airports if a.get("iata_code")}


def backfill_arrivals(date):
    """Fetch and save all arrivals for Germany airports on a given date."""
    with open(GERMANY_JSON_FILE, "r", encoding="utf-8") as f:
        airports = json.load(f)

    flights_all = []
    skipped_airports = []

    for airport in airports:
        iata_raw = airport.get("iata_code")
        iata = clean_iata(iata_raw)
        if not iata:
            continue

        print(f"Fetching ARRIVALS {iata} {date}")

        # Retry loop for API requests
        raw = None
        for round_num in range(1, MAX_RETRY_ROUNDS + 1):
            raw = get_flights_history(iata, "arrival", date)
            if isinstance(raw, list):
                break
            elif raw is None:
                print(f"Retry {round_num}/{MAX_RETRY_ROUNDS} for {iata}")
                time.sleep(RETRY_SLEEP)
            elif isinstance(raw, dict) and raw.get("success") is False:
                print(f"Skipping {iata}: {raw.get('message')}")
                skipped_airports.append(iata)
                raw = None
                break
        else:
            print(f"Failed to fetch data for {iata} after {MAX_RETRY_ROUNDS} retries")
            skipped_airports.append(iata)
            continue

        if not raw:
            continue

        # Remove codeshared flights
        raw = [f for f in raw if f.get("codeshared") is None]

        for flight in raw:
            dep = flight.get("departure", {})
            arr = flight.get("arrival", {})
            f_info = flight.get("flight", {})
            airline = flight.get("airline", {})

            dep_time = clean_timestamp(dep.get("scheduledTime"))
            arr_time = clean_timestamp(arr.get("scheduledTime"))

            dep_iata_raw = clean_iata(dep.get("iataCode"))
            arr_iata_raw = clean_iata(arr.get("iataCode"))
            dep_iata = dep_iata_raw if dep_iata_raw in world_iata_codes else "999"
            arr_iata = arr_iata_raw if arr_iata_raw in world_iata_codes else "999"
            dep_icao = clean_icao(dep.get("icaoCode"))
            arr_icao = clean_icao(arr.get("icaoCode"))

            record = {
                "key": f"{arr_time}_{arr_iata}_{f_info.get('iataNumber')}",
                "flight_iataNumber": f_info.get("iataNumber"),
                "flight_icaoNumber": f_info.get("icaoNumber"),
                "flight_duration": calculate_flight_duration_by_iata(
                    dep_iata, arr_iata, dep_time, arr_time
                ),
                "status": flight.get("status"),
                "airline_iata_code": airline.get("iataCode"),
                "airline_icao_code": airline.get("icaoCode"),
                "airline_name": airline.get("name"),
                "departure_baggage": dep.get("baggage"),
                "departure_delay": dep.get("delay"),
                "departure_estimatedTime": clean_timestamp(dep.get("estimatedTime")),
                "departure_gate": dep.get("gate"),
                "departure_iataCode": dep_iata,
                "departure_icaoCode": dep_icao,
                "departure_scheduledTime": dep_time,
                "departure_terminal": dep.get("terminal"),
                "arrival_baggage": arr.get("baggage"),
                "arrival_delay": arr.get("delay"),
                "arrival_estimatedTime": clean_timestamp(arr.get("estimatedTime")),
                "arrival_gate": arr.get("gate"),
                "arrival_iataCode": arr_iata,
                "arrival_icaoCode": arr_icao,
                "arrival_scheduledTime": arr_time,
                "arrival_terminal": arr.get("terminal"),
            }

            flights_all.append(record)

        time.sleep(SLEEP)

    save_arrival_flights(flights_all)
    print(f"Saved {len(flights_all)} arrivals to Neon PostgreSQL")
    if skipped_airports:
        print(f"Skipped airports (no data or 404): {', '.join(skipped_airports)}")
