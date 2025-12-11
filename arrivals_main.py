# arrivals_main.py

import json
import time
import requests
from datetime import datetime
from timezonefinder import TimezoneFinder
import pytz
import os

from save_arrivals import save_arrival_flights

# -----------------------------
# CONFIGURATION
# -----------------------------
API_KEY = API_KEY = os.getenv("API_KEY")
GERMANY_JSON_FILE = "data/germany_airports.json"
WORLD_JSON_FILE = "data/world_airports.json"
API_URL = "https://aviation-edge.com/v2/public/timetable"

SLEEP_BETWEEN_CALLS = 0.5
MAX_RETRY_ROUNDS = 5


# -----------------------------
# DATE-TIME CLEANING
# -----------------------------
def clean_timestamp(ts: str):
    if not isinstance(ts, str):
        return None

    try:
        if "." in ts:
            dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f")
        else:
            dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return None


# -----------------------------
# LOAD GERMANY AIRPORT LIST
# -----------------------------
with open(GERMANY_JSON_FILE, "r", encoding="utf-8") as f:
    airports = json.load(f)

iata_codes = [item["iata_code"] for item in airports if item.get("iata_code")]
print(f"Loaded {len(iata_codes)} airports with valid IATA codes.")


# -----------------------------
# LOAD WORLD AIRPORT LIST
# -----------------------------
with open(WORLD_JSON_FILE, "r", encoding="utf-8") as f:
    world_airports = json.load(f)

world_iata_codes = {item["iata_code"].strip() for item in world_airports if item.get("iata_code")}

# Build map: IATA → (lat, lon)
airport_coords = {}
for a in world_airports:
    code = a.get("iata_code")
    lat = a.get("latitude_deg")
    lon = a.get("longitude_deg")
    if code and lat and lon:
        airport_coords[code.strip()] = (float(lat), float(lon))


# -----------------------------
# FLIGHT DURATION CALCULATION FUNCTION
# -----------------------------
tf = TimezoneFinder()

def calculate_flight_duration(dep_iata, arr_iata, dep_time_str, arr_time_str):
    """Return flight duration in minutes (INT) or None if cannot compute."""

    # If airport code not found → cannot get coordinates
    if dep_iata not in airport_coords or arr_iata not in airport_coords:
        return None

    dep_coords = airport_coords[dep_iata]
    arr_coords = airport_coords[arr_iata]

    try:
        dep_tz_name = tf.timezone_at(lat=dep_coords[0], lng=dep_coords[1])
        arr_tz_name = tf.timezone_at(lat=arr_coords[0], lng=arr_coords[1])

        if not dep_tz_name or not arr_tz_name:
            return None

        # Convert the timestamp strings to datetime objects
        if not dep_time_str or not arr_time_str:
            return None

        dep_local = datetime.strptime(dep_time_str, "%Y-%m-%d %H:%M:%S")
        arr_local = datetime.strptime(arr_time_str, "%Y-%m-%d %H:%M:%S")

        # Localize using pytz
        dep_dt = pytz.timezone(dep_tz_name).localize(dep_local)
        arr_dt = pytz.timezone(arr_tz_name).localize(arr_local)

        # Convert to UTC
        dep_utc = dep_dt.astimezone(pytz.UTC)
        arr_utc = arr_dt.astimezone(pytz.UTC)

        # Calculate difference in minutes
        duration_minutes = int((arr_utc - dep_utc).total_seconds() / 60)

        # Sanity check (filter negative values)
        if duration_minutes < 0:
            return None

        return duration_minutes

    except Exception:
        return None


# -----------------------------
# API CALL FUNCTION
# -----------------------------
def get_timetable(iata_code: str, flight_type="arrival"):
    params = {
        "key": API_KEY,
        "iataCode": iata_code,
        "type": flight_type
    }
    try:
        response = requests.get(API_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching data for airport {iata_code}: {e}")
        return None


# -----------------------------
# FUNCTION TO PROCESS AIRPORT LIST
# -----------------------------
def process_airports(iata_list):
    clean_flights = []
    failed = []

    for iata in iata_list:
        print(f"\nFetching timetable for {iata} ...")
        raw_flights = get_timetable(iata)

        if raw_flights is None:
            failed.append(iata)
            continue

        if isinstance(raw_flights, dict) and raw_flights.get("success") is False:
            continue

        if not isinstance(raw_flights, list):
            failed.append(iata)
            continue

        # remove codeshared
        flights = [f for f in raw_flights if f.get("codeshared") is None]

        for flight in flights:
            airline = flight.get("airline", {})
            fblock = flight.get("flight", {})
            dep = flight.get("departure", {})
            arr = flight.get("arrival", {})

            # Extract raw codes
            dep_iata_raw = (dep.get("iataCode") or "").strip()
            arr_iata_raw = (arr.get("iataCode") or "").strip()

            # Validate airport codes using world airports reference file
            dep_iata = dep_iata_raw if dep_iata_raw in world_iata_codes else "999"
            arr_iata = arr_iata_raw if arr_iata_raw in world_iata_codes else "999"

            minimal = {
                "type": flight.get("type", "N/A"),
                "status": flight.get("status", "N/A"),

                "airline_iata_code": (airline.get("iataCode") or "").strip(),
                "airline_icao_code": (airline.get("icaoCode") or "").strip(),
                "airline_name": airline.get("name"),

                "flight_iataNumber": fblock.get("iataNumber"),
                "flight_icaoNumber": fblock.get("icaoNumber"),

                "departure_baggage": dep.get("baggage"),
                "departure_delay": dep.get("delay"),
                "departure_estimatedTime": clean_timestamp(dep.get("estimatedTime")),
                "departure_gate": dep.get("gate"),
                "departure_iataCode": dep_iata,
                "departure_icaoCode": (dep.get("icaoCode") or "").strip(),
                "departure_scheduledTime": clean_timestamp(dep.get("scheduledTime")),
                "departure_terminal": dep.get("terminal"),

                "arrival_baggage": arr.get("baggage"),
                "arrival_delay": arr.get("delay"),
                "arrival_estimatedTime": clean_timestamp(arr.get("estimatedTime")),
                "arrival_gate": arr.get("gate"),
                "arrival_iataCode": arr_iata,
                "arrival_icaoCode": (arr.get("icaoCode") or "").strip(),
                "arrival_scheduledTime": clean_timestamp(arr.get("scheduledTime")),
                "arrival_terminal": arr.get("terminal"),
            }

            minimal["key"] = f"{minimal['arrival_scheduledTime']}_{minimal['arrival_iataCode']}_{minimal['flight_iataNumber']}"

            # Calculate flight duration
            minimal["flight_duration"] = calculate_flight_duration(dep_iata, arr_iata, minimal["departure_scheduledTime"], minimal["arrival_scheduledTime"])

            clean_flights.append(minimal)

        time.sleep(SLEEP_BETWEEN_CALLS)

    return clean_flights, failed


# -----------------------------
# MAIN EXECUTION
# -----------------------------
all_clean_arrival_flights = []
to_process = iata_codes

for round_num in range(1, MAX_RETRY_ROUNDS + 1):
    print(f"\n===== RETRY ROUND {round_num} =====")

    flights, failed = process_airports(to_process)
    all_clean_arrival_flights.extend(flights)

    print(f"Finished round {round_num}: kept={len(flights)}, failed={len(failed)}")

    if not failed:
        break

    to_process = failed

print(f"\nTotal flights processed: {len(all_clean_arrival_flights)}")

# SAVE TO NEON DB
save_arrival_flights(all_clean_arrival_flights)
