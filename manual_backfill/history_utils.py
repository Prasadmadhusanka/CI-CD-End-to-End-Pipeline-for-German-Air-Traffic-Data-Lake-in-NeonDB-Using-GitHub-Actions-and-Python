# manual_backfill\history_utils.py

import os
import json
import requests
from datetime import datetime
from timezonefinder import TimezoneFinder
import pytz
from dotenv import load_dotenv

# -----------------------------
# CONFIG
# -----------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))  # load API_KEY and NEON_DB_URL

API_KEY = os.getenv("API_KEY")
HISTORY_API_URL = "https://aviation-edge.com/v2/public/flightsHistory"
WORLD_JSON_FILE = os.path.join(BASE_DIR, "data", "world_airports.json")

tf = TimezoneFinder()

# -----------------------------
# LOAD WORLD AIRPORT COORDS
# -----------------------------
with open(WORLD_JSON_FILE, "r", encoding="utf-8") as f:
    world_airports = json.load(f)

airport_coords = {
    a["iata_code"].strip(): (float(a["latitude_deg"]), float(a["longitude_deg"]))
    for a in world_airports
    if a.get("iata_code") and a.get("latitude_deg") and a.get("longitude_deg")
}

# -----------------------------
# CLEANERS
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
    except Exception:
        return None

def clean_iata(iata: str):
    if not iata:
        return None
    return iata.strip().upper()

def clean_icao(icao: str):
    if not icao:
        return None
    return icao.strip().upper()


# -----------------------------
# FLIGHT HISTORY API
# -----------------------------
def get_flights_history(iata_code, flight_type, date):
    """
    Fetch history for ONE DAY (API limitation)
    """
    params = {
        "key": API_KEY,
        "code": iata_code,
        "type": flight_type,
        "date_from": date,
        "date_to": date,
    }

    try:
        r = requests.get(HISTORY_API_URL, params=params, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"API error {iata_code} {date}: {e}")
        return None

# -----------------------------
# FLIGHT DURATION CALCULATION
# -----------------------------
def calculate_flight_duration_by_iata(
    dep_iata,
    arr_iata,
    dep_time_str,
    arr_time_str,
):
    if not dep_time_str or not arr_time_str:
        return None

    # Convert to uppercase
    dep_iata = dep_iata.upper() if dep_iata else None
    arr_iata = arr_iata.upper() if arr_iata else None

    if dep_iata not in airport_coords or arr_iata not in airport_coords:
        return None

    try:
        dep_coords = airport_coords[dep_iata]
        arr_coords = airport_coords[arr_iata]

        dep_tz = tf.timezone_at(lat=dep_coords[0], lng=dep_coords[1])
        arr_tz = tf.timezone_at(lat=arr_coords[0], lng=arr_coords[1])

        if not dep_tz or not arr_tz:
            return None

        dep_local = datetime.strptime(dep_time_str, "%Y-%m-%d %H:%M:%S")
        arr_local = datetime.strptime(arr_time_str, "%Y-%m-%d %H:%M:%S")

        dep_dt = pytz.timezone(dep_tz).localize(dep_local)
        arr_dt = pytz.timezone(arr_tz).localize(arr_local)

        duration = int(
            (arr_dt.astimezone(pytz.UTC) - dep_dt.astimezone(pytz.UTC))
            .total_seconds()
            / 60
        )

        return duration if duration > 0 else None

    except Exception:
        return None

