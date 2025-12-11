# save_arrivals.py

import psycopg2
from psycopg2.extras import execute_batch
import os

# Neon DB connection string stored in GitHub secrets → ${{ secrets.NEON_DB_URL }}
NEON_DB_URL = os.getenv("NEON_DB_URL")

def save_arrival_flights(flights):
    """
    Saves a list of cleaned arrival flights to PostgreSQL (Neon).
    INSERT when new, UPDATE when existing.
    """

    if not flights:
        print("No flights to save in database.")
        return

    conn = psycopg2.connect(NEON_DB_URL)
    cur = conn.cursor()

    sql = """
    INSERT INTO arrivals (
        arrival_id,
        flight_iataNumber,
        flight_icaoNumber,
        flight_duration,
        status,
        airline_iata_code,
        airline_icao_code,
        airline_name,
        departure_baggage,
        departure_delay,
        departure_estimatedTime,
        departure_gate,
        departure_iataCode,
        departure_icaoCode,
        departure_scheduledTime,
        departure_terminal,
        arrival_baggage,
        arrival_delay,
        arrival_estimatedTime,
        arrival_gate,
        arrival_iataCode,
        arrival_icaoCode,
        arrival_scheduledTime,
        arrival_terminal
    )
    VALUES (
        %(key)s,
        %(flight_iataNumber)s,
        %(flight_icaoNumber)s,
        %(flight_duration)s,
        %(status)s,
        %(airline_iata_code)s,
        %(airline_icao_code)s,
        %(airline_name)s,
        %(departure_baggage)s,
        %(departure_delay)s,
        %(departure_estimatedTime)s,
        %(departure_gate)s,
        %(departure_iataCode)s,
        %(departure_icaoCode)s,
        %(departure_scheduledTime)s,
        %(departure_terminal)s,
        %(arrival_baggage)s,
        %(arrival_delay)s,
        %(arrival_estimatedTime)s,
        %(arrival_gate)s,
        %(arrival_iataCode)s,
        %(arrival_icaoCode)s,
        %(arrival_scheduledTime)s,
        %(arrival_terminal)s
    )
    ON CONFLICT (arrival_id) DO UPDATE SET
        flight_iataNumber = EXCLUDED.flight_iataNumber,
        flight_icaoNumber = EXCLUDED.flight_icaoNumber,
        flight_duration = EXCLUDED.flight_duration,
        status = EXCLUDED.status,
        airline_iata_code = EXCLUDED.airline_iata_code,
        airline_icao_code = EXCLUDED.airline_icao_code,
        airline_name = EXCLUDED.airline_name,
        departure_baggage = EXCLUDED.departure_baggage,
        departure_delay = EXCLUDED.departure_delay,
        departure_estimatedTime = EXCLUDED.departure_estimatedTime,
        departure_gate = EXCLUDED.departure_gate,
        departure_iataCode = EXCLUDED.departure_iataCode,
        departure_icaoCode = EXCLUDED.departure_icaoCode,
        departure_scheduledTime = EXCLUDED.departure_scheduledTime,
        departure_terminal = EXCLUDED.departure_terminal,
        arrival_baggage = EXCLUDED.arrival_baggage,
        arrival_delay = EXCLUDED.arrival_delay,
        arrival_estimatedTime = EXCLUDED.arrival_estimatedTime,
        arrival_gate = EXCLUDED.arrival_gate,
        arrival_iataCode = EXCLUDED.arrival_iataCode,
        arrival_icaoCode = EXCLUDED.arrival_icaoCode,
        arrival_scheduledTime = EXCLUDED.arrival_scheduledTime,
        arrival_terminal = EXCLUDED.arrival_terminal;
    """

    execute_batch(cur, sql, flights, page_size=200)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Saved {len(flights)} flights to Neon PostgreSQL.")
