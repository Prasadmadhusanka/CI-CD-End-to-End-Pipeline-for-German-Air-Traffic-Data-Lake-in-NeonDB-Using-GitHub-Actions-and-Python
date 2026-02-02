# manual_backfill/run_backfill.py

from manual_backfill.arrivals_history_backfill import backfill_arrivals
from manual_backfill.departures_history_backfill import backfill_departures

MISSING_DATES = [
    # '2025-12-01',
    # '2025-12-02',
    # '2025-12-03',
    # '2025-12-04',
    # '2025-12-05',
    # '2025-12-06',
    # '2025-12-07',
    # '2025-12-08',
    # '2025-12-09',
    # '2025-12-10',
    # '2025-12-11',
    # "2026-01-06",
    # "2026-01-07",
    # "2026-01-08",
    # "2026-01-09",
    # "2026-01-10",
    # "2026-01-11",
    "2026-01-12",
]

for date in MISSING_DATES:
    print(f"\n===== BACKFILL {date} =====")
    backfill_departures(date)
    backfill_arrivals(date)
