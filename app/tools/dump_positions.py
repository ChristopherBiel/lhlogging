"""Quick position dump for debugging. Does NOT modify the database."""
import sys
import psycopg
from lhlogging import config

conn = psycopg.connect(
    host=config.DB_HOST, port=config.DB_PORT, dbname=config.DB_NAME,
    user=config.DB_USER, password=config.DB_PASSWORD,
)
cur = conn.cursor()
cur.execute(
    "SELECT captured_at, callsign, latitude, longitude, altitude_m, velocity_ms, on_ground"
    " FROM positions WHERE icao24 = %s AND captured_at >= %s AND captured_at <= %s"
    " ORDER BY captured_at",
    (sys.argv[1], sys.argv[2], sys.argv[3]),
)
for r in cur.fetchall():
    print(f"{r[0]}  {str(r[1]).strip():8s}  {r[2]:9.4f}  {r[3]:9.4f}  {str(r[4]):>8s}  {str(r[5]):>6s}  {r[6]}")
conn.close()
