import os, io, json, hashlib, time, datetime as dt
from typing import Dict, Any, List
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3

s3 = boto3.client("s3")

USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
# Add more hourly variables if you like; keep it small/cheap
OPEN_METEO_BASE = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_PARAMS = {
    "hourly": "temperature_2m,precipitation,wind_speed_10m",
    "timezone": "UTC"
}

# Default cities (small, illustrative; change later)
CITIES = [
    {"name": "Rochester", "lat": 43.1566, "lon": -77.6088},
    {"name": "New_York_City", "lat": 40.7128, "lon": -74.0060},
    {"name": "Los_Angeles", "lat": 34.0522, "lon": -118.2437},
]

def _utc_now():
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

def _ingestion_partition(now: dt.datetime):
    return now.strftime("ingestion_date=%Y/%m/%d/%H")

def _bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def _to_parquet_bytes(df: pd.DataFrame) -> bytes:
    table = pa.Table.from_pandas(df, preserve_index=False)
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="snappy")
    return sink.getvalue()

def _put_s3_bytes(bucket: str, key: str, body: bytes, content_type="application/octet-stream"):
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)

def fetch_usgs() -> pd.DataFrame:
    r = requests.get(USGS_URL, timeout=30)
    r.raise_for_status()
    payload = r.json()
    rows = []
    for feat in payload.get("features", []):
        props = feat.get("properties", {}) or {}
        geom = feat.get("geometry", {}) or {}
        coords = geom.get("coordinates", [None, None, None])
        rows.append({
            "event_id": feat.get("id"),
            "time_utc": pd.to_datetime(props.get("time"), unit="ms", utc=True),
            "mag": props.get("mag"),
            "place": props.get("place"),
            "lon": coords[0],
            "lat": coords[1],
            "depth_km": coords[2],
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        # Normalize types
        df["mag"] = pd.to_numeric(df["mag"], errors="coerce")
        df["depth_km"] = pd.to_numeric(df["depth_km"], errors="coerce")
    return df

def fetch_open_meteo(cities: List[Dict[str, Any]]) -> pd.DataFrame:
    frames = []
    for c in cities:
        params = dict(OPEN_METEO_PARAMS)
        params.update({"latitude": c["lat"], "longitude": c["lon"]})
        r = requests.get(OPEN_METEO_BASE, params=params, timeout=30)
        r.raise_for_status()
        j = r.json()
        hourly = j.get("hourly", {})
        times = hourly.get("time", []) or []
        temp = hourly.get("temperature_2m", []) or []
        precip = hourly.get("precipitation", []) or []
        wind = hourly.get("wind_speed_10m", []) or []
        # Align lengths defensively
        n = min(len(times), len(temp), len(precip), len(wind))
        if n == 0:
            continue
        f = pd.DataFrame({
            "city": c["name"],
            "timestamp_utc": pd.to_datetime(times[:n], utc=True),
            "temp_c": pd.to_numeric(temp[:n], errors="coerce"),
            "precip_mm": pd.to_numeric(precip[:n], errors="coerce"),
            "wind_mps": pd.to_numeric(wind[:n], errors="coerce"),
            "lat": c["lat"], "lon": c["lon"],
        })
        frames.append(f)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def write_manifest(bucket: str, key_prefix: str, manifest: Dict[str, Any]):
    body = json.dumps(manifest, separators=(",", ":"), default=str).encode()
    key = f"{key_prefix}/manifest_{int(time.time())}.json"
    _put_s3_bytes(bucket, key, body, content_type="application/json")

def handler(event, context):
    # Inputs: { "lakeBucket": "...", "cities": [..]?, "usgsSpan": "hour"|"day"? }
    bucket = event.get("lakeBucket") or os.environ.get("LAKE_BUCKET")
    if not bucket:
        raise ValueError("Missing lake bucket (event.lakeBucket or env LAKE_BUCKET).")
    cities = event.get("cities") or CITIES

    now = _utc_now()
    part = _ingestion_partition(now)

    # --- USGS ---
    usgs_df = fetch_usgs()
    usgs_count = int(usgs_df.shape[0]) if not usgs_df.empty else 0
    if usgs_count > 0:
        usgs_df.drop_duplicates(subset=["event_id"], inplace=True)
        pq_bytes = _to_parquet_bytes(usgs_df)
        h = _bytes(pq_bytes)[:16]
        usgs_key = f"bronze/usgs/{part}/usgs_{h}.parquet"
        _put_s3_bytes(bucket, usgs_key, pq_bytes)
    else:
        usgs_key = None

    # --- Weather ---
    wx_df = fetch_open_meteo(cities)
    wx_count = int(wx_df.shape[0]) if not wx_df.empty else 0
    wx_key = None
    if wx_count > 0:
        # partition by ingestion date and city to keep files tiny
        pq_bytes = _to_parquet_bytes(wx_df)
        h = _bytes(pq_bytes)[:16]
        wx_key = f"bronze/weather/{part}/weather_{h}.parquet"
        _put_s3_bytes(bucket, wx_key, pq_bytes)

    # Manifest (simple)
    manifest = {
        "run_utc": now.isoformat(),
        "usgs": {"rows": usgs_count, "path": usgs_key},
        "weather": {"rows": wx_count, "path": wx_key},
        "cities": [c["name"] for c in cities],
        "partition": part
    }
    write_manifest(bucket, f"bronze/_manifests/{part}", manifest)

    return {"ok": True, "manifest": manifest}
