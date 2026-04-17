"""Ingestion Lambda for Turnstile Bronze layer."""

import datetime as dt
import hashlib
import io
import json
import logging
import os
import time
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

USGS_URL = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'
OPEN_METEO_BASE = 'https://api.open-meteo.com/v1/forecast'
OPEN_METEO_PARAMS = {
    'hourly': 'temperature_2m,precipitation,wind_speed_10m',
    'timezone': 'UTC',
}

CITIES = [
    {'name': 'Rochester', 'lat': 43.1566, 'lon': -77.6088},
    {'name': 'New_York_City', 'lat': 40.7128, 'lon': -74.0060},
    {'name': 'Los_Angeles', 'lat': 34.0522, 'lon': -118.2437},
]


def _utc_now() -> dt.datetime:
    """Return timezone-aware UTC now."""
    return dt.datetime.now(tz=dt.timezone.utc)


def _ingestion_partition(now: dt.datetime) -> str:
    """Build S3 partition suffix for the current execution hour."""
    return now.strftime('ingestion_date=%Y/%m/%d/%H')


def _bytes(data: bytes) -> str:
    """Compute a deterministic SHA-256 hex hash for bytes payloads."""
    return hashlib.sha256(data).hexdigest()


def _to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Serialize a dataframe to snappy parquet bytes."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    sink = io.BytesIO()
    pq.write_table(table, sink, compression='snappy')
    return sink.getvalue()


def _put_s3_bytes(bucket: str, key: str, body: bytes, content_type: str = 'application/octet-stream') -> None:
    """Upload bytes to S3."""
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)


def validate_record(record: dict[str, Any]) -> tuple[bool, str]:
    """Validate a USGS record before it enters Bronze tables.

    Returns:
        Tuple of (is_valid, reason).
    """
    required_fields = ('event_id', 'time_utc', 'lon', 'lat')
    for field in required_fields:
        if record.get(field) in (None, ''):
            return False, f'missing_required_field:{field}'

    if not isinstance(record.get('event_id'), str):
        return False, 'type_mismatch:event_id'

    try:
        pd.to_datetime(record.get('time_utc'), utc=True)
    except Exception:  # noqa: BLE001
        return False, 'type_mismatch:time_utc'

    numeric_fields = ('lon', 'lat', 'depth_km')
    for field in numeric_fields:
        value = record.get(field)
        if value is None:
            continue
        try:
            float(value)
        except (TypeError, ValueError):
            return False, f'type_mismatch:{field}'

    mag = record.get('mag')
    if mag is not None:
        try:
            float(mag)
        except (TypeError, ValueError):
            return False, 'type_mismatch:mag'

    return True, 'ok'


def fetch_usgs() -> pd.DataFrame:
    """Fetch and quality-gate USGS hourly earthquake records."""
    response = requests.get(USGS_URL, timeout=30)
    response.raise_for_status()
    payload = response.json()

    accepted_rows: list[dict[str, Any]] = []
    rejected = 0

    for feature in payload.get('features', []):
        props = feature.get('properties', {}) or {}
        geom = feature.get('geometry', {}) or {}
        coords = geom.get('coordinates', [None, None, None])

        row: dict[str, Any] = {
            'event_id': feature.get('id'),
            'time_utc': pd.to_datetime(props.get('time'), unit='ms', utc=True),
            'mag': props.get('mag'),
            'place': props.get('place'),
            'lon': coords[0],
            'lat': coords[1],
            'depth_km': coords[2],
        }

        is_valid, reason = validate_record(row)
        if is_valid:
            accepted_rows.append(row)
        else:
            rejected += 1
            logger.warning('record_rejected source=usgs reason=%s event_id=%s', reason, row.get('event_id'))

    df = pd.DataFrame(accepted_rows)
    if not df.empty:
        df['mag'] = pd.to_numeric(df['mag'], errors='coerce')
        df['depth_km'] = pd.to_numeric(df['depth_km'], errors='coerce')

    logger.info('usgs_fetch_complete accepted=%s rejected=%s', len(accepted_rows), rejected)
    return df


def fetch_open_meteo(cities: list[dict[str, Any]]) -> pd.DataFrame:
    """Fetch weather forecast rows for configured cities."""
    frames: list[pd.DataFrame] = []

    for city in cities:
        params = dict(OPEN_METEO_PARAMS)
        params.update({'latitude': city['lat'], 'longitude': city['lon']})
        response = requests.get(OPEN_METEO_BASE, params=params, timeout=30)
        response.raise_for_status()

        payload = response.json()
        hourly = payload.get('hourly', {})
        times = hourly.get('time', []) or []
        temp = hourly.get('temperature_2m', []) or []
        precip = hourly.get('precipitation', []) or []
        wind = hourly.get('wind_speed_10m', []) or []

        count = min(len(times), len(temp), len(precip), len(wind))
        if count == 0:
            logger.warning('weather_city_empty city=%s', city['name'])
            continue

        city_frame = pd.DataFrame(
            {
                'city': city['name'],
                'timestamp_utc': pd.to_datetime(times[:count], utc=True),
                'temp_c': pd.to_numeric(temp[:count], errors='coerce'),
                'precip_mm': pd.to_numeric(precip[:count], errors='coerce'),
                'wind_mps': pd.to_numeric(wind[:count], errors='coerce'),
                'lat': city['lat'],
                'lon': city['lon'],
            }
        )
        frames.append(city_frame)

    merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    logger.info('weather_fetch_complete rows=%s cities=%s', int(merged.shape[0]), len(cities))
    return merged


def write_manifest(bucket: str, key_prefix: str, manifest: dict[str, Any]) -> None:
    """Persist a run manifest json file in the Bronze manifest prefix."""
    body = json.dumps(manifest, separators=(',', ':'), default=str).encode()
    key = f'{key_prefix}/manifest_{int(time.time())}.json'
    _put_s3_bytes(bucket, key, body, content_type='application/json')


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Lambda handler for ingesting source data into Bronze prefixes."""
    del context
    bucket = event.get('lakeBucket') or os.environ.get('LAKE_BUCKET')
    if not bucket:
        raise ValueError('Missing lake bucket (event.lakeBucket or env LAKE_BUCKET).')

    cities = event.get('cities') or CITIES
    now = _utc_now()
    partition = _ingestion_partition(now)

    logger.info('ingest_start partition=%s city_count=%s', partition, len(cities))

    usgs_df = fetch_usgs()
    usgs_count = int(usgs_df.shape[0]) if not usgs_df.empty else 0
    usgs_key: str | None = None
    if usgs_count > 0:
        usgs_df.drop_duplicates(subset=['event_id'], inplace=True)
        usgs_bytes = _to_parquet_bytes(usgs_df)
        usgs_hash = _bytes(usgs_bytes)[:16]
        usgs_key = f'bronze/usgs/{partition}/usgs_{usgs_hash}.parquet'
        _put_s3_bytes(bucket, usgs_key, usgs_bytes)

    weather_df = fetch_open_meteo(cities)
    weather_count = int(weather_df.shape[0]) if not weather_df.empty else 0
    weather_key: str | None = None
    if weather_count > 0:
        weather_bytes = _to_parquet_bytes(weather_df)
        weather_hash = _bytes(weather_bytes)[:16]
        weather_key = f'bronze/weather/{partition}/weather_{weather_hash}.parquet'
        _put_s3_bytes(bucket, weather_key, weather_bytes)

    manifest = {
        'run_utc': now.isoformat(),
        'partition': partition,
        'cities': [city['name'] for city in cities],
        'usgs': {'rows': usgs_count, 'path': usgs_key},
        'weather': {'rows': weather_count, 'path': weather_key},
    }
    write_manifest(bucket, f'bronze/_manifests/{partition}', manifest)

    logger.info(
        'ingest_complete partition=%s usgs_rows=%s weather_rows=%s',
        partition,
        usgs_count,
        weather_count,
    )
    return {'ok': True, 'manifest': manifest}
