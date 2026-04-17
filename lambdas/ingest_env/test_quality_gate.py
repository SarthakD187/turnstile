"""Unit tests for ingest quality gate validation."""

from app import validate_record


def test_validate_record_accepts_valid_record() -> None:
    record = {
        'event_id': 'abc123',
        'time_utc': '2026-01-01T00:00:00Z',
        'mag': 2.5,
        'place': 'test',
        'lon': -77.6,
        'lat': 43.1,
        'depth_km': 10.0,
    }

    is_valid, reason = validate_record(record)

    assert is_valid is True
    assert reason == 'ok'


def test_validate_record_rejects_missing_required_field() -> None:
    record = {
        'event_id': 'abc123',
        'time_utc': '2026-01-01T00:00:00Z',
        'mag': 2.5,
        'place': 'test',
        'lon': -77.6,
        'lat': None,
        'depth_km': 10.0,
    }

    is_valid, reason = validate_record(record)

    assert is_valid is False
    assert reason == 'missing_required_field:lat'


def test_validate_record_rejects_type_mismatch() -> None:
    record = {
        'event_id': 'abc123',
        'time_utc': '2026-01-01T00:00:00Z',
        'mag': 'not-a-number',
        'place': 'test',
        'lon': -77.6,
        'lat': 43.1,
        'depth_km': 10.0,
    }

    is_valid, reason = validate_record(record)

    assert is_valid is False
    assert reason == 'type_mismatch:mag'
