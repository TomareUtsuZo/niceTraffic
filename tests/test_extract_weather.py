import pytest
import os
import sys
import tempfile # Make sure tempfile is imported
from unittest.mock import patch, MagicMock
import pandas as pd
import duckdb
import requests
import datetime


# Add the directory containing the script to the Python path
# This allows importing the script as a module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

# Import the functions from the script
try:
    from extract_weather_duckdb import (
        construct_weather_api_url,
        fetch_data_from_api,
        parse_weather_response_to_dataframe,
        save_weather_to_duckdb,
    )
except ImportError as e:
    pytest.fail(f"Failed to import functions from extract_weather_duckdb.py: {e}. "
                "Ensure the script is in the same directory or the path is correct.")


# --- Fixtures ---

@pytest.fixture
def mock_location():
    """Provides a sample location dictionary."""
    return {"lat": 40.7128, "lon": -74.0060, "name": "New York City"}

@pytest.fixture
def mock_api_key():
    """Provides a sample API key."""
    return "test_api_key"

@pytest.fixture
def mock_base_url():
    """Provides a sample base URL."""
    return "https://api.testweather.com/v1/current"

@pytest.fixture
def mock_weather_response_json():
    """Provides sample valid JSON response data matching the parsing logic."""
    return """
    {
      "current": {
        "temperature": 15.5,
        "feels_like": 14.0,
        "pressure": 1012,
        "humidity": 65,
        "wind_speed": 5.2,
        "wind_direction": 220,
        "summary": "Partly Cloudy",
        "icon": "partly_cloudy",
        "cloud_cover": 50
      },
      "hourly": {},
      "daily": {}
    }
    """

@pytest.fixture
def mock_weather_response_invalid_json():
    """Provides invalid JSON response data."""
    return """
    This is not JSON data
    """

@pytest.fixture
def mock_weather_response_missing_keys():
    """Provides JSON response data missing expected keys."""
    return """
    {
      "other_section": {
        "some_key": "some_value"
      }
    }
    """

@pytest.fixture
def temp_duckdb_path():
    """Creates a temporary directory and provides a file path within it for DuckDB."""
    # Use TemporaryDirectory to ensure proper cleanup
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.duckdb")
        yield db_path
    # TemporaryDirectory automatically handles cleanup of the directory and its contents

# --- Tests ---

def test_construct_weather_api_url_success(mock_location, mock_api_key, mock_base_url):
    """Tests if construct_weather_api_url creates the correct URL."""
    expected_start = f"{mock_base_url}?lat={mock_location['lat']}&lon={mock_location['lon']}"
    url = construct_weather_api_url(mock_location, mock_api_key, mock_base_url)

    assert expected_start in url
    assert f"key={mock_api_key}" in url
    assert "sections=all" in url
    assert "timezone=UTC" in url
    assert "language=en" in url
    assert "units=metric" in url

def test_construct_weather_api_url_missing_coords(mock_api_key, mock_base_url):
    """Tests if construct_weather_api_url raises error for missing coords."""
    location_missing_lat = {"lon": -74.0060}
    location_missing_lon = {"lat": 40.7128}
    location_empty = {}

    with pytest.raises(ValueError, match="Invalid location coordinates"):
        construct_weather_api_url(location_missing_lat, mock_api_key, mock_base_url)
    with pytest.raises(ValueError, match="Invalid location coordinates"):
        construct_weather_api_url(location_missing_lon, mock_api_key, mock_base_url)
    with pytest.raises(ValueError, match="Invalid location coordinates"):
        construct_weather_api_url(location_empty, mock_api_key, mock_base_url)


@patch('requests.get')
def test_fetch_data_from_api_success(mock_get):
    """Tests if fetch_data_from_api successfully fetches data."""
    mock_response = MagicMock()
    mock_response.text = "mock response data"
    mock_response.raise_for_status.return_value = None # Simulate success
    mock_get.return_value = mock_response

    url = "http://test.com/api"
    timeout = 5
    data = fetch_data_from_api(url, timeout)

    mock_get.assert_called_once_with(url=url, timeout=timeout)
    assert data == "mock response data"

@patch('requests.get')
def test_fetch_data_from_api_failure(mock_get):
    """Tests if fetch_data_from_api handles request exceptions."""
    mock_get.side_effect = requests.exceptions.RequestException("API error")

    url = "http://test.com/api"
    timeout = 5
    data = fetch_data_from_api(url, timeout)

    mock_get.assert_called_once_with(url=url, timeout=timeout)
    assert data is None

@patch('requests.get')
def test_fetch_data_from_api_http_error(mock_get):
    """Tests if fetch_data_from_api handles HTTP errors."""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("HTTP error")
    mock_get.return_value = mock_response

    url = "http://test.com/api"
    timeout = 5
    data = fetch_data_from_api(url, timeout)

    mock_get.assert_called_once_with(url=url, timeout=timeout)
    assert data is None


def test_parse_weather_response_to_dataframe_success(mock_weather_response_json, mock_location):
    """Tests if parse_weather_response_to_dataframe parses valid JSON correctly."""
    df = parse_weather_response_to_dataframe(mock_weather_response_json, mock_location)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert len(df) == 1 # Expecting one row for current data

    expected_cols = [
        'latitude', 'longitude', 'fetch_timestamp_utc', 'location_name',
        'temperature_celsius', 'weather_description', 'weather_icon'
    ]
    for col in expected_cols:
        assert col in df.columns

    assert pd.api.types.is_float_dtype(df['latitude'])
    assert pd.api.types.is_float_dtype(df['longitude'])
    assert pd.api.types.is_datetime64_any_dtype(df['fetch_timestamp_utc'])
    assert pd.api.types.is_object_dtype(df['location_name'])
    assert pd.api.types.is_float_dtype(df['temperature_celsius'])
    assert pd.api.types.is_object_dtype(df['weather_description'])
    assert pd.api.types.is_object_dtype(df['weather_icon'])

    assert df['latitude'].iloc[0] == mock_location['lat']
    assert df['longitude'].iloc[0] == mock_location['lon']
    assert df['location_name'].iloc[0] == mock_location['name']
    assert df['temperature_celsius'].iloc[0] == 15.5
    assert df['weather_description'].iloc[0] == "Partly Cloudy"
    assert df['weather_icon'].iloc[0] == "partly_cloudy"


def test_parse_weather_response_to_dataframe_invalid_json(mock_weather_response_invalid_json, mock_location):
    """Tests if parse_weather_response_to_dataframe handles invalid JSON."""
    df = parse_weather_response_to_dataframe(mock_weather_response_invalid_json, mock_location)
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_parse_weather_response_to_dataframe_missing_keys(mock_weather_response_missing_keys, mock_location):
    """Tests if parse_weather_response_to_dataframe handles missing keys in JSON."""
    df = parse_weather_response_to_dataframe(mock_weather_response_missing_keys, mock_location)
    assert isinstance(df, pd.DataFrame)
    assert df.empty


# Renamed fixture to temp_duckdb_path and updated the test to use it
def test_save_weather_to_duckdb_success(temp_duckdb_path):
    """Tests if save_weather_to_duckdb saves data correctly."""
    db_path = temp_duckdb_path
    table_name = "test_weather_data"

    data = {
        'latitude': [10.1],
        'longitude': [10.2],
        'fetch_timestamp_utc': [datetime.datetime.now(datetime.timezone.utc)],
        'location_name': ['Test Place'],
        'temperature_celsius': [25.0],
        'weather_description': ['Sunny'],
        'weather_icon': ['sunny'],
    }
    df_to_save = pd.DataFrame(data)

    save_weather_to_duckdb(df_to_save, db_path, table_name)

    with duckdb.connect(database=db_path, read_only=True) as con:
        tables = con.execute("SHOW TABLES").fetchall()
        assert (table_name,) in tables

        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        assert count == 1

        df_read = con.execute(f"SELECT * FROM {table_name}").fetchdf()

        # --- Convert timestamp columns to a common format before comparison ---
        # Convert both to UTC with microsecond precision
        df_to_save['fetch_timestamp_utc'] = df_to_save['fetch_timestamp_utc'].dt.tz_convert('UTC').astype('datetime64[us, UTC]')
        df_read['fetch_timestamp_utc'] = df_read['fetch_timestamp_utc'].dt.tz_convert('UTC').astype('datetime64[us, UTC]')
        # ---------------------------------------------------------------------

        pd.testing.assert_frame_equal(
            df_to_save.reset_index(drop=True),
            df_read.reset_index(drop=True),
            # check_dtype=True is now implicitly handled by converting dtypes first
            # check_exact=True # Use check_exact=True after dtype conversion for precise value comparison
        )


# Renamed fixture to temp_duckdb_path and updated the test to use it
def test_save_weather_to_duckdb_empty_df(temp_duckdb_path):
    """Tests if save_weather_to_duckdb handles empty DataFrame."""
    db_path = temp_duckdb_path
    table_name = "test_weather_data_empty"
    df_to_save = pd.DataFrame()

    save_weather_to_duckdb(df_to_save, db_path, table_name)

    # Verify the table was NOT created as df is empty
    # Check if the database file was even created (it shouldn't be if save_weather_to_duckdb exits early)
    assert not os.path.exists(db_path)

    # If the file *was* created (which indicates an issue), then connect and check the table
    if os.path.exists(db_path):
         try:
            with duckdb.connect(database=db_path, read_only=True) as con:
                tables = con.execute("SHOW TABLES").fetchall()
                assert (table_name,) not in tables
         except duckdb.duckdb.IOException as e:
             print(f"\nWarning: IO Error when trying to connect to check empty db: {e}")
         except Exception as e:
              print(f"\nWarning: Unexpected error checking empty db: {e}")
