import pytest
import requests
import pandas as pd
import os
import sys
from unittest.mock import patch, MagicMock
import xml.etree.ElementTree as ET
import datetime # Import datetime for timestamp checks
import numpy as np # Import numpy for potential boolean type checks if needed, though == is preferred


# Import the functions from the original script
# Assuming extract_traffic_duckdb.py is in the project root directory
try:
    from ELTscripts.extract_traffic_duckdb import ( 
        construct_api_url,
        fetch_data_from_api,
        parse_traffic_response_to_dataframe,
        extract_and_transform_traffic_data,
        CONFIG # We might need CONFIG for some tests
    )
    # print("Successfully imported extract_traffic_duckdb module.") # Optional: for debugging import
except ImportError as e:
    # If import still fails, provide a more specific error message
    pytest.fail(f"Failed to import extract_traffic_duckdb from ELTscripts. Check the path and if there are other import issues. Error: {e}") # <-- Updated error message

# Define a dummy API key for testing purposes
# In a real scenario, you might mock os.getenv as well, but using a dummy here is simpler
# Ensure this doesn't accidentally use a real key
DUMMY_API_KEY = "dummy_api_key_for_testing"

# Mock the environment variable loading for tests
@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """Fixture to mock environment variables."""
    monkeypatch.setenv("TOMTOM_API_KEY", DUMMY_API_KEY)
    monkeypatch.setenv("DUCKDB_DATABASE", "test_traffic_data.duckdb") # Use a test database name
    # Reload CONFIG after setting env vars if CONFIG is loaded at import time
    # If CONFIG is only loaded within functions, this might not be strictly necessary
    # but it's good practice if CONFIG is global/module-level
    CONFIG["TOMTOM_API_KEY"] = os.getenv("TOMTOM_API_KEY")
    CONFIG["DUCKDB_DATABASE"] = os.getenv("DUCKDB_DATABASE", "traffic_data.duckdb")


# --- Tests for construct_api_url ---
def test_construct_api_url_basic():
    """Test basic URL construction with required parameters."""
    point = "10.0,20.0"
    expected_base = CONFIG["TOMTOM_TRAFFIC_API_BASE_URL"]
    expected_key = DUMMY_API_KEY
    expected_url = f"{expected_base}/10/xml?key={expected_key}&point={point}"
    assert construct_api_url(point) == expected_url

def test_construct_api_url_with_zoom_and_format():
    """Test URL construction with custom zoom and format."""
    point = "10.0,20.0"
    zoom = 12
    format = 'json'
    expected_base = CONFIG["TOMTOM_TRAFFIC_API_BASE_URL"]
    expected_key = DUMMY_API_KEY
    expected_url = f"{expected_base}/{zoom}/{format}?key={expected_key}&point={point}"
    assert construct_api_url(point, zoom=zoom, format=format) == expected_url

def test_construct_api_url_with_extra_kwargs():
    """Test URL construction with additional query parameters."""
    point = "10.0,20.0"
    extra_param = "value"
    expected_base = CONFIG["TOMTOM_TRAFFIC_API_BASE_URL"]
    expected_key = DUMMY_API_KEY
    expected_url = f"{expected_base}/10/xml?key={expected_key}&point={point}&extra_param={extra_param}"
    assert construct_api_url(point, extra_param=extra_param) == expected_url

# --- Tests for fetch_data_from_api ---
def test_fetch_data_from_api_success(mocker):
    """Test successful API data fetching."""
    mock_response = MagicMock()
    mock_response.text = "<trafficData><segment><frc>FRC0</frc></segment></trafficData>"
    mock_response.raise_for_status.return_value = None # Simulate success
    mocker.patch('requests.get', return_value=mock_response)

    url = "http://fakeapi.com/data"
    data = fetch_data_from_api(url)
    assert data == mock_response.text

def test_fetch_data_from_api_http_error(mocker):
    """Test API data fetching with HTTP error."""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
    mocker.patch('requests.get', return_value=mock_response)

    url = "http://fakeapi.com/data"
    data = fetch_data_from_api(url)
    assert data is None

def test_fetch_data_from_api_timeout(mocker):
    """Test API data fetching with timeout."""
    mocker.patch('requests.get', side_effect=requests.exceptions.Timeout)

    url = "http://fakeapi.com/data"
    data = fetch_data_from_api(url)
    assert data is None

def test_fetch_data_from_api_request_exception(mocker):
    """Test API data fetching with a general RequestException."""
    mocker.patch('requests.get', side_effect=requests.exceptions.RequestException("Some error"))

    url = "http://fakeapi.com/data"
    data = fetch_data_from_api(url)
    assert data is None


# --- Tests for parse_traffic_response_to_dataframe ---
def test_parse_traffic_response_to_dataframe_valid_xml():
    """Test parsing a valid XML response."""
    # This is a simplified example XML based on the expected structure
    xml_data = """
    <flowSegmentData>
        <frc>FRC0</frc>
        <currentSpeed>50</currentSpeed>
        <freeFlowSpeed>60</freeFlowSpeed>
        <currentTravelTime>120</currentTravelTime>
        <freeFlowTravelTime>100</freeFlowTravelTime>
        <confidence>1.0</confidence>
        <roadClosure>false</roadClosure>
    </flowSegmentData>
    """
    df = parse_traffic_response_to_dataframe(xml_data)

    assert not df.empty
    assert len(df) == 1
    assert list(df.columns) == ['frc', 'currentSpeed', 'freeFlowSpeed', 'currentTravelTime',
                                'freeFlowTravelTime', 'confidence', 'roadClosure']
    assert df['currentSpeed'].iloc[0] == 50
    # Fix: Use == for boolean comparison instead of 'is'
    assert df['roadClosure'].iloc[0] == False

def test_parse_traffic_response_to_dataframe_invalid_xml():
    """Test parsing invalid XML data."""
    xml_data = "<invalid_xml>"
    df = parse_traffic_response_to_dataframe(xml_data)
    assert df.empty

def test_parse_traffic_response_to_dataframe_empty_xml():
    """Test parsing empty XML data."""
    xml_data = ""
    df = parse_traffic_response_to_dataframe(xml_data)
    assert df.empty

def test_parse_traffic_response_to_dataframe_missing_elements():
    """Test parsing XML with missing optional elements."""
    xml_data = """
    <flowSegmentData>
        <frc>FRC1</frc>
        <currentSpeed>30</currentSpeed>
        <freeFlowSpeed>40</freeFlowSpeed>
        </flowSegmentData>
    """
    df = parse_traffic_response_to_dataframe(xml_data)

    assert not df.empty
    assert len(df) == 1
    assert list(df.columns) == ['frc', 'currentSpeed', 'freeFlowSpeed', 'currentTravelTime',
                                'freeFlowTravelTime', 'confidence', 'roadClosure']
    assert df['frc'].iloc[0] == 'FRC1'
    assert pd.isna(df['currentTravelTime'].iloc[0]) # Check if missing numeric is NaN
    # Fix: Missing roadClosure should result in False based on parsing logic
    assert df['roadClosure'].iloc[0] == False

def test_parse_traffic_response_to_dataframe_road_closure_true():
    """Test parsing XML with roadClosure as true."""
    xml_data = """
    <flowSegmentData>
        <frc>FRC0</frc>
        <currentSpeed>0</currentSpeed>
        <freeFlowSpeed>60</freeFlowSpeed>
        <currentTravelTime>999</currentTravelTime>
        <freeFlowTravelTime>100</freeFlowTravelTime>
        <confidence>1.0</confidence>
        <roadClosure>True</roadClosure> </flowSegmentData>
    """
    df = parse_traffic_response_to_dataframe(xml_data)
    assert not df.empty
    # Fix: Use == for boolean comparison instead of 'is'
    assert df['roadClosure'].iloc[0] == True

def test_parse_traffic_response_to_dataframe_road_closure_other():
    """Test parsing XML with roadClosure as something other than true/false."""
    xml_data = """
    <flowSegmentData>
        <frc>FRC0</frc>
        <currentSpeed>50</currentSpeed>
        <freeFlowSpeed>60</freeFlowSpeed>
        <currentTravelTime>120</currentTravelTime>
        <freeFlowTravelTime>100</freeFlowTravelTime>
        <confidence>1.0</confidence>
        <roadClosure>maybe</roadClosure>
    </flowSegmentData>
    """
    df = parse_traffic_response_to_dataframe(xml_data)
    assert not df.empty
    # Fix: Use == for boolean comparison instead of 'is'
    assert df['roadClosure'].iloc[0] == False # Should default to False if not 'true'


# --- Tests for extract_and_transform_traffic_data ---
def test_extract_and_transform_traffic_data_success(mocker):
    """Test the main extraction and transformation flow with successful API calls."""
    points = ["10.0,20.0", "11.0,21.0"]
    mock_xml_data_1 = """
    <flowSegmentData>
        <frc>FRC0</frc>
        <currentSpeed>50</currentSpeed>
        <freeFlowSpeed>60</freeFlowSpeed>
        <currentTravelTime>120</currentTravelTime>
        <freeFlowTravelTime>100</freeFlowTravelTime>
        <confidence>1.0</confidence>
        <roadClosure>false</roadClosure>
    </flowSegmentData>
    """
    mock_xml_data_2 = """
    <flowSegmentData>
        <frc>FRC1</frc>
        <currentSpeed>30</currentSpeed>
        <freeFlowSpeed>40</freeFlowSpeed>
        <currentTravelTime>200</currentTravelTime>
        <freeFlowTravelTime>150</freeFlowTravelTime>
        <confidence>0.9</confidence>
        <roadClosure>true</roadClosure>
    </flowSegmentData>
    """

    # Mock fetch_data_from_api to return different data for each point
    def mock_fetch(url):
        # This mock checks the URL to return the correct data
        if "point=10.0,20.0" in url:
            return mock_xml_data_1
        elif "point=11.0,21.0" in url:
            return mock_xml_data_2
        return None

    mocker.patch('ELTscripts.extract_traffic_duckdb.fetch_data_from_api', side_effect=mock_fetch) # <-- Added ELTscripts.
    # Fix: Do NOT mock construct_api_url here, let the real function generate the URL
    # mocker.patch('extract_traffic_duckdb.construct_api_url', return_value="http://fakeapi.com/url") # Removed this line

    # Mock datetime.datetime.now for predictable timestamp testing
    fixed_timestamp = datetime.datetime(2023, 1, 1, 12, 0, 0)
    # Fix: Patch the datetime module within extract_traffic_duckdb and set the return_value of datetime.datetime.now
    with patch('ELTscripts.extract_traffic_duckdb.datetime') as mock_datetime_module: 
        mock_datetime_module.datetime.now.return_value = fixed_timestamp
        # Removed mock_datetime.side_effect

        extracted_dfs = extract_and_transform_traffic_data(points)

    assert len(extracted_dfs) == 2 # Expect one DataFrame per point
    assert isinstance(extracted_dfs[0], pd.DataFrame)
    assert isinstance(extracted_dfs[1], pd.DataFrame)

    # Check content of the first DataFrame
    df1 = extracted_dfs[0]
    assert not df1.empty
    assert df1['point'].iloc[0] == "10.0,20.0"
    assert 'extraction_timestamp' in df1.columns
    assert df1['currentSpeed'].iloc[0] == 50
    assert (df1['extraction_timestamp'].iloc[0] - fixed_timestamp).total_seconds() < 1 # Check timestamp

    # Check content of the second DataFrame
    df2 = extracted_dfs[1]
    assert not df2.empty
    assert df2['point'].iloc[0] == "11.0,21.0"
    assert 'extraction_timestamp' in df2.columns
    assert df2['currentSpeed'].iloc[0] == 30
    assert df2['roadClosure'].iloc[0] == True # Fix: Use ==
    assert (df2['extraction_timestamp'].iloc[0] - fixed_timestamp).total_seconds() < 1 # Check timestamp


def test_extract_and_transform_traffic_data_api_failure(mocker):
    """Test the main flow when API fetching fails for one point."""
    points = ["10.0,20.0", "11.0,21.0"]
    mock_xml_data_2 = """
    <flowSegmentData>
        <frc>FRC1</frc>
        <currentSpeed>30</currentSpeed>
        <freeFlowSpeed>40</freeFlowSpeed>
        <currentTravelTime>200</currentTravelTime>
        <freeFlowTravelTime>150</freeFlowTravelTime>
        <confidence>0.9</confidence>
        <roadClosure>true</roadClosure>
    </flowSegmentData>
    """

    # Mock fetch_data_from_api: fail for the first point, succeed for the second
    def mock_fetch(url):
        if "point=10.0,20.0" in url:
            return None # Simulate API failure
        elif "point=11.0,21.0" in url:
            return mock_xml_data_2
        return None

    mocker.patch('ELTscripts.extract_traffic_duckdb.fetch_data_from_api', side_effect=mock_fetch) 
    # Fix: Do NOT mock construct_api_url here
    # mocker.patch('extract_traffic_duckdb.construct_api_url', return_value="http://fakeapi.com/url") # Removed this line

    # Mock datetime.datetime.now for predictable timestamp testing
    fixed_timestamp = datetime.datetime(2023, 1, 1, 12, 0, 0)
    # Fix: Patch the datetime module within extract_traffic_duckdb and set the return_value of datetime.datetime.now
    with patch('ELTscripts.extract_traffic_duckdb.datetime') as mock_datetime_module: # <-- Added ELTscripts.
        mock_datetime_module.datetime.now.return_value = fixed_timestamp
        # Removed mock_datetime.side_effect

        extracted_dfs = extract_and_transform_traffic_data(points)

    assert len(extracted_dfs) == 1 # Only the second point should yield a DataFrame
    assert extracted_dfs[0]['point'].iloc[0] == "11.0,21.0"
    assert (extracted_dfs[0]['extraction_timestamp'].iloc[0] - fixed_timestamp).total_seconds() < 1 # Check timestamp


def test_extract_and_transform_traffic_data_parsing_failure(mocker):
    """Test the main flow when XML parsing fails for one point."""
    points = ["10.0,20.0", "11.0,21.0"]
    mock_xml_data_1_invalid = "<invalid_xml>"
    mock_xml_data_2_valid = """
    <flowSegmentData>
        <frc>FRC1</frc>
        <currentSpeed>30</currentSpeed>
        <freeFlowSpeed>40</freeFlowSpeed>
        <currentTravelTime>200</currentTravelTime>
        <freeFlowTravelTime>150</freeFlowTravelTime>
        <confidence>0.9</confidence>
        <roadClosure>true</roadClosure>
    </flowSegmentData>
    """

    # Mock fetch_data_from_api to return invalid XML for the first point, valid for the second
    def mock_fetch(url):
        if "point=10.0,20.0" in url:
            return mock_xml_data_1_invalid
        elif "point=11.0,21.0" in url:
            return mock_xml_data_2_valid
        return None

    mocker.patch('ELTscripts.extract_traffic_duckdb.fetch_data_from_api', side_effect=mock_fetch) 
    # Fix: Do NOT mock construct_api_url here
    # mocker.patch('extract_traffic_duckdb.construct_api_url', return_value="http://fakeapi.com/url") # Removed this line

    # Mock datetime.datetime.now for predictable timestamp testing
    fixed_timestamp = datetime.datetime(2023, 1, 1, 12, 0, 0)
    # Fix: Patch the datetime module within extract_traffic_duckdb and set the return_value of datetime.datetime.now
    with patch('ELTscripts.extract_traffic_duckdb.datetime') as mock_datetime_module: # <-- Added ELTscripts.
        mock_datetime_module.datetime.now.return_value = fixed_timestamp
        # Removed mock_datetime.side_effect

        extracted_dfs = extract_and_transform_traffic_data(points)

    assert len(extracted_dfs) == 1 # Only the second point should yield a DataFrame
    assert extracted_dfs[0]['point'].iloc[0] == "11.0,21.0"
    assert (extracted_dfs[0]['extraction_timestamp'].iloc[0] - fixed_timestamp).total_seconds() < 1 # Check timestamp


def test_extract_and_transform_traffic_data_no_points():
    """Test the main flow with an empty list of points."""
    points = []
    extracted_dfs = extract_and_transform_traffic_data(points)
    assert len(extracted_dfs) == 0

