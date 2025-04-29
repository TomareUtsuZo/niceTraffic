# test_extract_traffic.py

import pytest
import duckdb
import pandas as pd
import os
import sys
from unittest.mock import patch # Using unittest.mock which pytest-mock wraps

# Add the parent directory to the path so we can import the script
# This assumes test_extract_traffic.py is in a subdirectory (e.g., 'tests/')
# and extract_load_traffic_duckdb.py is in the parent directory.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the functions from your main script
# Make sure the import path matches your file structure
try:
    from extract_load_traffic_duckdb import (
        extract_and_load_traffic_data,
        CONFIG, # We might need CONFIG for table name etc.
        parse_traffic_response_to_dataframe # Also good to test parsing separately if needed, but we'll test it via the main function
    )
except ImportError as e:
    pytest.fail(f"Could not import the main script functions. Ensure extract_load_traffic_duckdb.py is in the correct path relative to the test file. Error: {e}")


# --- Sample Data ---
# This is a minimal example of the XML response we expect from the TomTom API
# based on the structure parsed by parse_traffic_response_to_dataframe.
# Updated to remove coordinates section.
SAMPLE_XML_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<flowSegmentData>
    <frc>FRC0</frc>
    <currentSpeed>55</currentSpeed>
    <freeFlowSpeed>60</freeFlowSpeed>
    <currentTravelTime>120</currentTravelTime>
    <freeFlowTravelTime>100</freeFlowTravelTime>
    <confidence>1.0</confidence>
    <roadClosure>false</roadClosure>
    </flowSegmentData>
"""

# --- Pytest Fixtures ---

@pytest.fixture(scope="function")
def in_memory_duckdb_con():
    """
    Provides a temporary in-memory DuckDB connection for each test function.
    The database is cleared after each test.
    """
    print("\nSetting up in-memory DuckDB...")
    con = duckdb.connect(database=':memory:', read_only=False)
    yield con
    print("\nTearing down in-memory DuckDB...")
    con.close()

@pytest.fixture(scope="function")
def mock_tomtom_api(mocker):
    """
    Mocks the requests.get call to the TomTom API.
    Uses pytest-mock's mocker fixture.
    """
    print("\nSetting up API mock...")
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.text = SAMPLE_XML_RESPONSE
    mock_response.raise_for_status.return_value = None # Ensure raise_for_status doesn't raise for 200

    # Patch requests.get to return our mock response
    mocker.patch('requests.get', return_value=mock_response)
    print("API mock configured.")
    yield mock_response # Yield the mock object if needed for further inspection in tests
    print("API mock torn down.")


# --- Unit Tests ---

def test_extract_and_load_success(in_memory_duckdb_con, mock_tomtom_api):
    """
    Tests the main extract_and_load_traffic_data function.
    Verifies that data is fetched, parsed, and loaded into DuckDB.
    """
    print("\nRunning test_extract_and_load_success...")

    # Use a single point for a simpler test case
    test_points = ["10.79187,106.68831"]

    # Call the main function with the in-memory connection and mocked API
    success = extract_and_load_traffic_data(in_memory_duckdb_con, test_points)

    # Assert that the function reported success
    assert success is True, "extract_and_load_traffic_data should return True on success"

    # Verify data was loaded into the DuckDB table
    table_name = CONFIG["TRAFFIC_TABLE_NAME"]
    print(f"Verifying data in table '{table_name}'...")

    try:
        # Check if the table exists and has data
        count_result = in_memory_duckdb_con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        assert count_result is not None, f"Could not get row count from table '{table_name}'"
        assert count_result[0] > 0, f"Table '{table_name}' should contain data after load"
        print(f"Table '{table_name}' contains {count_result[0]} row(s).")

        # Check if the expected columns are present
        columns_df = in_memory_duckdb_con.execute(f"PRAGMA table_info('{table_name}')").fetchdf()
        column_names = columns_df['name'].tolist()
        print(f"Columns found in table: {column_names}")

        expected_columns = [
            'frc', 'currentSpeed', 'freeFlowSpeed', 'currentTravelTime',
            'freeFlowTravelTime', 'confidence', 'roadClosure',
            'point', 'extraction_timestamp' # point and extraction_timestamp are added in load_dataframe_to_duckdb
        ]
        # Check that the number of columns matches expected plus the two added columns
        assert len(column_names) == len(expected_columns), f"Expected {len(expected_columns)} columns, but found {len(column_names)}"

        for col in expected_columns:
            assert col in column_names, f"Expected column '{col}' not found in table '{table_name}'"
        print("All expected columns found.")

    except duckdb.CatalogException:
        pytest.fail(f"Table '{table_name}' was not created in DuckDB.")
    except Exception as e:
        pytest.fail(f"An error occurred during DuckDB verification: {e}")

    print("Test test_extract_and_load_success completed successfully.")

