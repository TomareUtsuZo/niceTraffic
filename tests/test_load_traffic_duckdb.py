# tests/test_load_traffic_duckdb.py

import pytest
import duckdb
import pandas as pd
import os
import sys
from unittest.mock import patch, MagicMock
import datetime
import traceback

# You no longer need the path setup here if it's in conftest.py

# Import load_dataframe_to_duckdb (ensure path is correct, potentially via conftest path setup)
try:
    from ELTscripts.load_traffic_duckdb import load_dataframe_to_duckdb
except ImportError as e:
    pytest.fail(f"Failed to import load_traffic_duckdb from ELTscripts. Check the path and if there are other import issues. Error: {e}")

# Import helper functions from duckdb_fixtures.py
# Ensure the path is correct based on your project structure
from tests.testHelpers.duckdb_fixtures import (
    table_exists,
    get_row_count,
    get_table_data, # Also import get_table_data if you used it
    # Import any other helpers/fixtures you need directly here if not relying solely on conftest auto-discovery
    # e.g., temp_duckdb_con, traffic_table, etc.
)


TEST_TABLE_NAME = "test_traffic_flow_data"

# Test functions now request the necessary fixtures

def test_load_dataframe_to_duckdb_empty_dataframe(temp_duckdb_con):
    """Test loading an empty DataFrame should not create a table or load data."""
    con = temp_duckdb_con
    point_identifier = "10.0,20.0"

    empty_data = {
        'frc': [], 'currentSpeed': [], 'freeFlowSpeed': [], 'currentTravelTime': [],
        'freeFlowTravelTime': [], 'confidence': [], 'roadClosure': []
    }
    df = pd.DataFrame(empty_data)

    load_dataframe_to_duckdb(con, df, TEST_TABLE_NAME, point_identifier)

    # Use the helper function to check if the table exists
    assert not table_exists(con, TEST_TABLE_NAME), f"Table '{TEST_TABLE_NAME}' should not exist for empty DataFrame."

    print("test_load_dataframe_to_duckdb_empty_dataframe passed.")


def test_load_dataframe_to_duckdb_create_new_table(temp_duckdb_con):
    """Test loading data when the table does not exist, should create the table."""
    con = temp_duckdb_con
    point_identifier = "10.0,20.0"

    data = {
        'frc': ['FRC0'], 'currentSpeed': [50], 'freeFlowSpeed': [60],
        'currentTravelTime': [120], 'freeFlowTravelTime': [100],
        'confidence': [1.0], 'roadClosure': [False]
    }
    df = pd.DataFrame(data)

    fixed_timestamp = datetime.datetime(2023, 1, 1, 12, 0, 0)
    with patch('ELTscripts.load_traffic_duckdb.datetime') as mock_datetime_module:
        mock_datetime_module.datetime.now.return_value = fixed_timestamp

        load_dataframe_to_duckdb(con, df, TEST_TABLE_NAME, point_identifier)

    # Use the helper function to check if the table exists
    assert table_exists(con, TEST_TABLE_NAME), f"Table '{TEST_TABLE_NAME}' should have been created."

    # Use the helper function to get row count and data
    assert get_row_count(con, TEST_TABLE_NAME) == 1, "DataFrame should contain exactly one row after load."
    loaded_df = get_table_data(con, TEST_TABLE_NAME)

    assert loaded_df['frc'].iloc[0] == 'FRC0'
    assert loaded_df['currentSpeed'].iloc[0] == 50
    assert loaded_df['point'].iloc[0] == point_identifier
    assert (loaded_df['extraction_timestamp'].iloc[0] - fixed_timestamp).total_seconds() < 1

    print("test_load_dataframe_to_duckdb_create_new_table passed.")


def test_load_dataframe_to_duckdb_append_to_existing_table(traffic_table):
    """Test loading data when the table already exists, should append data."""
    # Request the traffic_table fixture to automatically create the table
    con = traffic_table

    point_identifier_1 = "10.0,20.0"
    point_identifier_2 = "11.0,21.0"

    # Insert initial data using the connection from the fixture
    initial_data = {
        'frc': ['FRC0'], 'currentSpeed': [50], 'freeFlowSpeed': [60],
        'currentTravelTime': [120], 'freeFlowTravelTime': [100],
        'confidence': [1.0], 'roadClosure': [False],
        'point': [point_identifier_1], 'extraction_timestamp': [datetime.datetime.now()]
    }
    initial_df = pd.DataFrame(initial_data)
    con.execute("INSERT INTO traffic_flow_data SELECT * FROM initial_df") # Use the table name created by the fixture
    print(f"Created initial table '{con.execute('SHOW TABLES').fetchone()[0]}' with 1 row.")


    new_data = {
        'frc': ['FRC1'], 'currentSpeed': [30], 'freeFlowSpeed': [40],
        'currentTravelTime': [200], 'freeFlowTravelTime': [150],
        'confidence': [0.9], 'roadClosure': [True]
    }
    new_df = pd.DataFrame(new_data)

    fixed_timestamp_2 = datetime.datetime(2023, 1, 1, 12, 5, 0)
    with patch('ELTscripts.load_traffic_duckdb.datetime') as mock_datetime_module:
        mock_datetime_module.datetime.now.return_value = fixed_timestamp_2

        # Load the new data into the existing table
        load_dataframe_to_duckdb(con, new_df, con.execute('SHOW TABLES').fetchone()[0], point_identifier_2) # Use table name from fixture

    # Use helper function to verify total rows
    assert get_row_count(con, con.execute('SHOW TABLES').fetchone()[0]) == 2, "Table should contain 2 rows after appending."

    # Use helper function to get specific data
    loaded_df_new = con.execute(f"SELECT * FROM {con.execute('SHOW TABLES').fetchone()[0]} WHERE point = '{point_identifier_2}'").fetchdf()

    assert len(loaded_df_new) == 1
    assert loaded_df_new['frc'].iloc[0] == 'FRC1'
    assert loaded_df_new['currentSpeed'].iloc[0] == 30
    assert loaded_df_new['point'].iloc[0] == point_identifier_2
    assert (loaded_df_new['extraction_timestamp'].iloc[0] - fixed_timestamp_2).total_seconds() < 1

    print("test_load_dataframe_to_duckdb_append_to_existing_table passed.")


def test_load_dataframe_to_duckdb_column_mismatch(temp_duckdb_con, capsys):
    """Test loading a DataFrame with columns that don't match the existing table."""
    con = temp_duckdb_con
    initial_table_name = "mismatch_test_table"

    # Create the table manually here if its schema is specific to this test
    # Or create a specific fixture for this schema in duckdb_fixtures.py
    initial_schema_sql = """
    CREATE TABLE mismatch_test_table (
        frc VARCHAR,
        currentSpeed INTEGER,
        point VARCHAR,
        extraction_timestamp TIMESTAMP
    )
    """
    con.execute(initial_schema_sql)

    initial_data = {
        'frc': ['FRC0'], 'currentSpeed': [50],
        'point': ["10.0,20.0"], 'extraction_timestamp': [datetime.datetime.now()]
    }
    initial_df = pd.DataFrame(initial_data)
    con.execute(f"INSERT INTO {initial_table_name} SELECT * FROM initial_df")
    print(f"Created initial table '{initial_table_name}' with subset of columns.")


    new_data = {
        'frc': ['FRC1'],
        'freeFlowSpeed': [40],
        'extra_col': ['test']
    }
    new_df = pd.DataFrame(new_data)

    fixed_timestamp = datetime.datetime(2023, 1, 1, 12, 10, 0)
    with patch('ELTscripts.load_traffic_duckdb.datetime') as mock_datetime_module:
        mock_datetime_module.datetime.now.return_value = fixed_timestamp

        load_dataframe_to_duckdb(con, new_df, initial_table_name, "11.0,21.0") # Use correct table name

    # Use helper function to verify row count hasn't changed
    assert get_row_count(con, initial_table_name) == 1, "Number of rows should not change due to column mismatch."

    captured = capsys.readouterr()
    assert "DuckDB Error loading data" in captured.out or "DuckDB Error loading data" in captured.err, \
        "Should print a DuckDB Error message due to column mismatch."

    print("test_load_dataframe_to_duckdb_column_mismatch passed.")