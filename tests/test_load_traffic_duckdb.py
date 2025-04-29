import pytest
import duckdb
import pandas as pd
import os
import sys
from unittest.mock import patch, MagicMock
import datetime
import traceback # Import traceback for potential debugging in tests

# --- Path Setup ---
# Get the absolute path of the directory where pytest is being run (project root)
# and add it to sys.path so modules like load_traffic_duckdb can be found.
# This is a robust approach when tests are in a subdirectory.
project_root = os.path.abspath(os.getcwd())
if project_root not in sys.path:
    sys.path.insert(0, project_root)
    # print(f"Added {project_root} to sys.path") # Optional: for debugging path issues

# Import the function from the original script
# Assuming load_traffic_duckdb.py is in the project root directory
try:
    from load_traffic_duckdb import load_dataframe_to_duckdb
    # print("Successfully imported load_traffic_duckdb module.") # Optional: for debugging import
except ImportError as e:
    # If import still fails, provide a more specific error message
    pytest.fail(f"Failed to import load_traffic_duckdb. Check if the file exists in the project root ({project_root}) and if there are other import issues. Error: {e}")

# Use a temporary table name for the DuckDB database during tests
TEST_TABLE_NAME = "test_traffic_flow_data"

@pytest.fixture(scope="function")
def in_memory_duckdb_con():
    """Fixture to provide a temporary in-memory DuckDB connection for each test."""
    # Using ':memory:' creates a database in RAM that is destroyed after the connection closes
    print("\nSetting up in-memory DuckDB connection...")
    con = duckdb.connect(database=':memory:', read_only=False)
    yield con
    # Close the connection after the test function finishes
    print("\nTearing down in-memory DuckDB connection...")
    con.close()

# --- Tests for load_dataframe_to_duckdb ---

def test_load_dataframe_to_duckdb_empty_dataframe(in_memory_duckdb_con):
    """Test loading an empty DataFrame should not create a table or load data."""
    con = in_memory_duckdb_con
    point_identifier = "10.0,20.0"

    # Create an empty DataFrame with expected columns (schema matters even if empty)
    empty_data = {
        'frc': [], 'currentSpeed': [], 'freeFlowSpeed': [], 'currentTravelTime': [],
        'freeFlowTravelTime': [], 'confidence': [], 'roadClosure': []
    }
    df = pd.DataFrame(empty_data)

    # Call the function with the empty DataFrame
    load_dataframe_to_duckdb(con, df, TEST_TABLE_NAME, point_identifier)

    # Verify the table was NOT created
    tables = con.execute("SHOW TABLES").fetchall()
    assert (TEST_TABLE_NAME,) not in tables, f"Table '{TEST_TABLE_NAME}' should not exist for empty DataFrame."

    print("test_load_dataframe_to_duckdb_empty_dataframe passed.")


def test_load_dataframe_to_duckdb_create_new_table(in_memory_duckdb_con):
    """Test loading data when the table does not exist, should create the table."""
    con = in_memory_duckdb_con
    point_identifier = "10.0,20.0"

    # Create a sample DataFrame
    data = {
        'frc': ['FRC0'],
        'currentSpeed': [50],
        'freeFlowSpeed': [60],
        'currentTravelTime': [120],
        'freeFlowTravelTime': [100],
        'confidence': [1.0],
        'roadClosure': [False]
    }
    df = pd.DataFrame(data)

    # Mock datetime.datetime.now to return a fixed timestamp for predictable testing
    fixed_timestamp = datetime.datetime(2023, 1, 1, 12, 0, 0)
    # Patch datetime.datetime within the load_traffic_duckdb module
    with patch('load_traffic_duckdb.datetime') as mock_datetime_module:
        mock_datetime_module.datetime.now.return_value = fixed_timestamp

        # Call the function to load data
        load_dataframe_to_duckdb(con, df, TEST_TABLE_NAME, point_identifier)

    # Verify the table was created
    tables = con.execute("SHOW TABLES").fetchall()
    assert (TEST_TABLE_NAME,) in tables, f"Table '{TEST_TABLE_NAME}' should have been created."

    # Verify the data was loaded correctly, including added columns
    loaded_df = con.execute(f"SELECT * FROM {TEST_TABLE_NAME}").fetchdf()

    assert len(loaded_df) == 1, "DataFrame should contain exactly one row after load."
    assert loaded_df['frc'].iloc[0] == 'FRC0'
    assert loaded_df['currentSpeed'].iloc[0] == 50
    assert loaded_df['point'].iloc[0] == point_identifier
    # Check if the timestamp is close to the fixed timestamp (allow for minor differences)
    assert (loaded_df['extraction_timestamp'].iloc[0] - fixed_timestamp).total_seconds() < 1, "Timestamp should match the mocked time."

    print("test_load_dataframe_to_duckdb_create_new_table passed.")


def test_load_dataframe_to_duckdb_append_to_existing_table(in_memory_duckdb_con):
    """Test loading data when the table already exists, should append data."""
    con = in_memory_duckdb_con
    point_identifier_1 = "10.0,20.0"
    point_identifier_2 = "11.0,21.0"

    # Create the table with initial data using the connection directly
    initial_data = {
        'frc': ['FRC0'],
        'currentSpeed': [50],
        'freeFlowSpeed': [60],
        'currentTravelTime': [120],
        'freeFlowTravelTime': [100],
        'confidence': [1.0],
        'roadClosure': [False],
        'point': [point_identifier_1],
        'extraction_timestamp': [datetime.datetime.now()] # Use real time for initial data
    }
    initial_df = pd.DataFrame(initial_data)
    con.execute(f"CREATE TABLE {TEST_TABLE_NAME} AS SELECT * FROM initial_df")
    print(f"Created initial table '{TEST_TABLE_NAME}' with 1 row.")


    # Create a new DataFrame to append
    new_data = {
        'frc': ['FRC1'],
        'currentSpeed': [30],
        'freeFlowSpeed': [40],
        'currentTravelTime': [200],
        'freeFlowTravelTime': [150],
        'confidence': [0.9],
        'roadClosure': [True]
    }
    new_df = pd.DataFrame(new_data)

    # Mock datetime.datetime.now for the second load
    fixed_timestamp_2 = datetime.datetime(2023, 1, 1, 12, 5, 0)
    with patch('load_traffic_duckdb.datetime') as mock_datetime_module:
        mock_datetime_module.datetime.now.return_value = fixed_timestamp_2

        # Call the function to load the new data
        load_dataframe_to_duckdb(con, new_df, TEST_TABLE_NAME, point_identifier_2)

    # Verify the total number of rows
    total_rows = con.execute(f"SELECT COUNT(*) FROM {TEST_TABLE_NAME}").fetchone()[0]
    assert total_rows == 2, f"Table '{TEST_TABLE_NAME}' should contain 2 rows after appending."

    # Verify the new data was appended correctly
    loaded_df_new = con.execute(f"SELECT * FROM {TEST_TABLE_NAME} WHERE point = '{point_identifier_2}'").fetchdf()

    assert len(loaded_df_new) == 1, "Should have loaded exactly one new row for the second point."
    assert loaded_df_new['frc'].iloc[0] == 'FRC1'
    assert loaded_df_new['currentSpeed'].iloc[0] == 30
    assert loaded_df_new['point'].iloc[0] == point_identifier_2
    assert (loaded_df_new['extraction_timestamp'].iloc[0] - fixed_timestamp_2).total_seconds() < 1, "Timestamp for appended data should match the mocked time."

    print("test_load_dataframe_to_duckdb_append_to_existing_table passed.")


def test_load_dataframe_to_duckdb_column_mismatch(in_memory_duckdb_con, capsys):
    """Test loading a DataFrame with columns that don't match the existing table."""
    con = in_memory_duckdb_con
    point_identifier = "10.0,20.0"
    initial_table_name = "mismatch_test_table" # Use a different table name for isolation

    # Create the table with initial data (subset of columns)
    initial_data = {
        'frc': ['FRC0'],
        'currentSpeed': [50],
        'point': [point_identifier],
        'extraction_timestamp': [datetime.datetime.now()]
    }
    initial_df = pd.DataFrame(initial_data)
    con.execute(f"CREATE TABLE {initial_table_name} AS SELECT * FROM initial_df")
    print(f"Created initial table '{initial_table_name}' with subset of columns.")

    # Create a new DataFrame with different columns (missing some, adding others)
    new_data = {
        'frc': ['FRC1'],
        'freeFlowSpeed': [40], # Different column name
        'extra_col': ['test'] # Extra column
    }
    new_df = pd.DataFrame(new_data)

    # Mock datetime.datetime.now for the load attempt
    fixed_timestamp = datetime.datetime(2023, 1, 1, 12, 10, 0)
    with patch('load_traffic_duckdb.datetime') as mock_datetime_module:
        mock_datetime_module.datetime.now.return_value = fixed_timestamp

        # Call the function - this should ideally print an error message
        load_dataframe_to_duckdb(con, new_df, initial_table_name, point_identifier)

    # Verify the number of rows in the table hasn't changed
    total_rows_after_attempt = con.execute(f"SELECT COUNT(*) FROM {initial_table_name}").fetchone()[0]
    assert total_rows_after_attempt == 1, "Number of rows should not change due to column mismatch."

    # Optionally, check if an error message was printed (requires capsys fixture)
    captured = capsys.readouterr()
    assert "DuckDB Error loading data" in captured.out or "DuckDB Error loading data" in captured.err, \
        "Should print a DuckDB Error message due to column mismatch."

    print("test_load_dataframe_to_duckdb_column_mismatch passed.")

