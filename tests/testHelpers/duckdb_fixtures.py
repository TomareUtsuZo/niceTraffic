# tests/testHelpers/duckdb_fixtures.py
import pytest
import duckdb
import os
import tempfile
import pandas as pd
import datetime

# --- Fixtures for Database Connection and File ---

@pytest.fixture(scope="function")
def temp_duckdb_file():
    """Provides a path to a temporary DuckDB file."""
    # Use TemporaryDirectory to ensure cleanup
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_database.duckdb")
        # DuckDB creates the file on connect if it doesn't exist
        yield db_path
    # TemporaryDirectory handles cleanup automatically


@pytest.fixture(scope="function")
def temp_duckdb_con(temp_duckdb_file):
    """Provides a connection to a temporary DuckDB in-memory database or file."""
    # Using a file path from temp_duckdb_file fixture
    con = duckdb.connect(database=temp_duckdb_file, read_only=False)
    yield con
    con.close() # Ensure connection is closed after the test


# --- Schema Definitions (Helpers or Constants) ---

# Define your table schemas here as SQL strings
TRAFFIC_FLOW_SCHEMA_SQL = """
CREATE TABLE traffic_flow_data (
    frc VARCHAR,
    currentSpeed INTEGER,
    freeFlowSpeed INTEGER,
    currentTravelTime INTEGER,
    freeFlowTravelTime INTEGER,
    confidence DOUBLE,
    roadClosure BOOLEAN,
    point VARCHAR,
    extraction_timestamp TIMESTAMP
)
"""

WEATHER_SCHEMA_SQL = """
CREATE TABLE weather_data (
    latitude DOUBLE,
    longitude DOUBLE,
    fetch_timestamp_utc TIMESTAMP,
    location_name VARCHAR,
    temperature_celsius DOUBLE,
    weather_description VARCHAR,
    weather_icon VARCHAR
)
"""

# You could also have functions that return schemas if they are dynamic
# def get_traffic_schema_sql():
#     return """..."""


# --- Common Table Fixtures ---

@pytest.fixture(scope="function")
def traffic_table(temp_duckdb_con):
    """Creates the traffic_flow_data table in the test database."""
    con = temp_duckdb_con
    con.execute(TRAFFIC_FLOW_SCHEMA_SQL)
    return con # Return the connection for use in tests

@pytest.fixture(scope="function")
def weather_table(temp_duckdb_con):
    """Creates the weather_data table in the test database."""
    con = temp_duckdb_con
    con.execute(WEATHER_SCHEMA_SQL)
    return con # Return the connection for use in tests

@pytest.fixture(scope="function")
def traffic_table_with_data(traffic_table):
    """Creates the traffic_flow_data table and inserts sample data."""
    con = traffic_table # Uses the traffic_table fixture to create the table first

    # Sample data for the traffic table
    sample_data = {
        'frc': ['FRC0', 'FRC1'],
        'currentSpeed': [50, 30],
        'freeFlowSpeed': [60, 40],
        'currentTravelTime': [120, 200],
        'freeFlowTravelTime': [100, 150],
        'confidence': [1.0, 0.9],
        'roadClosure': [False, True],
        'point': ['10.0,20.0', '11.0,21.0'],
        'extraction_timestamp': [datetime.datetime.now(), datetime.datetime.now() + datetime.timedelta(minutes=5)]
    }
    sample_df = pd.DataFrame(sample_data)

    # Insert the sample data
    con.execute("INSERT INTO traffic_flow_data SELECT * FROM sample_df")

    return con # Return the connection


# --- Database Interaction Helper Functions ---

def table_exists(con, table_name):
    """Checks if a table exists in the database."""
    result = con.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()
    return result[0] > 0

def get_row_count(con, table_name):
    """Gets the number of rows in a table."""
    if not table_exists(con, table_name):
        return 0
    result = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()
    return result[0] if result else 0 # Return 0 if table exists but is empty

def get_table_data(con, table_name):
    """Fetches all data from a table as a Pandas DataFrame."""
    if not table_exists(con, table_name):
        return pd.DataFrame() # Return empty DataFrame if table doesn't exist
    return con.execute(f"SELECT * FROM {table_name}").fetchdf()

# Add more helper functions as needed, e.g.,
# def insert_traffic_data(con, df):
#     """Inserts a DataFrame into the traffic_flow_data table."""
#     con.execute("INSERT INTO traffic_flow_data SELECT * FROM df")