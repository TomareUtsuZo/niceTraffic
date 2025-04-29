# extract_load_traffic_duckdb.py 

import os
import datetime
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
import traceback
import duckdb

# --- Configuration Loading ---
# Load environment variables from a .env file
load_dotenv()

# --- Configuration Settings ---
# Define key configuration parameters for the script
CONFIG = {
    "TOMTOM_API_KEY": os.getenv("TOMTOM_API_KEY"),
    "TOMTOM_TRAFFIC_API_BASE_URL": "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute",

    # Example geographic POINTS (latitude, longitude) for data extraction
    "ROUTE_POINTS_EXAMPLE": [
        "10.79187,106.68831", # Near Cach Mang Thang Tam, Vietnam
        "10.78792,106.70215", # Middle stretch, Vietnam
        "10.79096,106.71560"  # Near bridge/boundary, Vietnam
    ],

    "api_timeout_seconds": 10, # Timeout for API requests

    # DuckDB Database Configuration
    "DUCKDB_DATABASE": os.getenv("DUCKDB_DATABASE", "traffic_data.duckdb"), # Path to the DuckDB file
    "TRAFFIC_TABLE_NAME": "traffic_flow_data", # Name of the table in DuckDB
}

# --- Initial Environment Validation ---
# Check if the essential API key is set
if not CONFIG["TOMTOM_API_KEY"]:
    raise ValueError("TOMTOM_API_KEY environment variable not set. Please set it in your .env file.")


# --- API Interaction Functions ---

def construct_api_url(point_lat_lon_str, zoom=10, format='xml', **kwargs):
    """
    Constructs the TomTom Traffic API URL for the /flowSegmentData/absolute endpoint.

    Args:
        point_lat_lon_str (str): Geographic point coordinate string (latitude,longitude in degrees).
        zoom (int): Zoom level for the traffic data (unitless).
        format (str): Data format requested from the API (e.g., 'xml', 'json').
        **kwargs: Additional query parameters for the API.

    Returns:
        str: The complete constructed API URL including API key and parameters.
    """
    base = CONFIG["TOMTOM_TRAFFIC_API_BASE_URL"]
    key = CONFIG["TOMTOM_API_KEY"]
    url = f"{base}/{zoom}/{format}"

    # Build query parameters string
    query_params = f"key={key}&point={point_lat_lon_str}" # Point is lat,lon string
    for param, value in kwargs.items():
        query_params += f"&{param}={value}"

    full_url = f"{url}?{query_params}"
    print(f"Constructed URL: {full_url}")
    return full_url


def fetch_data_from_api(url):
    """
    Fetches data from the given API URL using a GET request.

    Args:
        url (str): The API endpoint URL to fetch data from.

    Returns:
        str or None: The raw response text (XML) if successful, None otherwise.
        API timeout is controlled by CONFIG['api_timeout_seconds'].
    """
    print(f"🌐 Fetching data from: {url} (Timeout: {CONFIG['api_timeout_seconds']} seconds)")
    try:
        response = requests.get(url=url, timeout=CONFIG["api_timeout_seconds"])
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        return response.text

    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch data from API: {e}")
        return None


# --- Data Transformation (Parsing) Function ---

def parse_traffic_response_to_dataframe(xml_data):
    """
    Parses the XML response from the TomTom Traffic API into a pandas DataFrame.
    Extracts key traffic flow metrics.

    Args:
        xml_data (str): The raw XML data received from the API.

    Returns:
        pd.DataFrame: A DataFrame containing the parsed traffic data, or an empty DataFrame if parsing fails or no data.
        Column units: currentSpeed, freeFlowSpeed (km/h); currentTravelTime, freeFlowTravelTime (seconds per segment).
        (Coordinate data is intentionally excluded as per previous refactoring).
    """
    if not xml_data:
        print("No XML data provided for parsing.")
        return pd.DataFrame()

    records = []
    try:
        root = ET.fromstring(xml_data)
        segment_data = {}

        # Define the XML tags for the scalar traffic metrics to extract
        scalar_tags = ['frc', 'currentSpeed', 'freeFlowSpeed', 'currentTravelTime',
                       'freeFlowTravelTime', 'confidence', 'roadClosure']

        # Extract data for each scalar tag
        for tag in scalar_tags:
            element = root.find(tag)
            segment_data[tag] = element.text if element is not None else None

        # Append the extracted data for this segment as a record
        records.append(segment_data)

        # Create DataFrame from the extracted records
        df = pd.DataFrame(records)
        print(f"Created DataFrame with {df.shape[0]} rows and {df.shape[1]} columns after parsing.")
        print(f"DataFrame column units: currentSpeed, freeFlowSpeed (km/h); currentTravelTime, freeFlowTravelTime (seconds per segment).")

        # Convert numeric columns to appropriate types, coercing errors
        numeric_cols = ['currentSpeed', 'freeFlowSpeed', 'currentTravelTime', 'freeFlowTravelTime', 'confidence']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Convert roadClosure to boolean
        if 'roadClosure' in df.columns:
            df['roadClosure'] = df['roadClosure'].astype(str).str.lower().apply(lambda x: True if x == 'true' else False)

        print(f"Successfully parsed data for {len(df)} record(s).")
        return df

    except ET.ParseError as e:
        print(f"❌ Error parsing XML response: {e}")
        traceback.print_exc()
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error processing parsed XML data: {e}")
        traceback.print_exc()
        return pd.DataFrame()


# --- Data Loading Function (DuckDB) ---

def load_dataframe_to_duckdb(con, df: pd.DataFrame, table_name: str, point_identifier: str):
    """
    Loads a pandas DataFrame into a DuckDB table.
    Adds 'point' and 'extraction_timestamp' metadata columns.
    Creates the table if it does not exist, inferring schema from the DataFrame.

    Args:
        con: Active DuckDB connection object.
        df (pd.DataFrame): The DataFrame containing data to load.
        table_name (str): The name of the target table in the DuckDB database.
        point_identifier (str): The 'lat,lon' string identifying the geographic point.
    """
    if df.empty:
        print("🚫 DataFrame is empty, skipping load to DuckDB.")
        return

    print(f"Attempting to load data into DuckDB table '{table_name}'...")

    # Add metadata columns to the DataFrame before loading
    df['point'] = point_identifier # Geographic point identifier
    df['extraction_timestamp'] = datetime.datetime.now() # Timestamp of data extraction

    try:
        # Check if the target table exists in DuckDB
        table_exists = False
        try:
            # Use PRAGMA table_info to check for table existence without erroring if not found
            con.execute(f"PRAGMA table_info('{table_name}')")
            table_exists = True
            print(f"Table '{table_name}' already exists.")
        except duckdb.CatalogException:
            # Table does not exist, proceed to create it
            print(f"Table '{table_name}' does not exist. Creating table...")
            # Create the table schema based on the DataFrame structure but insert no data (WHERE 1=0)
            # This ensures the table has the correct columns as per the current DataFrame structure
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df WHERE 1=0;")
            print(f"Table '{table_name}' created.")
            table_exists = True # Table is now created

        # If table creation failed or it didn't exist and couldn't be created
        if not table_exists:
             print(f"❌ Failed to create or find table '{table_name}'. Cannot insert data.")
             return

        # Insert data from the DataFrame into the table
        # This relies on column names matching between the DataFrame and the table schema
        con.execute(f"INSERT INTO {table_name} SELECT * FROM df")

        print(f"✅ Successfully loaded {len(df)} row(s) into '{table_name}'.")

    except duckdb.Error as e:
        print(f"❌ DuckDB Error loading data: {e}")
        traceback.print_exc()
    except Exception as e:
        print(f"❌ An unexpected error occurred during DuckDB load: {e}")
        traceback.print_exc()


# --- Main ETL Orchestration Function ---

def extract_and_load_traffic_data(con, points_to_process):
    """
    Orchestrates the ETL process: Fetches traffic data for specified points,
    parses it, and loads it into the DuckDB database.

    Args:
        con: Active DuckDB connection object.
        points_to_process (list): A list of geographic point strings (latitude,longitude)
                                  for which to extract traffic data.

    Returns:
        bool: True if data for at least one point was successfully extracted and loaded,
              False otherwise.
    """
    success_count = 0 # Track how many points were successfully processed

    if not points_to_process:
        print("No points specified for extraction.")
        return False

    print(f"\n--- Starting ETL (Extract & Load) for {len(points_to_process)} point(s) ---")

    # Process each point iteratively
    for point_identifier in points_to_process:
        try:
            # E: Extract - Construct URL and fetch raw data
            api_url = construct_api_url(point_lat_lon_str=point_identifier, zoom=10, format='xml')
            print(f"\nProcessing point: {point_identifier}")

            xml_data = fetch_data_from_api(api_url)

            if xml_data:
                # T: Transform - Parse raw XML into a DataFrame
                df = parse_traffic_response_to_dataframe(xml_data)

                if not df.empty:
                    # L: Load - Load the DataFrame into DuckDB
                    load_dataframe_to_duckdb(con, df, CONFIG["TRAFFIC_TABLE_NAME"], point_identifier)
                    success_count += 1 # Increment success count
                else:
                    print(f"No data or failed to parse data for point: {point_identifier}.")
            else:
                print(f"Failed to fetch data for point: {point_identifier}.")

        except Exception as e:
            # Catch any unexpected errors during the processing of a single point
            print(f"❌ An unexpected error occurred while processing point {point_identifier}: {e}")
            traceback.print_exc()
            continue # Continue to the next point even if one fails

    print("\n✅ ETL Extract & Load phase completed.")
    return success_count > 0 # Return True if at least one point was successful


# --- Main Execution Block ---
# This block runs when the script is executed directly
if __name__ == "__main__":
    print("Running extract_load_traffic_duckdb.py directly...")

    # Establish DuckDB Connection
    # The database file will be created if it doesn't exist at the specified path
    duckdb_con = None
    try:
        print(f"Attempting to connect to DuckDB database: {CONFIG['DUCKDB_DATABASE']}")
        # Connect with read_only=False to allow writing data
        duckdb_con = duckdb.connect(database=CONFIG["DUCKDB_DATABASE"], read_only=False)
        print("✅ DuckDB connection successful.")

        # --- Execute the main ETL process ---
        # This calls the function that fetches, parses, and loads the data
        etl_successful = extract_and_load_traffic_data(duckdb_con, CONFIG['ROUTE_POINTS_EXAMPLE'])

        if etl_successful:
            print("\n--- ETL Process Verification ---")
            try:
                # Optional: Query DuckDB to show loaded data and count for verification
                table_name = CONFIG['TRAFFIC_TABLE_NAME']
                print(f"Querying first 5 rows from '{table_name}':")
                # Fetch results as a pandas DataFrame for easy viewing
                result_df = duckdb_con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
                print(result_df)

                count_result = duckdb_con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                if count_result:
                     print(f"\nTotal rows currently in '{table_name}': {count_result[0]}")

            except duckdb.CatalogException:
                print(f"❌ Table '{table_name}' does not exist in DuckDB yet after ETL.")
            except duckdb.Error as e:
                print(f"❌ Error querying DuckDB after ETL: {e}")
            except Exception as e:
                 print(f"❌ An unexpected error occurred during verification: {e}")
        else:
            print("\n❌ ETL process did not complete successfully for any points.")


    except duckdb.Error as e:
        print(f"❌ Failed to connect to DuckDB: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred during script execution: {e}")
    finally:
        # Ensure the DuckDB connection is closed
        if duckdb_con:
            # Commit any pending transactions before closing to save data
            duckdb_con.commit()
            duckdb_con.close()
            print("\n✅ DuckDB connection closed.")

