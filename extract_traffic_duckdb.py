# extract_traffic_duckdb.py - Refactored for Readability

import os
import datetime
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
import traceback
# Removed duckdb import as it's no longer used directly here

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

    # DuckDB Database Configuration (still needed for table name in main ETL script)
    "DUCKDB_DATABASE": os.getenv("DUCKDB_DATABASE", "traffic_data.duckdb"), # Path to the DuckDB file
    "TRAFFIC_TABLE_NAME": "traffic_flow_data", # Name of the table in DuckDB
}

# --- Initial Environment Validation ---
# Check if the essential API key is set
if not CONFIG["TOMTOM_API_KEY"]:
    raise ValueError("TOMTOM_API_KEY environment variable not set. Please set it in your .env file.")


# --- API Interaction Functions (Extract) ---

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


# --- Main Extraction and Transformation Orchestration Function ---

def extract_and_transform_traffic_data(points_to_process):
    """
    Orchestrates the Extraction and Transformation process:
    Fetches traffic data for specified points and parses it into DataFrames.

    Args:
        points_to_process (list): A list of geographic point strings (latitude,longitude)
                                  for which to extract traffic data.

    Returns:
        list: A list of pandas DataFrames, one for each successfully processed point,
              or an empty list if no data was extracted/transformed.
    """
    processed_dataframes = []

    if not points_to_process:
        print("No points specified for extraction.")
        return processed_dataframes

    print(f"\n--- Starting Extract & Transform for {len(points_to_process)} point(s) ---")

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
                    # Add point identifier as a column here, before returning the DataFrame
                    # This keeps the point context with the data
                    df['point'] = point_identifier
                    df['extraction_timestamp'] = datetime.datetime.now() # Add timestamp here too
                    processed_dataframes.append(df)
                else:
                    print(f"No data or failed to parse data for point: {point_identifier}.")
            else:
                print(f"Failed to fetch data for point: {point_identifier}.")

        except Exception as e:
            # Catch any unexpected errors during the processing of a single point
            print(f"❌ An unexpected error occurred while processing point {point_identifier}: {e}")
            traceback.print_exc()
            continue # Continue to the next point even if one fails

    print("\n✅ Extract & Transform phase completed.")
    return processed_dataframes


# --- Main Execution Block ---
# This block runs when the script is executed directly
if __name__ == "__main__":
    print("Running extract_transform_traffic.py directly...")

    # This script now only performs Extraction and Transformation.
    # The loading step would be done by a separate script (like an ETL orchestrator)
    # that imports and uses extract_and_transform_traffic_data and load_dataframe_to_duckdb.

    print("\nNote: This script performs Extraction and Transformation only.")
    print("Use a separate script (e.g., an ETL orchestrator) to call this function")
    print("and then load the returned DataFrames into your database.")

    # Example of how you might use it (without actual loading here):
    # extracted_dfs = extract_and_transform_traffic_data(CONFIG['ROUTE_POINTS_EXAMPLE'])
    # print(f"\nExtracted and transformed {len(extracted_dfs)} DataFrames.")
    # if extracted_dfs:
    #     print("First DataFrame head:")
    #     print(extracted_dfs[0].head())

