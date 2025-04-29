# extract_weather_duckdb.py

import os
import datetime
import requests
import pandas as pd
import json
import duckdb
from dotenv import load_dotenv
import traceback


# --- Configuration ---
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
WEATHER_API_BASE_URL = os.getenv("WEATHER_API_BASE_URL", "https://api.exampleweather.com/v1/current")
# UPDATED DEFAULT DB PATH: Changed default to match the traffic database path
DUCKDB_DATABASE_PATH = os.getenv("DUCKDB_DATABASE_PATH", "traffic_data.duckdb")
WEATHER_TABLE_NAME = os.getenv("WEATHER_TABLE_NAME", "weather_data")
API_TIMEOUT_SECONDS = int(os.getenv("WEATHER_API_TIMEOUT_SECONDS", 10))


# --- Helper Functions ---

def construct_weather_api_url(location_coords: dict, api_key: str, base_url: str, **kwargs) -> str:
    """Constructs Weather API URL."""
    lat = location_coords.get("lat")
    lon = location_coords.get("lon")

    if lat is None or lon is None:
        raise ValueError(f"Invalid location coordinates: {location_coords}. Requires 'lat' and 'lon'.")

    # API parameters - Adjust for your specific Weather API
    params = {
        "lat": lat,
        "lon": lon,
        "key": api_key,
        "sections": "all",
        "timezone": "UTC",
        "language": "en",
        "units": "metric",
        **kwargs
    }

    req = requests.Request('GET', base_url, params=params)
    prepared_url = req.prepare().url

    print(f"Constructed Weather API URL (excluding key): {req.prepare().url.split('key=')[0]}key=...")
    return prepared_url


def fetch_data_from_api(url: str, timeout: int):
    """Fetches data from API URL."""
    print(f"🌐 Fetching data from API (Timeout: {timeout}s)")
    try:
        response = requests.get(url=url, timeout=timeout)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch data from API: {e}")
        traceback.print_exc()
        return None


def parse_weather_response_to_dataframe(json_data: str, location_coords: dict) -> pd.DataFrame:
    """Parses JSON response into DataFrame."""
    if not json_data:
        print("No JSON data provided for parsing.")
        return pd.DataFrame()

    try:
        data = json.loads(json_data)

        # Adjust based on your Weather API's JSON structure
        current_data = data.get('current', {})

        records = []
        if current_data:
            record = {
                'latitude': location_coords.get('lat'),
                'longitude': location_coords.get('lon'),
                'fetch_timestamp_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                'location_name': location_coords.get('name'),
                'temperature_celsius': current_data.get('temperature'),
                'weather_description': current_data.get('summary'),
                'weather_icon': current_data.get('icon'),
            }
            records.append(record)
        else:
            print("Warning: 'current' data section not found or is empty in the API response.")

        df = pd.DataFrame(records)

        # Convert data types
        numeric_cols = ['latitude', 'longitude', 'temperature_celsius']
        for col in numeric_cols:
            if col in df.columns:
                 df[col] = pd.to_numeric(df[col], errors='coerce')

        if 'fetch_timestamp_utc' in df.columns:
             df['fetch_timestamp_utc'] = pd.to_datetime(df['fetch_timestamp_utc'], errors='coerce', utc=True)

        print(f"Successfully parsed {len(df)} record(s) into DataFrame.")
        return df

    except json.JSONDecodeError as e:
        print(f"❌ Error decoding JSON response: {e}")
        traceback.print_exc()
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error processing parsed weather data: {e}")
        traceback.print_exc()
        return pd.DataFrame()


def save_weather_to_duckdb(df: pd.DataFrame, db_path: str, table_name: str):
    """Saves DataFrame to DuckDB table."""
    if df.empty:
        print("No data to save to DuckDB.")
        return

    print(f"💾 Appending {len(df)} record(s) to DuckDB database '{db_path}' table '{table_name}'")
    try:
        with duckdb.connect(database=db_path) as con:
            # Use CREATE TABLE IF NOT EXISTS to avoid errors if the table already exists
            con.sql(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0")
            con.append(table_name, df)
        print(f"✅ Weather data saved successfully to DuckDB table '{table_name}'.")
    except Exception as e:
        print(f"❌ Error saving weather data to DuckDB: {e}")
        traceback.print_exc()
        raise


# --- Main Execution Block ---

if __name__ == "__main__":
    print("Running extract_weather_duckdb_standalone.py")

    load_dotenv()
    print(".env file loaded (if exists).")

    # --- Define Locations to Extract ---
    LOCATIONS_TO_EXTRACT = [
        {"lat": 10.79187, "lon": 106.68831, "name": "Ho Chi Minh City"}, # Saigon
        {"lat": 40.7128, "lon": -74.0060, "name": "New York City"},
        {"lat": 51.5074, "lon": -0.1278, "name": "London"},
    ]

    # --- Initial Checks ---
    api_key = WEATHER_API_KEY
    base_url = WEATHER_API_BASE_URL
    db_path = DUCKDB_DATABASE_PATH # Use the potentially updated path
    table_name = WEATHER_TABLE_NAME
    api_timeout = API_TIMEOUT_SECONDS

    if not api_key:
        print("❌ WEATHER_API_KEY environment variable is not set. Exiting.")
        exit(1)
    if not base_url:
         print("❌ WEATHER_API_BASE_URL environment variable is not set. Exiting.")
         exit(1)
    if not db_path:
         print("❌ DUCKDB_DATABASE_PATH environment variable is not set. Exiting.")
         exit(1)

    # Ensure the directory for the DuckDB file exists
    db_directory = os.path.dirname(db_path)
    if db_directory and not os.path.exists(db_directory):
         try:
            os.makedirs(db_directory, exist_ok=True)
            print(f"Ensured DuckDB directory exists: '{db_directory}'")
         except OSError as e:
            print(f"❌ Error creating DuckDB directory '{db_directory}': {e}. Exiting.")
            traceback.print_exc()
            exit(1)

    print(f"\nStarting weather data extraction for {len(LOCATIONS_TO_EXTRACT)} location(s)...")
    print(f"Saving data to DuckDB database: '{db_path}' into table '{table_name}'.")

    # --- DROP TABLE ONCE BEFORE THE LOOP ---
    # Note: If you want to append weather data across multiple runs,
    # you should remove or comment out this DROP TABLE block.
    # Keeping it for now for clean demonstration runs.
    try:
        if os.path.exists(db_path):
            with duckdb.connect(database=db_path) as con:
                print(f"Attempting to drop existing table '{table_name}'...")
                con.sql(f"DROP TABLE IF EXISTS {table_name}")
                print(f"Table '{table_name}' dropped if it existed.")
        else:
             print(f"Database file '{db_path}' does not exist yet, skipping drop.")
    except Exception as e:
         print(f"❌ Error dropping table: {e}")
         traceback.print_exc()
         pass

    processed_count = 0
    failed_locations = []

    # --- Process Each Location ---
    for location in LOCATIONS_TO_EXTRACT:
        location_name = location.get('name', f"lat{location.get('lat')}_lon{location.get('lon')}")
        print(f"\n--- Processing location: {location_name} ({location.get('lat')},{location.get('lon')}) ---")

        try:
            api_url = construct_weather_api_url(
                location_coords=location,
                api_key=api_key,
                base_url=base_url
            )
            json_data = fetch_data_from_api(api_url, api_timeout)

            if json_data:
                df = parse_weather_response_to_dataframe(json_data, location)
                if not df.empty:
                    # Pass the consolidated db_path to the save function
                    save_weather_to_duckdb(df, db_path, table_name)
                    processed_count += 1
                    print(f"✅ Processed {location_name}.")
                else:
                    print(f"Skipping save for {location_name}: No data parsed.")
                    failed_locations.append(location_name)
            else:
                print(f"Skipping processing for {location_name}: Failed to fetch data.")
                failed_locations.append(location_name)

        except Exception as e:
            print(f"❌ Error processing {location_name}: {e}")
            traceback.print_exc()
            failed_locations.append(location_name)

    # --- Final Summary ---
    print("\n--- Extraction Process Finished ---")
    print(f"Processed {processed_count} out of {len(LOCATIONS_TO_EXTRACT)} locations.")
    if failed_locations:
        print(f"Failed locations ({len(failed_locations)}): {', '.join(failed_locations)}")
    else:
        print("All locations processed successfully.")

    print(f"\nWeather data saved to DuckDB: '{db_path}' table '{table_name}'.")

    # Optional: Query the saved data after all locations are processed
    try:
        print("\nQuerying data from DuckDB:")
        if os.path.exists(db_path):
            with duckdb.connect(database=db_path, read_only=True) as con:
                try:
                    table_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    print(f"Table '{table_name}' contains {table_count} row(s).")
                    if table_count > 0:
                         print("\nFirst 5 rows:")
                         df_test = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
                         print(df_test)
                    else:
                        print("No data found in the table.")
                except duckdb.CatalogException:
                     print(f"Table '{table_name}' not found.")
                except Exception as e:
                     print(f"❌ Error during DuckDB query: {e}")
                     traceback.print_exc()
        else:
            print(f"Error: Database file not found at '{db_path}'.")
    except duckdb.Error as e:
        print(f"Error connecting to DuckDB: {e}")
    except Exception as e:
         print(f"❌ Unexpected error during DuckDB verification: {e}")
         traceback.print_exc()

    print("\nScript finished.")
