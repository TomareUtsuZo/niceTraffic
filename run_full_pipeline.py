# run_full_pipeline.py - Orchestrates the full ETL and Visualization pipeline

import os
import sys
import traceback
from dotenv import load_dotenv
import datetime
import duckdb # Import duckdb here as it's used for connections in the pipeline

# --- Path Setup ---
# Get the absolute path of the directory where the script is being run (project root)
# and add it to sys.path so modules can be found.
project_root = os.path.abspath(os.getcwd())
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- Import Functions and Configuration from ETL and Viz Scripts ---
try:
    # Import from Traffic ETL (assuming extract and load are separate scripts)
    # Import the extraction/transformation function and CONFIG
    from extract_traffic_duckdb import extract_and_transform_traffic_data, CONFIG as TRAFFIC_CONFIG
    # Import the loading function
    from load_traffic_duckdb import load_dataframe_to_duckdb

    # Import from Weather ETL
    # Import the necessary functions and configuration variables
    from extract_weather_duckdb import (
        construct_weather_api_url,
        fetch_data_from_api,
        parse_weather_response_to_dataframe,
        save_weather_to_duckdb,
        WEATHER_API_KEY, # Import config variables
        WEATHER_API_BASE_URL,
        WEATHER_TABLE_NAME,
        API_TIMEOUT_SECONDS as WEATHER_API_TIMEOUT
    )

    # Import the transformation function (from the refactored script)
    from transform_weather_traffic_duckdb import run_transformation, TRANSFORMED_TABLE_NAME

    # Import the visualization function and output file name (from the refactored script)
    from visualize_duckdb_data import run_visualization, OUTPUT_HTML_FILE

except ImportError as e:
    print(f"Error: Could not import necessary modules or functions.")
    print(f"Ensure all ETL, transformation, and visualization scripts are in the project root ({project_root})")
    print(f"and contain the expected functions/variables. Details: {e}")
    sys.exit(1)

# --- Configuration ---
# Load environment variables once at the start
load_dotenv()

# Define the single DuckDB database path for all steps
# Use the path from Traffic CONFIG as the canonical one, or environment variable
# Ensure this matches the path used in your other scripts' defaults/configs
DUCKDB_DATABASE_PATH = os.getenv("DUCKDB_DATABASE_PATH", TRAFFIC_CONFIG.get("DUCKDB_DATABASE", "traffic_data.duckdb"))

# Get table names from imported configs/variables
TRAFFIC_TABLE_NAME = TRAFFIC_CONFIG.get("TRAFFIC_TABLE_NAME", "traffic_flow_data")
WEATHER_TABLE_NAME = WEATHER_TABLE_NAME # Imported directly from extract_weather_duckdb
TRANSFORMED_TABLE_NAME = TRANSFORMED_TABLE_NAME # Imported directly from transform_weather_traffic_duckdb

# Define weather locations here or import from demonstrate_weather_etl_run if it's a config there
# For simplicity, let's define them here for the orchestration script
WEATHER_LOCATIONS_TO_EXTRACT = [
    {"lat": 10.79187, "lon": 106.68831, "name": "Ho Chi Minh City"}, # Saigon
    {"lat": 40.7128, "lon": -74.0060, "name": "New York City"},
    {"lat": 51.5074, "lon": -0.1278, "name": "London"},
    # Add more locations as needed
]


# --- Main Pipeline Execution ---
if __name__ == "__main__":
    print("Running the full ETL and Visualization pipeline...")
    pipeline_start_time = datetime.datetime.now()

    db_path = DUCKDB_DATABASE_PATH
    traffic_table = TRAFFIC_TABLE_NAME
    weather_table = WEATHER_TABLE_NAME
    transformed_table = TRANSFORMED_TABLE_NAME
    viz_output_file = OUTPUT_HTML_FILE

    pipeline_success = True # Flag to track overall success


    # --- Initial Checks ---
    if not os.getenv("TOMTOM_API_KEY"):
        print("❌ TOMTOM_API_KEY environment variable is not set. Cannot run Traffic ETL. Exiting.")
        sys.exit(1)
    if not os.getenv("WEATHER_API_KEY"):
        print("❌ WEATHER_API_KEY environment variable is not set. Cannot run Weather ETL. Exiting.")
        sys.exit(1)
    if not db_path:
         print("❌ DUCKDB_DATABASE_PATH environment variable is not set. Exiting.")
         sys.exit(1)

    # Ensure the directory for the DuckDB file exists
    db_directory = os.path.dirname(db_path)
    if db_directory and not os.path.exists(db_directory):
         try:
            os.makedirs(db_directory, exist_ok=True)
            print(f"Ensured DuckDB directory exists: '{db_directory}'")
         except OSError as e:
            print(f"❌ Error creating DuckDB directory '{db_directory}': {e}. Exiting.")
            traceback.print_exc()
            sys.exit(1)


    # --- Step 1: Run Traffic ETL (Extract & Load) ---
    print("\n--- Step 1: Running Traffic ETL (Extract & Load) ---")
    traffic_etl_success = False
    try:
        traffic_points = TRAFFIC_CONFIG.get('ROUTE_POINTS_EXAMPLE', [])

        if not traffic_points:
             print("Warning: No traffic points defined in TRAFFIC_CONFIG['ROUTE_POINTS_EXAMPLE']. Skipping Traffic ETL.")
        else:
            # extract_and_transform_traffic_data returns a list of DataFrames
            print(f"Extracting and transforming data for {len(traffic_points)} traffic point(s)...")
            traffic_dfs = extract_and_transform_traffic_data(traffic_points)

            if traffic_dfs:
                print(f"Loading {len(traffic_dfs)} DataFrame(s) into DuckDB table '{traffic_table}'...")
                # Need to connect to DuckDB for loading
                with duckdb.connect(database=db_path, read_only=False) as con:
                    for i, df_point in enumerate(traffic_dfs):
                        if not df_point.empty:
                            # load_dataframe_to_duckdb expects the connection, df, table_name, and point
                            # We need the point identifier from the original list for load_dataframe_to_duckdb
                            point_identifier = traffic_points[i] # Assuming order is preserved
                            load_dataframe_to_duckdb(con, df_point, traffic_table, point_identifier)
                        else:
                            print(f"Warning: Empty DataFrame for traffic point {traffic_points[i]}. Skipping load.")
                print("✅ Traffic ETL completed successfully.")
                traffic_etl_success = True
            else:
                print("No traffic DataFrames were extracted. Skipping Traffic ETL loading.")


    except Exception as e:
        print(f"❌ An error occurred during Traffic ETL: {e}")
        traceback.print_exc()
        pipeline_success = False


    # --- Step 2: Run Weather ETL (Extract & Load) ---
    print("\n--- Step 2: Running Weather ETL (Extract & Load) ---")
    weather_etl_success = False
    try:
        # Process Each Weather Location
        total_loaded_weather = 0
        failed_weather_locations = []

        if not WEATHER_LOCATIONS_TO_EXTRACT:
             print("Warning: No weather locations defined. Skipping Weather ETL.")
        else:
            for location in WEATHER_LOCATIONS_TO_EXTRACT:
                location_name = location.get('name', f"lat{location.get('lat')}_lon{location.get('lon')}")
                print(f"\n--- Processing weather for location: {location_name} ({location.get('lat')},{location.get('lon')}) ---")

                try:
                    # Construct API URL
                    api_url = construct_weather_api_url(
                        location_coords=location,
                        api_key=WEATHER_API_KEY,
                        base_url=WEATHER_API_BASE_URL,
                        # Pass other relevant config like units, language if needed
                        units="metric",
                        language="en"
                    )
                    json_data = fetch_data_from_api(api_url, WEATHER_API_TIMEOUT)

                    if json_data:
                        df = parse_weather_response_to_dataframe(json_data, location)
                        if not df.empty:
                            # save_weather_to_duckdb handles its own connection/closing internally
                            save_weather_to_duckdb(df, db_path, weather_table)
                            total_loaded_weather += len(df)
                            print(f"✅ Processed weather for {location_name}.")
                        else:
                            print(f"Skipping weather save for {location_name}: No data parsed.")
                            failed_weather_locations.append(location_name)
                    else:
                        print(f"Skipping weather processing for {location_name}: Failed to fetch data.")
                        failed_weather_locations.append(location_name)

                except Exception as e:
                    print(f"❌ Error processing weather for {location_name}: {e}")
                    traceback.print_exc()
                    failed_weather_locations.append(location_name)

            print(f"\nWeather Processing Summary:")
            print(f"Processed {len(WEATHER_LOCATIONS_TO_EXTRACT) - len(failed_weather_locations)} out of {len(WEATHER_LOCATIONS_TO_EXTRACT)} locations.")
            if failed_weather_locations:
                print(f"Failed weather locations ({len(failed_weather_locations)}): {', '.join(failed_weather_locations)}")
            else:
                print("All weather locations processed successfully.")

            if total_loaded_weather > 0:
                 print(f"✅ Weather ETL completed successfully. Total rows loaded: {total_loaded_weather}.")
                 weather_etl_success = True
            else:
                 print("⚠️ Weather ETL completed, but no rows were loaded.")


    except Exception as e:
        print(f"❌ An error occurred during Weather ETL: {e}")
        traceback.print_exc()
        pipeline_success = False


    # --- Step 3: Run Transformation ---
    print("\n--- Step 3: Running Transformation ---")
    transformation_success = False
    # Only run transformation if both ETL steps had some success or didn't fail critically
    if traffic_etl_success or weather_etl_success: # Decide your logic: require both or either? Let's require at least one loaded row for transformation to make sense.
         # Check if the source tables actually exist and have data before transforming
         try:
             with duckdb.connect(database=db_path, read_only=True) as con:
                 traffic_count = con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{traffic_table}'").fetchone()[0] > 0 and con.execute(f"SELECT COUNT(*) FROM {traffic_table}").fetchone()[0] > 0
                 weather_count = con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{weather_table}'").fetchone()[0] > 0 and con.execute(f"SELECT COUNT(*) FROM {weather_table}").fetchone()[0] > 0

             if traffic_count and weather_count:
                 try:
                     # Call the transformation function
                     run_transformation(db_path, weather_table, traffic_table, transformed_table)
                     transformation_success = True
                     print("✅ Transformation completed successfully.")
                 except Exception as e:
                     print(f"❌ An error occurred during Transformation: {e}")
                     traceback.print_exc()
                     pipeline_success = False
             else:
                 print("Skipping Transformation: One or both source tables are missing or empty.")
                 if not traffic_count: print(f" - Traffic table '{traffic_table}' is missing or empty.")
                 if not weather_count: print(f" - Weather table '{weather_table}' is missing or empty.")


         except Exception as e:
              print(f"❌ Error checking source tables before transformation: {e}")
              traceback.print_exc()
              pipeline_success = False

    else:
        print("Skipping Transformation: Both Traffic and Weather ETL steps were skipped or failed.")


    # --- Step 4: Run Visualization ---
    print("\n--- Step 4: Running Visualization ---")
    visualization_success = False
    # Only run visualization if transformation was successful and the transformed table has data
    if transformation_success:
         try:
             # Call the visualization function
             run_visualization(db_path, transformed_table, viz_output_file)
             visualization_success = True
             print("✅ Visualization completed successfully.")
         except Exception as e:
             print(f"❌ An error occurred during Visualization: {e}")
             traceback.print_exc()
             pipeline_success = False
    else:
        print("Skipping Visualization: Transformation step was skipped or failed.")


    # --- Pipeline Summary ---
    print("\n--- Full Pipeline Summary ---")
    pipeline_end_time = datetime.datetime.now()
    duration = pipeline_end_time - pipeline_start_time
    print(f"Pipeline started at: {pipeline_start_time.isoformat()}")
    print(f"Pipeline finished at: {pipeline_end_time.isoformat()}")
    print(f"Total duration: {duration}")

    if pipeline_success:
        print("\n🎉 Full pipeline executed successfully!")
        print(f"Check the visualization output file: {os.path.abspath(viz_output_file)}")
    else:
        print("\n❌ Full pipeline execution failed or encountered errors in one or more steps.")
        print("Review the output above for details on which steps failed.")

