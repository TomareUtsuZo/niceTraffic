# visualize_duckdb_data.py - Script to visualize data from DuckDB tables over time (as a function)

import duckdb
import os
import pandas as pd
from dotenv import load_dotenv
import plotly.express as px
import plotly.graph_objects as go
import webbrowser
import traceback
import datetime

# --- Configuration ---
# Load environment variables (needed if this script is run standalone)
load_dotenv()

# Get the database path and table names from environment variables
DUCKDB_DATABASE_PATH = os.getenv("DUCKDB_DATABASE_PATH", "traffic_data.duckdb")
TRANSFORMED_TABLE_NAME = os.getenv("TRANSFORMED_TABLE_NAME", "transformed_weather_traffic") # Table to visualize

# Output HTML file name
OUTPUT_HTML_FILE = "weather_traffic_time_visualization.html" # Changed output file name


# --- Visualization Function ---
def run_visualization(db_path: str, transformed_table: str, output_file: str):
    """
    Connects to DuckDB, queries the transformed data, generates visualizations,
    saves them to an HTML file, and opens the file in a browser.
    Only generates the Transit Time Over Time by Location plot.
    """
    print("\n--- Running Visualization ---")
    print(f"Connecting to database: {db_path}")

    duckdb_con = None
    try:
        # Connect to the DuckDB database in read-only mode
        duckdb_con = duckdb.connect(database=db_path, read_only=True)
        print("✅ DuckDB connection successful (read-only).")

        # --- Query Data from Transformed Table ---
        print(f"\nQuerying data from table: '{transformed_table}'...")
        try:
            # Check if the transformed table exists
            table_exists = duckdb_con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{transformed_table}'").fetchone()[0] > 0

            if not table_exists:
                 print(f"❌ Table '{transformed_table}' does not exist in the database '{db_path}'. Please run the transformation script first.")
                 # Decide if we should exit or return False/None
                 # Let's raise an exception to signal failure
                 raise duckdb.CatalogException(f"Table '{transformed_table}' not found.")


            # Query all data from the transformed table
            # Ensure transformation_timestamp is included for time series plots
            df_transformed = duckdb_con.execute(f"SELECT * FROM {transformed_table}").fetchdf()

            if df_transformed.empty:
                print(f"Table '{transformed_table}' is empty. No data to visualize.")
                return # Return gracefully if no data

            print(f"✅ Successfully queried {len(df_transformed)} rows from '{transformed_table}'.")
            # print(df_transformed.head()) # Optional: Print head for debugging

            # Ensure the timestamp column is datetime type for plotting
            if 'transformation_timestamp' in df_transformed.columns:
                 df_transformed['transformation_timestamp'] = pd.to_datetime(df_transformed['transformation_timestamp'], errors='coerce')
            else:
                 print("Warning: 'transformation_timestamp' column not found. Time series plots may not work.")


        except duckdb.Error as e:
            print(f"❌ DuckDB Error querying table '{transformed_table}': {e}")
            traceback.print_exc()
            raise # Re-raise the exception for the caller
        except Exception as e:
            print(f"❌ An unexpected error occurred querying '{transformed_table}': {e}")
            traceback.print_exc()
            raise # Re-raise the exception for the caller
    finally:
        # Ensure the DuckDB connection is closed
        if duckdb_con:
            try:
                duckdb_con.close()
                print("\n✅ DuckDB connection closed.")
            except Exception as e:
                 print(f"Warning: Error closing DuckDB connection: {e}")
                 traceback.print_exc()


    # --- Generate Visualizations using Plotly (Time Series Focus) ---
    print("\nGenerating time series visualizations...")

    # Create a list to hold Plotly figures
    figures = []

    # Visualization 1: Transit Time over Time, faceted by Location
    # ONLY KEEPING THIS PLOT
    if 'transformation_timestamp' in df_transformed.columns and 'transit_time_minutes' in df_transformed.columns and 'location_name' in df_transformed.columns:
        fig1 = px.line(df_transformed,
                       x="transformation_timestamp",
                       y="transit_time_minutes",
                       color="location_name", # Color lines by location
                       line_group="location_name", # Ensure separate lines per location
                       hover_name="location_name",
                       title="Transit Time Over Time by Location")
        figures.append(fig1)
    else:
         print("Skipping Transit Time over Time plot due to missing required columns.")


    if not figures:
         print("No figures were generated. Check data and column names.")
         return # Return gracefully if no figures were created


    # --- Generate HTML Output ---
    print(f"\nGenerating HTML file: {output_file}...")

    # Create an HTML string containing all figures
    html_content = "<html><head><title>Weather and Traffic Visualization Over Time</title></head><body>"
    html_content += "<h1>Weather and Traffic Data Visualization Over Time</h1>"

    for i, fig in enumerate(figures):
        # Add each figure as an HTML div
        html_content += fig.to_html(full_html=False, include_plotlyjs='cdn')
        # Removed the horizontal rule as there's only one plot
        # html_content += "<hr>" # Add a separator between plots

    html_content += "</body></html>"

    # Save the HTML content to a file
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"✅ HTML file saved successfully: {output_file}")
    except IOError as e:
        print(f"❌ Error saving HTML file '{output_file}': {e}")
        traceback.print_exc()
        raise # Re-raise the exception for the caller


    # --- Launch in Browser ---
    print(f"\nLaunching '{output_file}' in default web browser...")
    try:
        webbrowser.open(f"file://{os.path.abspath(output_file)}")
        print("✅ Launched successfully.")
    except Exception as e:
        print(f"❌ Error launching web browser: {e}")
        print(f"You can manually open the file: {os.path.abspath(output_file)}")
        traceback.print_exc()
        # Don't necessarily re-raise here, as the file was saved


    except duckdb.Error as e:
        print(f"❌ DuckDB Error during visualization: {e}")
        traceback.print_exc()
        raise # Re-raise the exception for the caller
    except Exception as e:
        print(f"❌ An unexpected error occurred during visualization: {e}")
        traceback.print_exc()
        raise # Re-raise the exception for the caller
    finally:
        # Ensure the DuckDB connection is closed
        if duckdb_con:
            try:
                duckdb_con.close()
                print("\n✅ DuckDB connection closed.")
            except Exception as e:
                 print(f"Warning: Error closing DuckDB connection: {e}")
                 traceback.print_exc()


# --- Main Execution Block (for standalone testing) ---
if __name__ == "__main__":
    print("Running visualize_duckdb_data.py standalone...")
    # This block allows running the script directly for testing the visualization function
    db_path = DUCKDB_DATABASE_PATH
    transformed_table = TRANSFORMED_TABLE_NAME
    output_file = OUTPUT_HTML_FILE

    # Ensure the database file exists before attempting visualization
    if not os.path.exists(db_path):
        print(f"❌ Database file not found at '{db_path}'. Cannot run visualization standalone.")
    else:
        try:
            run_visualization(db_path, transformed_table, output_file)
        except Exception as e:
            print(f"Standalone visualization run failed: {e}")

    print("\nStandalone visualization script finished.")
