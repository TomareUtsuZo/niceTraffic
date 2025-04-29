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

        # --- Query Transformed Data ---
        print(f"\nQuerying data from table: '{transformed_table}'...")
        try:
            # Check if the transformed table exists and has data
            table_exists = duckdb_con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{transformed_table}'").fetchone()[0] > 0
            if not table_exists:
                print(f"❌ Transformed table '{transformed_table}' not found. Cannot generate visualization.")
                return

            row_count = duckdb_con.execute(f"SELECT COUNT(*) FROM {transformed_table}").fetchone()[0]
            print(f"✅ Successfully queried {row_count} rows from '{transformed_table}'.")

            if row_count == 0:
                print("⚠️ Transformed table is empty. No data to visualize.")
                return

            # Fetch data into a pandas DataFrame
            # MODIFIED: Select the new aggregated column name 'avg_transit_time_minutes'
            transformed_df = duckdb_con.execute(f"""
                SELECT
                    location_name,
                    avg_transit_time_minutes, -- Use the new aggregated column name
                    transformation_timestamp,
                    weather_description,
                    temperature_celsius
                FROM {transformed_table}
                ORDER BY transformation_timestamp, location_name -- Order for better plot
            """).fetchdf()

        except duckdb.CatalogException:
             print(f"❌ Table '{transformed_table}' not found during query.")
             return
        except Exception as e:
             print(f"❌ Error querying transformed data: {e}")
             traceback.print_exc()
             return # Exit visualization if query fails


        # --- Generate Visualizations ---
        print("\nGenerating time series visualizations...")
        figures = []

        # Check for required columns in the DataFrame
        # MODIFIED: Check for the new aggregated column name
        required_cols = ['location_name', 'avg_transit_time_minutes', 'transformation_timestamp']
        if not all(col in transformed_df.columns for col in required_cols):
            print(f"Skipping Transit Time over Time plot due to missing required columns.")
            print(f"Required: {required_cols}, Found: {transformed_df.columns.tolist()}")
        else:
            try:
                # Transit Time Over Time by Location
                # MODIFIED: Use the new aggregated column name for the y-axis
                fig_transit_time = px.line(
                    transformed_df,
                    x="transformation_timestamp",
                    y="avg_transit_time_minutes", # Use the new aggregated column name
                    color="location_name",
                    title="Average Transit Time Over Time by Location",
                    labels={
                        "transformation_timestamp": "Time",
                        "avg_transit_time_minutes": "Average Transit Time (minutes)", # Update label
                        "location_name": "Location"
                    },
                    hover_data={
                        "weather_description": True,
                        "temperature_celsius": True,
                        "avg_transit_time_minutes": ':.2f' # Format hover data
                    }
                )
                fig_transit_time.update_layout(hovermode="x unified") # Unified hover for time series
                figures.append(fig_transit_time)
                print("✅ Generated Average Transit Time Over Time plot.")

            except Exception as e:
                print(f"❌ Error generating Transit Time plot: {e}")
                traceback.print_exc()


        if not figures:
            print("No figures were generated. Check data and column names.")
            return # Exit if no figures were created

        # --- Save and Open HTML File ---
        print(f"\nSaving visualization to HTML file: '{output_file}'...")
        try:
            # Create a single HTML file containing all figures
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("<html><head><title>Weather and Traffic Visualization</title></head><body>\n")
                f.write("<h1>Weather and Traffic Visualization</h1>\n")
                for i, fig in enumerate(figures):
                    f.write(f"<h2>Figure {i+1}</h2>\n")
                    f.write(fig.to_html(full_html=False, include_plotlyjs='cdn')) # Embed figures
                f.write("</body></html>")

            print(f"✅ Visualization saved successfully to '{output_file}'.")

            # Open the HTML file in the default web browser
            # Check if running in an environment where a browser can be opened
            if os.name != 'posix' or 'DISPLAY' in os.environ: # Basic check for graphical environment
                 try:
                     webbrowser.open(f'file://{os.path.abspath(output_file)}')
                     print(f"✅ Opened '{output_file}' in default web browser.")
                 except Exception as e:
                     print(f"⚠️ Could not automatically open web browser: {e}")
                     print(f"Please open the file manually: {os.path.abspath(output_file)}")
            else:
                 print(f"Running in a non-graphical environment. Please open the file manually: {os.path.abspath(output_file)}")


        except Exception as e:
            print(f"❌ Error saving or opening HTML file: {e}")
            traceback.print_exc()


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
    print("Running visualize_duckdb_data.py standalone...\n")
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
