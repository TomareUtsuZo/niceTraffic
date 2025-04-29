# Weather and Traffic Data Pipeline

This project implements a simple ETL (Extract, Transform, Load) pipeline to fetch traffic and weather data, store it in a DuckDB database, and visualize the transformed data.

## Project Structure

The project is organized as follows:


your_project_root/
├── ELTscripts/
│   ├── extract_traffic_duckdb.py   # Extracts and transforms traffic data
│   ├── load_traffic_duckdb.py      # Loads traffic data into DuckDB
│   ├── extract_weather_duckdb.py   # Extracts and loads weather data into DuckDB
│   └── transform_weather_traffic_duckdb.py # Transforms/joins weather and traffic data in DuckDB
├── requirements.txt              # Lists Python dependencies
├── run_full_pipeline.py          # Orchestrates the entire ETL and Visualization process
├── setup.ps1                     # PowerShell script for environment setup
├── view_duckdb_tables.py         # Python script to view DuckDB table contents
├── visualize_duckdb_data.py      # Python script to visualize transformed data
└── .env                          # Environment variables (API keys, DB path, etc.)
## File Descriptions

- **run_full_pipeline.py**: This is the main script that orchestrates the entire data pipeline. It calls the functions from the ELT scripts to extract, load, and transform the data, and then runs the visualization script.
- **ELTscripts/**: This folder contains the individual scripts responsible for the Extract, Load, and Transform steps.
  - **extract_traffic_duckdb.py**: Fetches traffic flow data from the TomTom Traffic API and performs initial transformation into a pandas DataFrame.
  - **load_traffic_duckdb.py**: Contains a function to load pandas DataFrames into a specified DuckDB table.
  - **extract_weather_duckdb.py**: Fetches current weather data from a weather API and loads it into a DuckDB table.
  - **transform_weather_traffic_duckdb.py**: Connects to the DuckDB database and performs a SQL transformation to join weather and traffic data, aggregating traffic data by location.
- **visualize_duckdb_data.py**: This script queries the transformed data from DuckDB and generates visualizations (e.g., time series plots) using Plotly, saving the output to an HTML file.
- **view_duckdb_tables.py**: A utility script to connect to the DuckDB database and display the contents of the weather_data, traffic_flow_data, and transformed_weather_traffic tables.
- **requirements.txt**: Specifies the Python libraries required to run the project (e.g., pandas, duckdb, requests, plotly, python-dotenv).
- **setup.ps1**: A PowerShell script to automate the setup of the Python virtual environment and installation of dependencies listed in requirements.txt.
- **.env**: This file (you'll need to create it) is used to store environment variables such as API keys and the DuckDB database path.

run the setup.ps1 file first. 
then run run_full_pipeline.py a few times to build up a database, and then if you're curious about seeing the database tables, feel free to run, 
view_duckdb_tables.py