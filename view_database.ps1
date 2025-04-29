# view_database.ps1
# This script activates the Python virtual environment and runs the view_duckdb_tables.py script
# to display the contents of the DuckDB database tables.

Write-Host "Activating virtual environment and viewing DuckDB tables..."

# Define the path to the virtual environment
$venvPath = ".\.niceTraffic"
$activateScript = Join-Path $venvPath "Scripts\Activate.ps1"

# Check if the virtual environment exists
if (-not (Test-Path $venvPath -PathType Container)) {
    Write-Host "Error: Virtual environment not found at '$venvPath'."
    Write-Host "Please run the setup.ps1 script first to create and set up the environment."
    exit 1
}

# Check if the activate script exists
if (-not (Test-Path $activateScript -PathType Leaf)) {
    Write-Host "Error: Virtual environment activation script not found at '$activateScript'."
    Write-Host "Ensure the virtual environment was created correctly."
    exit 1
}

# Check if the view script exists
if (-not (Test-Path ".\view_duckdb_tables.py" -PathType Leaf)) {
    Write-Host "Error: view_duckdb_tables.py script not found in the current directory."
    exit 1
}

# --- Configuration ---
# Set the path to your DuckDB database file.
# This should match the path used by your ETL scripts.
# Defaults to 'traffic_data.duckdb' if not explicitly set here or in your .env file
# You can uncomment the line below to explicitly set it if needed:
# $env:DUCKDB_DATABASE_PATH = "your_database_path.duckdb"

# Activate the virtual environment in the current session
. $activateScript
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to activate virtual environment."
    # Continue cautiously, as activation errors might not always set LASTEXITCODE reliably
} else {
    Write-Host "Virtual environment activated."
}

# Run the Python script to view the tables
Write-Host "`nRunning view_duckdb_tables.py..."
# Use the python executable from the activated environment
& python .\view_duckdb_tables.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "`nError: The view_duckdb_tables.py script encountered an error."
    exit 1
}

Write-Host "`nFinished viewing DuckDB tables."

# Note: The virtual environment remains active in the current session
# after this script finishes. You can deactivate it manually by typing 'deactivate'.