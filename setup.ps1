# setup.ps1
Write-Host "Setting up the project environment..."

# --- 1. Check and Create Virtual Environment ---
Write-Host "`nChecking for virtual environment..."
# Use .niceTraffic as the virtual environment folder name
$venvPath = ".\.niceTraffic"
if (-not (Test-Path $venvPath -PathType Container)) {
    Write-Host "Creating virtual environment in .niceTraffic..."
    # Using & to ensure the command runs correctly
    & python -m venv $venvPath
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: Failed to create virtual environment. Make sure Python is installed and in your PATH."
        exit 1
    }
    Write-Host "Virtual environment created in .niceTraffic."
} else {
    Write-Host "Virtual environment .niceTraffic already exists."
}

# --- 2. Install Dependencies ---
Write-Host "`nInstalling dependencies from requirements.txt..."
if (-not (Test-Path ".\requirements.txt" -PathType Leaf)) {
    Write-Host "Error: requirements.txt not found in the current directory."
    exit 1
}

# Construct the path to the pip executable inside the venv
$pipPath = Join-Path $venvPath "Scripts\pip.exe"

# Use & to execute pip from the virtual environment
& $pipPath install -r requirements.txt
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to install dependencies. Check the error messages above."
    exit 1
}
Write-Host "Dependencies installed successfully."

# --- 3. Activate Virtual Environment ---
Write-Host "`nActivating virtual environment..."
# Construct the path to the activate script
$activateScript = Join-Path $venvPath "Scripts\Activate.ps1"

# Dot-source the activate script to run it in the current session
. $activateScript
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to activate virtual environment."
    # Note: Activation errors might not always set LASTEXITCODE reliably
}
Write-Host "Virtual environment activated."

