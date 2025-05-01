# tests/conftest.py
import pytest
import os
import sys
from unittest.mock import patch # Needed if patching in conftest

# Import ALL fixtures from testHelpers.duckdb_fixtures
from .testHelpers.duckdb_fixtures import (
    temp_duckdb_file,
    temp_duckdb_con,
    TRAFFIC_FLOW_SCHEMA_SQL, # You can import constants too
    WEATHER_SCHEMA_SQL,
    traffic_table,
    weather_table,
    traffic_table_with_data,
    table_exists, # Import helper functions
    get_row_count,
    get_table_data
)

# Pytest will automatically discover these fixtures
# and make them available to tests in the 'tests' directory and its subdirectories.

@pytest.fixture(scope='session', autouse=True)
def add_project_root_to_path():
    """Add the project root to sys.path for the test session."""
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
        print(f"\nAdded {project_root} to sys.path for test session.")
