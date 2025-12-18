"""Shared pytest fixtures and configuration."""

import pytest
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test environment before running tests."""
    # Set environment variables for testing
    import os
    os.environ.setdefault("MONGODB_HOST", "localhost")
    os.environ.setdefault("MONGODB_PORT", "27017")
    os.environ.setdefault("LOG_LEVEL", "WARNING")
    
    yield
    
    # Cleanup after tests
    pass


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )

