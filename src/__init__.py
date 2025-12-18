"""MongoDB Big Data Pipeline - Source Package.

This package provides modules for:
- Data ingestion into MongoDB sharded cluster
- Data cleaning using Pandas
- Data aggregation and analytics
- Configuration management
- Pydantic models for validation
"""

from src.config import settings
from src.models import (
    AggregatedDataModel,
    CleanDataModel,
    PipelineMetrics,
    RawDataModel,
)
from src.ingestion import DataIngestion
from src.cleaning import DataCleaning
from src.aggregation import DataAggregation
from src.utils import (
    chunk_list,
    format_bytes,
    format_duration,
    format_number,
    retry,
    safe_divide,
    timer,
)

__version__ = "1.0.0"
__all__ = [
    # Config
    "settings",
    # Models
    "RawDataModel",
    "CleanDataModel", 
    "AggregatedDataModel",
    "PipelineMetrics",
    # Classes
    "DataIngestion",
    "DataCleaning",
    "DataAggregation",
    # Utils
    "timer",
    "retry",
    "format_bytes",
    "format_number",
    "format_duration",
    "chunk_list",
    "safe_divide",
]
