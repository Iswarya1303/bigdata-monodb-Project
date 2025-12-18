"""Configuration management for the MongoDB pipeline."""

from typing import Literal
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings with environment variable support."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # MongoDB Configuration
    mongodb_host: str = Field(default="localhost", description="MongoDB host")
    mongodb_port: int = Field(default=27019, description="MongoDB port")
    mongodb_database: str = Field(default="bigdata_project", description="Database name")
    
    # Collections
    raw_collection: str = Field(default="raw_data", description="Raw data collection")
    clean_collection: str = Field(default="clean_data", description="Cleaned data collection")
    agg_collection: str = Field(default="aggregated_data", description="Aggregated collection")
    
    # Sharding Configuration
    shard_key: str = Field(default="user_id", description="Field to shard on")
    
    # Processing Configuration
    batch_size: int = Field(default=10000, description="Batch size for processing")
    chunk_size: int = Field(default=100000, description="Chunk size for data loading")
    
    # Logging
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO", 
        description="Logging level"
    )
    log_file: str = Field(default="pipeline.log", description="Log file path")
    
    @property
    def mongodb_uri(self) -> str:
        """Generate MongoDB connection URI."""
        return f"mongodb://{self.mongodb_host}:{self.mongodb_port}"


# Global settings instance
settings = Settings()
