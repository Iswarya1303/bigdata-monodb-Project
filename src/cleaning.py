"""Data cleaning module using Pandas."""

import time
from typing import Any

import pandas as pd
from loguru import logger
from pymongo import MongoClient

from src.config import settings
from src.models import PipelineMetrics


class DataCleaning:
    """Handle data cleaning operations using Pandas."""
    
    def __init__(self) -> None:
        """Initialize MongoDB connection."""
        self.client = MongoClient(settings.mongodb_uri)
        self.db = self.client[settings.mongodb_database]
        logger.info(f"Connected to MongoDB: {settings.mongodb_uri}")
    
    def read_from_mongodb(self, collection: str) -> pd.DataFrame:
        """Read data from MongoDB collection.
        
        Args:
            collection: Collection name to read from
            
        Returns:
            Pandas DataFrame
        """
        logger.info(f"Reading from MongoDB collection: {collection}")
        
        cursor = self.db[collection].find()
        df = pd.DataFrame(list(cursor))
        
        # Remove MongoDB's _id field if present
        if "_id" in df.columns:
            df = df.drop("_id", axis=1)
        
        logger.info(f"Loaded {len(df)} rows from {collection}")
        return df
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply cleaning transformations to the data.
        
        Args:
            df: Input Pandas DataFrame
            
        Returns:
            Cleaned Pandas DataFrame
        """
        logger.info("Starting data cleaning...")
        initial_count = len(df)
        
        # Remove duplicates
        df = df.drop_duplicates()
        logger.info(f"Removed {initial_count - len(df)} duplicate rows")
        
        # Handle missing values
        df["product_name"] = df["product_name"].fillna("Unknown Product")
        df["category"] = df["category"].fillna("Uncategorized")
        df["status"] = df["status"].fillna("unknown")
        
        # Drop rows with missing critical fields
        critical_fields = ["user_id", "order_id", "product_id", "price", "quantity"]
        df = df.dropna(subset=critical_fields)
        logger.info(f"Rows after dropping nulls in critical fields: {len(df)}")
        
        # Normalize text fields
        df["product_name"] = df["product_name"].astype(str).str.strip()
        df["category"] = df["category"].astype(str).str.strip().str.title()
        df["status"] = df["status"].astype(str).str.strip().str.lower()
        
        # Standardize status values
        status_map = {
            "completed": "completed", "complete": "completed", "done": "completed",
            "pending": "pending", "processing": "pending",
            "cancelled": "cancelled", "canceled": "cancelled",
            "returned": "returned", "refunded": "returned",
        }
        df["status"] = df["status"].map(lambda x: status_map.get(x, "unknown"))
        
        # Parse and standardize dates
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
        
        # Add derived columns
        df["total_amount"] = (df["price"] * df["quantity"]).round(2)
        df["year"] = df["order_date"].dt.year
        df["month"] = df["order_date"].dt.month
        df["day_of_week"] = df["order_date"].dt.day_name()
        
        # Ensure proper types
        df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce").astype("Int64")
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
        
        # Remove invalid data
        df = df[df["price"] >= 0]
        df = df[df["quantity"] > 0]
        df = df[df["user_id"] > 0]
        df = df.dropna(subset=["order_date"])
        
        final_count = len(df)
        logger.info(f"Cleaning complete: {final_count} valid rows")
        
        return df.reset_index(drop=True)
    
    def write_to_mongodb(self, df: pd.DataFrame, collection: str) -> None:
        """Write DataFrame to MongoDB collection.
        
        Args:
            df: Pandas DataFrame to write
            collection: Target collection name
        """
        logger.info(f"Writing {len(df)} rows to MongoDB collection: {collection}")
        
        # Drop existing collection
        self.db[collection].drop()
        
        # Convert DataFrame to dict records and insert
        records = df.to_dict("records")
        
        if records:
            # Insert in batches
            for i in range(0, len(records), settings.batch_size):
                batch = records[i:i + settings.batch_size]
                self.db[collection].insert_many(batch)
        
        logger.info(f"Successfully wrote to {collection}")
    
    def run_cleaning_pipeline(self) -> PipelineMetrics:
        """Execute the complete cleaning pipeline.
        
        Returns:
            PipelineMetrics with cleaning statistics
        """
        start_time = time.time()
        
        try:
            # Read raw data
            raw_df = self.read_from_mongodb(settings.raw_collection)
            initial_count = len(raw_df)
            
            # Clean data
            clean_df = self.clean_data(raw_df)
            final_count = len(clean_df)
            
            # Write cleaned data
            self.write_to_mongodb(clean_df, settings.clean_collection)
            
            execution_time = time.time() - start_time
            
            metrics = PipelineMetrics(
                stage="cleaning",
                records_processed=final_count,
                records_failed=initial_count - final_count,
                execution_time_seconds=execution_time
            )
            
            logger.info(
                f"Cleaning pipeline complete: {final_count} records in {execution_time:.2f}s"
            )
            
            return metrics
            
        except Exception as e:
            logger.error(f"Cleaning pipeline failed: {e}")
            raise
    
    def get_data_quality_report(self) -> dict[str, Any]:
        """Generate data quality report."""
        df = self.read_from_mongodb(settings.clean_collection)
        
        report = {
            "total_rows": len(df),
            "null_counts": df.isnull().sum().to_dict(),
            "distinct_counts": {
                "category": df["category"].nunique(),
                "status": df["status"].nunique(),
            },
            "numeric_stats": {
                "avg_price": float(df["price"].mean()),
                "min_price": float(df["price"].min()),
                "max_price": float(df["price"].max()),
                "total_revenue": float(df["total_amount"].sum()),
            }
        }
        
        logger.info("Data quality report generated")
        return report
    
    def close(self) -> None:
        """Close MongoDB connection."""
        self.client.close()
        logger.info("MongoDB connection closed")
