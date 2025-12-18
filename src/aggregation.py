"""Data aggregation module using Pandas."""

import time
from typing import Any

import pandas as pd
from loguru import logger
from pymongo import MongoClient

from src.config import settings
from src.models import PipelineMetrics


class DataAggregation:
    """Handle data aggregation operations using Pandas."""
    
    def __init__(self, client: MongoClient | None = None) -> None:
        """Initialize with MongoDB connection.
        
        Args:
            client: Optional existing MongoClient instance
        """
        if client is None:
            self.client = MongoClient(settings.mongodb_uri)
            self._owns_client = True
        else:
            self.client = client
            self._owns_client = False
        
        self.db = self.client[settings.mongodb_database]
        logger.info("DataAggregation initialized")
    
    def read_from_mongodb(self, collection: str) -> pd.DataFrame:
        """Read data from MongoDB collection."""
        logger.info(f"Reading from MongoDB collection: {collection}")
        
        cursor = self.db[collection].find()
        df = pd.DataFrame(list(cursor))
        
        if "_id" in df.columns:
            df = df.drop("_id", axis=1)
        
        logger.info(f"Loaded {len(df)} rows from {collection}")
        return df
    
    def aggregate_by_category(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate metrics by product category.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Aggregated DataFrame by category
        """
        logger.info("Aggregating by category...")
        
        agg_df = df.groupby("category").agg(
            total_orders=("order_id", "count"),
            total_revenue=("total_amount", "sum"),
            avg_order_value=("total_amount", "mean"),
            unique_customers=("user_id", "nunique"),
            total_quantity=("quantity", "sum"),
            first_order_date=("order_date", "min"),
            last_order_date=("order_date", "max"),
        ).reset_index()
        
        agg_df = agg_df.rename(columns={"category": "_id"})
        agg_df["total_revenue"] = agg_df["total_revenue"].round(2)
        agg_df["avg_order_value"] = agg_df["avg_order_value"].round(2)
        agg_df = agg_df.sort_values("total_revenue", ascending=False)
        
        logger.info(f"Category aggregation complete: {len(agg_df)} categories")
        return agg_df
    
    def aggregate_by_month(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate metrics by year-month.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Aggregated DataFrame by month
        """
        logger.info("Aggregating by month...")
        
        df = df.copy()
        df["year_month"] = df["order_date"].dt.strftime("%Y-%m")
        
        agg_df = df.groupby("year_month").agg(
            total_orders=("order_id", "count"),
            total_revenue=("total_amount", "sum"),
            avg_order_value=("total_amount", "mean"),
            unique_customers=("user_id", "nunique"),
            total_quantity=("quantity", "sum"),
        ).reset_index()
        
        agg_df = agg_df.rename(columns={"year_month": "_id"})
        agg_df["total_revenue"] = agg_df["total_revenue"].round(2)
        agg_df["avg_order_value"] = agg_df["avg_order_value"].round(2)
        agg_df = agg_df.sort_values("_id")
        
        logger.info(f"Monthly aggregation complete: {len(agg_df)} months")
        return agg_df
    
    def aggregate_by_status(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate metrics by order status.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Aggregated DataFrame by status
        """
        logger.info("Aggregating by status...")
        
        agg_df = df.groupby("status").agg(
            total_orders=("order_id", "count"),
            total_revenue=("total_amount", "sum"),
            avg_order_value=("total_amount", "mean"),
            unique_customers=("user_id", "nunique"),
            total_quantity=("quantity", "sum"),
        ).reset_index()
        
        agg_df = agg_df.rename(columns={"status": "_id"})
        agg_df["total_revenue"] = agg_df["total_revenue"].round(2)
        agg_df["avg_order_value"] = agg_df["avg_order_value"].round(2)
        agg_df = agg_df.sort_values("total_orders", ascending=False)
        
        logger.info(f"Status aggregation complete: {len(agg_df)} statuses")
        return agg_df
    
    def aggregate_by_user(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate metrics by user (top customers).
        
        Args:
            df: Input DataFrame
            
        Returns:
            Aggregated DataFrame by user
        """
        logger.info("Aggregating by user...")
        
        agg_df = df.groupby("user_id").agg(
            total_orders=("order_id", "count"),
            total_revenue=("total_amount", "sum"),
            avg_order_value=("total_amount", "mean"),
            total_quantity=("quantity", "sum"),
            first_order_date=("order_date", "min"),
            last_order_date=("order_date", "max"),
            categories_purchased=("category", "nunique"),
        ).reset_index()
        
        agg_df["_id"] = agg_df["user_id"].astype(str)
        agg_df = agg_df.drop("user_id", axis=1)
        agg_df["total_revenue"] = agg_df["total_revenue"].round(2)
        agg_df["avg_order_value"] = agg_df["avg_order_value"].round(2)
        agg_df = agg_df.sort_values("total_revenue", ascending=False).head(1000)
        
        logger.info(f"User aggregation complete: {len(agg_df)} top users")
        return agg_df
    
    def aggregate_day_of_week(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate metrics by day of week.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Aggregated DataFrame by day of week
        """
        logger.info("Aggregating by day of week...")
        
        agg_df = df.groupby("day_of_week").agg(
            total_orders=("order_id", "count"),
            total_revenue=("total_amount", "sum"),
            avg_order_value=("total_amount", "mean"),
            unique_customers=("user_id", "nunique"),
        ).reset_index()
        
        # Sort by day order
        day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        agg_df["day_order"] = agg_df["day_of_week"].map(
            {day: i for i, day in enumerate(day_order)}
        )
        agg_df = agg_df.sort_values("day_order").drop("day_order", axis=1)
        agg_df = agg_df.rename(columns={"day_of_week": "_id"})
        agg_df["total_revenue"] = agg_df["total_revenue"].round(2)
        agg_df["avg_order_value"] = agg_df["avg_order_value"].round(2)
        
        logger.info(f"Day of week aggregation complete: {len(agg_df)} days")
        return agg_df
    
    def write_to_mongodb(self, df: pd.DataFrame, collection_suffix: str) -> None:
        """Write aggregated data to MongoDB.
        
        Args:
            df: DataFrame to write
            collection_suffix: Suffix for collection name
        """
        collection_name = f"{settings.agg_collection}_{collection_suffix}"
        logger.info(f"Writing {len(df)} rows to {collection_name}")
        
        # Drop existing collection
        self.db[collection_name].drop()
        
        # Convert datetime columns to string for JSON serialization
        df_copy = df.copy()
        for col in df_copy.columns:
            if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
                df_copy[col] = df_copy[col].dt.strftime("%Y-%m-%d %H:%M:%S")
        
        records = df_copy.to_dict("records")
        if records:
            self.db[collection_name].insert_many(records)
        
        logger.info(f"Successfully wrote to {collection_name}")
    
    def run_aggregation_pipeline(self) -> PipelineMetrics:
        """Execute the complete aggregation pipeline.
        
        Returns:
            PipelineMetrics with aggregation statistics
        """
        start_time = time.time()
        total_records = 0
        
        try:
            # Read cleaned data
            clean_df = self.read_from_mongodb(settings.clean_collection)
            
            # Perform all aggregations
            aggregations = {
                "category": self.aggregate_by_category(clean_df),
                "month": self.aggregate_by_month(clean_df),
                "status": self.aggregate_by_status(clean_df),
                "user": self.aggregate_by_user(clean_df),
                "day_of_week": self.aggregate_day_of_week(clean_df),
            }
            
            # Write each aggregation to MongoDB
            for name, agg_df in aggregations.items():
                self.write_to_mongodb(agg_df, name)
                total_records += len(agg_df)
            
            execution_time = time.time() - start_time
            
            metrics = PipelineMetrics(
                stage="aggregation",
                records_processed=total_records,
                records_failed=0,
                execution_time_seconds=execution_time
            )
            
            logger.info(
                f"Aggregation pipeline complete: {total_records} records in {execution_time:.2f}s"
            )
            
            return metrics
            
        except Exception as e:
            logger.error(f"Aggregation pipeline failed: {e}")
            raise
    
    def get_summary_stats(self) -> dict[str, Any]:
        """Get summary statistics across all aggregations."""
        clean_df = self.read_from_mongodb(settings.clean_collection)
        
        return {
            "total_records": len(clean_df),
            "unique_users": clean_df["user_id"].nunique(),
            "unique_categories": clean_df["category"].nunique(),
            "total_revenue": round(float(clean_df["total_amount"].sum()), 2),
            "avg_order_value": round(float(clean_df["total_amount"].mean()), 2),
            "date_range_start": str(clean_df["order_date"].min()),
            "date_range_end": str(clean_df["order_date"].max()),
        }
    
    def close(self) -> None:
        """Close MongoDB connection if owned."""
        if self._owns_client:
            self.client.close()
            logger.info("MongoDB connection closed")
