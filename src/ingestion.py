"""Data ingestion module for loading data into MongoDB."""

import time
from typing import Any, Iterator
from pathlib import Path
import pandas as pd
from pymongo import MongoClient, errors
from pymongo.collection import Collection
from loguru import logger

from src.config import settings
from src.models import RawDataModel, PipelineMetrics


class DataIngestion:
    """Handle data ingestion into MongoDB."""
    
    def __init__(self) -> None:
        """Initialize MongoDB connection."""
        self.client = MongoClient(settings.mongodb_uri)
        self.db = self.client[settings.mongodb_database]
        self.collection: Collection = self.db[settings.raw_collection]
        logger.info(f"Connected to MongoDB: {settings.mongodb_uri}")
    
    def setup_sharding(self) -> None:
        """Enable sharding on database and collection."""
        try:
            admin_db = self.client.admin
            
            # Enable sharding on database
            admin_db.command({"enableSharding": settings.mongodb_database})
            logger.info(f"Sharding enabled on database: {settings.mongodb_database}")
            
            # Create index on shard key
            self.collection.create_index([(settings.shard_key, 1)])
            logger.info(f"Index created on shard key: {settings.shard_key}")
            
            # Shard the collection
            admin_db.command({
                "shardCollection": f"{settings.mongodb_database}.{settings.raw_collection}",
                "key": {settings.shard_key: "hashed"}
            })
            logger.info(f"Collection sharded: {settings.raw_collection}")
            
        except errors.OperationFailure as e:
            if "already enabled" in str(e).lower():
                logger.info("Sharding already enabled")
            else:
                logger.warning(f"Sharding setup warning: {e}")
    
    def validate_and_chunk_data(
        self, 
        df: pd.DataFrame, 
        chunk_size: int
    ) -> Iterator[list[dict[str, Any]]]:
        """Validate data and yield chunks."""
        total_rows = len(df)
        valid_records = []
        failed_count = 0
        
        for idx, row in df.iterrows():
            try:
                # Validate using Pydantic model
                validated = RawDataModel(**row.to_dict())
                valid_records.append(validated.model_dump(mode='json'))
                
                # Yield chunk when size reached
                if len(valid_records) >= chunk_size:
                    yield valid_records
                    valid_records = []
                    
            except Exception as e:
                failed_count += 1
                if failed_count <= 5:  # Log first 5 failures
                    logger.warning(f"Validation failed for row {idx}: {e}")
        
        # Yield remaining records
        if valid_records:
            yield valid_records
        
        logger.info(
            f"Validation complete: {total_rows - failed_count}/{total_rows} records valid"
        )
    
    def ingest_from_csv(self, file_path: str | Path) -> PipelineMetrics:
        """Ingest data from CSV file.
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            PipelineMetrics with ingestion statistics
        """
        start_time = time.time()
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        logger.info(f"Starting ingestion from: {file_path}")
        
        # Read CSV in chunks
        total_inserted = 0
        total_failed = 0
        
        try:
            # Read entire CSV first (for smaller files) or use chunksize for large files
            df = pd.read_csv(file_path)
            logger.info(f"Loaded {len(df)} rows from CSV")
            
            # Validate and insert in chunks
            for chunk in self.validate_and_chunk_data(df, settings.chunk_size):
                try:
                    result = self.collection.insert_many(chunk, ordered=False)
                    total_inserted += len(result.inserted_ids)
                    logger.info(f"Inserted {len(result.inserted_ids)} records")
                except errors.BulkWriteError as e:
                    total_failed += len(e.details.get('writeErrors', []))
                    logger.error(f"Bulk insert error: {e.details}")
            
            execution_time = time.time() - start_time
            
            metrics = PipelineMetrics(
                stage="ingestion",
                records_processed=total_inserted,
                records_failed=total_failed,
                execution_time_seconds=execution_time
            )
            
            logger.info(
                f"Ingestion complete: {total_inserted} records in {execution_time:.2f}s"
            )
            
            return metrics
            
        except Exception as e:
            logger.error(f"Ingestion failed: {e}")
            raise
    
    def get_row_count(self) -> int:
        """Get total row count from collection."""
        count = self.collection.count_documents({})
        logger.info(f"Total documents in {settings.raw_collection}: {count}")
        return count
    
    def get_schema_info(self) -> dict[str, Any]:
        """Get schema information from collection."""
        pipeline = [
            {"$limit": 1000},
            {"$project": {
                field: {"$type": f"${field}"}
                for field in RawDataModel.model_fields.keys()
            }}
        ]
        
        sample_doc = self.collection.find_one()
        if sample_doc:
            return {
                "sample_document": sample_doc,
                "field_count": len(sample_doc),
                "fields": list(sample_doc.keys())
            }
        return {"error": "No documents found"}
    
    def close(self) -> None:
        """Close MongoDB connection."""
        self.client.close()
        logger.info("MongoDB connection closed")