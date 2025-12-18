"""Database setup script for initializing MongoDB collections and sharding."""

import sys
from pathlib import Path

from loguru import logger
from pymongo import MongoClient
from pymongo.errors import OperationFailure

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import settings


def setup_logging() -> None:
    """Configure logging for the setup script."""
    logger.remove()
    logger.add(
        sys.stderr,
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
    )


def check_connection(client: MongoClient) -> bool:
    """Check if MongoDB connection is working.
    
    Args:
        client: MongoDB client instance
        
    Returns:
        True if connected, False otherwise
    """
    try:
        client.admin.command("ping")
        logger.info("Successfully connected to MongoDB")
        return True
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return False


def check_sharding_status(client: MongoClient) -> dict:
    """Check the status of sharding in the cluster.
    
    Args:
        client: MongoDB client instance
        
    Returns:
        Dictionary with sharding status information
    """
    try:
        status = client.admin.command("listShards")
        shards = status.get("shards", [])
        
        logger.info(f"Found {len(shards)} shard(s) in the cluster:")
        for shard in shards:
            logger.info(f"  - {shard['_id']}: {shard['host']}")
        
        return {
            "sharding_enabled": len(shards) > 0,
            "shard_count": len(shards),
            "shards": shards
        }
    except OperationFailure as e:
        logger.warning(f"Sharding status check failed (may not be a sharded cluster): {e}")
        return {
            "sharding_enabled": False,
            "shard_count": 0,
            "shards": []
        }


def enable_database_sharding(client: MongoClient, database: str) -> bool:
    """Enable sharding on a database.
    
    Args:
        client: MongoDB client instance
        database: Database name
        
    Returns:
        True if successful, False otherwise
    """
    try:
        client.admin.command("enableSharding", database)
        logger.info(f"Sharding enabled on database: {database}")
        return True
    except OperationFailure as e:
        if "already enabled" in str(e).lower():
            logger.info(f"Sharding already enabled on database: {database}")
            return True
        logger.error(f"Failed to enable sharding on database: {e}")
        return False


def create_indexes(client: MongoClient, database: str) -> None:
    """Create indexes for collections.
    
    Args:
        client: MongoDB client instance
        database: Database name
    """
    db = client[database]
    
    # Indexes for raw_data collection
    raw_collection = db[settings.raw_collection]
    raw_collection.create_index([("user_id", 1)])
    raw_collection.create_index([("order_date", -1)])
    raw_collection.create_index([("category", 1)])
    logger.info(f"Created indexes on {settings.raw_collection}")
    
    # Indexes for clean_data collection
    clean_collection = db[settings.clean_collection]
    clean_collection.create_index([("user_id", 1)])
    clean_collection.create_index([("order_date", -1)])
    clean_collection.create_index([("category", 1)])
    clean_collection.create_index([("status", 1)])
    clean_collection.create_index([("year", 1), ("month", 1)])
    logger.info(f"Created indexes on {settings.clean_collection}")


def shard_collection(
    client: MongoClient, 
    database: str, 
    collection: str, 
    shard_key: str
) -> bool:
    """Shard a collection using hash-based sharding.
    
    Args:
        client: MongoDB client instance
        database: Database name
        collection: Collection name
        shard_key: Field to shard on
        
    Returns:
        True if successful, False otherwise
    """
    namespace = f"{database}.{collection}"
    
    try:
        # Create index on shard key first
        db = client[database]
        db[collection].create_index([(shard_key, "hashed")])
        
        # Shard the collection
        client.admin.command({
            "shardCollection": namespace,
            "key": {shard_key: "hashed"}
        })
        logger.info(f"Collection sharded: {namespace} on {shard_key} (hashed)")
        return True
    except OperationFailure as e:
        if "already sharded" in str(e).lower():
            logger.info(f"Collection already sharded: {namespace}")
            return True
        logger.error(f"Failed to shard collection {namespace}: {e}")
        return False


def drop_database(client: MongoClient, database: str) -> bool:
    """Drop a database (use with caution!).
    
    Args:
        client: MongoDB client instance
        database: Database name to drop
        
    Returns:
        True if successful, False otherwise
    """
    try:
        client.drop_database(database)
        logger.info(f"Dropped database: {database}")
        return True
    except Exception as e:
        logger.error(f"Failed to drop database: {e}")
        return False


def get_collection_stats(client: MongoClient, database: str) -> dict:
    """Get statistics for all collections in the database.
    
    Args:
        client: MongoDB client instance
        database: Database name
        
    Returns:
        Dictionary with collection statistics
    """
    db = client[database]
    stats = {}
    
    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        count = collection.count_documents({})
        stats[collection_name] = {
            "document_count": count,
            "indexes": list(collection.index_information().keys())
        }
        logger.info(f"  - {collection_name}: {count:,} documents")
    
    return stats


def main() -> None:
    """Main setup function."""
    setup_logging()
    
    logger.info("=" * 50)
    logger.info("MongoDB Database Setup")
    logger.info("=" * 50)
    logger.info(f"URI: {settings.mongodb_uri}")
    logger.info(f"Database: {settings.mongodb_database}")
    
    # Connect to MongoDB
    client = MongoClient(settings.mongodb_uri)
    
    if not check_connection(client):
        logger.error("Cannot proceed without MongoDB connection")
        sys.exit(1)
    
    # Check sharding status
    logger.info("\n--- Sharding Status ---")
    shard_status = check_sharding_status(client)
    
    if shard_status["sharding_enabled"]:
        # Enable sharding on database
        logger.info("\n--- Enabling Database Sharding ---")
        enable_database_sharding(client, settings.mongodb_database)
        
        # Shard the raw collection
        logger.info("\n--- Sharding Collections ---")
        shard_collection(
            client, 
            settings.mongodb_database, 
            settings.raw_collection,
            settings.shard_key
        )
    else:
        logger.warning("Cluster is not sharded. Running in standalone mode.")
    
    # Create indexes
    logger.info("\n--- Creating Indexes ---")
    create_indexes(client, settings.mongodb_database)
    
    # Show collection stats
    logger.info("\n--- Collection Statistics ---")
    get_collection_stats(client, settings.mongodb_database)
    
    logger.info("\n" + "=" * 50)
    logger.info("Database setup complete!")
    logger.info("=" * 50)
    
    client.close()


if __name__ == "__main__":
    main()

