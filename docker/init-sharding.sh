#!/bin/bash
# This script is called automatically after cluster setup
# It initializes sharding on the default database

echo "Initializing sharding on bigdata_project database..."

mongosh --host mongos1 --port 27017 --eval '
// Enable sharding on the database
sh.enableSharding("bigdata_project");

// Create indexes on raw_data collection
db = db.getSiblingDB("bigdata_project");
db.raw_data.createIndex({"user_id": "hashed"});

// Shard the collection
sh.shardCollection("bigdata_project.raw_data", {"user_id": "hashed"});

print("Sharding initialized successfully!");
'

