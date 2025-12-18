#!/bin/bash

# MongoDB Sharded Cluster Setup Script
# Works on Windows (Git Bash/WSL), Linux, and macOS

set -e

echo "========================================"
echo "MongoDB Sharded Cluster Initialization"
echo "========================================"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to wait for MongoDB to be ready
wait_for_mongo() {
    local container=$1
    local max_attempts=30
    local attempt=1
    
    echo -e "${BLUE}Waiting for $container to be ready...${NC}"
    while [ $attempt -le $max_attempts ]; do
        if docker exec $container mongosh --quiet --eval "db.runCommand('ping').ok" 2>/dev/null | grep -q "1"; then
            echo -e "${GREEN}$container is ready!${NC}"
            return 0
        fi
        sleep 2
        attempt=$((attempt + 1))
    done
    
    echo -e "${RED}Timeout waiting for $container${NC}"
    return 1
}

# Wait for containers to be ready
echo -e "${BLUE}Waiting for containers to be ready...${NC}"
sleep 10

# Wait for config server
wait_for_mongo "config1"

# Step 1: Initialize Config Server Replica Set
echo -e "${GREEN}Step 1: Initializing Config Server Replica Set...${NC}"
docker exec config1 mongosh --quiet --eval '
rs.initiate({
  _id: "configrs",
  configsvr: true,
  members: [
    { _id: 0, host: "config1:27017" },
    { _id: 1, host: "config2:27017" },
    { _id: 2, host: "config3:27017" }
  ]
});
' || echo -e "${YELLOW}Config RS may already be initialized${NC}"

echo -e "${GREEN}Config servers initialized!${NC}"
sleep 10

# Wait for shard primaries
wait_for_mongo "shard1-primary"
wait_for_mongo "shard2-primary"
wait_for_mongo "shard3-primary"

# Step 2: Initialize Shard 1 Replica Set
echo -e "${GREEN}Step 2: Initializing Shard 1 Replica Set...${NC}"
docker exec shard1-primary mongosh --quiet --eval '
rs.initiate({
  _id: "shard1rs",
  members: [
    { _id: 0, host: "shard1-primary:27017" },
    { _id: 1, host: "shard1-secondary1:27017" },
    { _id: 2, host: "shard1-secondary2:27017" }
  ]
});
' || echo -e "${YELLOW}Shard 1 RS may already be initialized${NC}"

echo -e "${GREEN}Shard 1 initialized!${NC}"
sleep 5

# Step 3: Initialize Shard 2 Replica Set
echo -e "${GREEN}Step 3: Initializing Shard 2 Replica Set...${NC}"
docker exec shard2-primary mongosh --quiet --eval '
rs.initiate({
  _id: "shard2rs",
  members: [
    { _id: 0, host: "shard2-primary:27017" },
    { _id: 1, host: "shard2-secondary1:27017" },
    { _id: 2, host: "shard2-secondary2:27017" }
  ]
});
' || echo -e "${YELLOW}Shard 2 RS may already be initialized${NC}"

echo -e "${GREEN}Shard 2 initialized!${NC}"
sleep 5

# Step 4: Initialize Shard 3 Replica Set
echo -e "${GREEN}Step 4: Initializing Shard 3 Replica Set...${NC}"
docker exec shard3-primary mongosh --quiet --eval '
rs.initiate({
  _id: "shard3rs",
  members: [
    { _id: 0, host: "shard3-primary:27017" },
    { _id: 1, host: "shard3-secondary1:27017" },
    { _id: 2, host: "shard3-secondary2:27017" }
  ]
});
' || echo -e "${YELLOW}Shard 3 RS may already be initialized${NC}"

echo -e "${GREEN}Shard 3 initialized!${NC}"
sleep 15

# Wait for mongos router
wait_for_mongo "mongos1"

# Step 5: Add Shards to Cluster
echo -e "${GREEN}Step 5: Adding shards to the cluster...${NC}"
docker exec mongos1 mongosh --quiet --eval '
sh.addShard("shard1rs/shard1-primary:27017,shard1-secondary1:27017,shard1-secondary2:27017");
sh.addShard("shard2rs/shard2-primary:27017,shard2-secondary1:27017,shard2-secondary2:27017");
sh.addShard("shard3rs/shard3-primary:27017,shard3-secondary1:27017,shard3-secondary2:27017");
' || echo -e "${YELLOW}Shards may already be added${NC}"

echo -e "${GREEN}All shards added!${NC}"
sleep 5

# Step 6: Verify Cluster Status
echo -e "${GREEN}Step 6: Verifying cluster status...${NC}"
docker exec mongos1 mongosh --quiet --eval 'sh.status()'

echo ""
echo -e "${GREEN}========================================"
echo "Cluster Setup Complete!"
echo "========================================${NC}"
echo ""
echo "Connection strings:"
echo "  Primary Mongos: mongodb://localhost:27019"
echo "  Secondary Mongos: mongodb://localhost:27020"
echo ""
echo "To enable sharding on your database, run:"
echo "  docker exec mongos1 mongosh"
echo "  sh.enableSharding('your_database_name')"
echo ""
echo "Or run the database setup script:"
echo "  uv run python scripts/setup_database.py"
echo ""
