# Detailed Setup Guide

This guide provides step-by-step instructions for setting up the MongoDB Sharded Cluster Big Data Pipeline.

## Prerequisites

### System Requirements

| Requirement | Minimum | Recommended |
|------------|---------|-------------|
| RAM | 8 GB | 16 GB |
| Disk Space | 20 GB | 50 GB |
| CPU Cores | 4 | 8 |

### Software Requirements

1. **Docker** (v20.x or higher)
   - Linux: `sudo apt install docker.io docker-compose`
   - Mac/Windows: [Docker Desktop](https://www.docker.com/products/docker-desktop/)

2. **Python 3.10+**
   - Check: `python --version`

3. **Java 11+** (for PySpark)
   - Ubuntu: `sudo apt install default-jdk`
   - Check: `java -version`

## Installation Steps

### Step 1: Clone the Repository

```bash
git clone https://github.com/yourusername/bigdata-mongodb-project.git
cd bigdata-mongodb-project
```

### Step 2: Install UV Package Manager

```bash
# Linux/macOS
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or using pip
pip install uv
```

### Step 3: Install Python Dependencies

```bash
uv sync
```

This installs all dependencies including dev dependencies for testing.

### Step 4: Configure Environment

```bash
# Copy the template
cp env.template .env

# Edit if needed (defaults work for local development)
nano .env
```

### Step 5: Start MongoDB Cluster

```bash
# Navigate to docker directory
cd docker

# Start all containers (14 total)
docker compose up -d

# Wait for containers to start
docker ps
```

You should see 14 containers running:
- 3 config servers (config1, config2, config3)
- 9 shard servers (3 per shard)
- 2 mongos routers

### Step 6: Initialize Sharding

```bash
# Make script executable (Linux/Mac)
chmod +x setup-cluster.sh

# Run the setup script
./setup-cluster.sh
```

The script will:
1. Initialize config server replica set
2. Initialize 3 shard replica sets
3. Add all shards to the cluster
4. Verify cluster status

### Step 7: Verify Cluster

```bash
# Check sharding status
docker exec mongos1 mongosh --eval "sh.status()"
```

You should see all 3 shards listed as "connected".

## Data Setup

### Option 1: Generate Sample Data

```bash
# Generate 1 million rows (default)
uv run python scripts/generate_data.py

# Or specify row count
uv run python scripts/generate_data.py 500000
```

### Option 2: Use Your Own Dataset

Place your CSV file at `data/raw_data.csv` with these columns:
- `user_id` (int)
- `order_id` (string)
- `product_id` (string)
- `product_name` (string)
- `category` (string)
- `price` (float)
- `quantity` (int)
- `order_date` (string, format: YYYY-MM-DD)
- `status` (string)

## Running the Pipeline

### Setup Database (Optional)

```bash
uv run python scripts/setup_database.py
```

This creates indexes and enables sharding on collections.

### Run Full Pipeline

```bash
uv run python scripts/run_pipeline.py
```

The pipeline will:
1. **Ingest** raw data into MongoDB with sharding
2. **Clean** data using PySpark (remove duplicates, normalize, validate)
3. **Aggregate** data into 5 analytical views

### View Dashboard

```bash
uv run streamlit run dashboard/app.py
```

Open http://localhost:8501 in your browser.

## Troubleshooting

### Docker Issues

**Problem:** Containers won't start
```bash
# Check logs
docker compose logs

# Restart containers
docker compose down
docker compose up -d
```

**Problem:** Port 27017 in use
```bash
# Find process
lsof -i :27017

# Kill it
kill -9 <PID>
```

### MongoDB Issues

**Problem:** Sharding setup fails
```bash
# Re-run setup script
./setup-cluster.sh

# Or manually initialize
docker exec mongos1 mongosh --eval "sh.addShard('shard1rs/shard1-primary:27017')"
```

### PySpark Issues

**Problem:** Java not found
```bash
# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

**Problem:** Out of memory
```bash
# Reduce chunk size in .env
CHUNK_SIZE=50000
```

### Python Issues

**Problem:** Module not found
```bash
# Reinstall dependencies
uv sync

# Or add to PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     MONGOS ROUTERS                          │
│              (Load Balancing & Query Routing)               │
│                 mongos1:27017  mongos2:27018                │
└────────────────────────┬────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
   ┌────▼─────┐    ┌────▼─────┐    ┌────▼─────┐
   │  SHARD 1 │    │  SHARD 2 │    │  SHARD 3 │
   │ 3-node RS│    │ 3-node RS│    │ 3-node RS│
   └──────────┘    └──────────┘    └──────────┘
```

### Data Flow

1. **Ingestion**: CSV → Validation → MongoDB Raw Collection
2. **Cleaning**: MongoDB → PySpark → MongoDB Clean Collection
3. **Aggregation**: MongoDB Clean → PySpark → 5 Aggregation Collections
4. **Visualization**: MongoDB Aggregations → Streamlit Dashboard

## Useful Commands

```bash
# Start cluster
make setup

# Run pipeline
make pipeline

# Run dashboard
make dashboard

# Run tests
make test

# Type checking
make lint

# Stop cluster
make stop

# Clean everything
make clean
```

## Next Steps

1. Run the pipeline: `make pipeline`
2. View the dashboard: `make dashboard`
3. Run tests: `make test`
4. Customize for your dataset: Edit `src/models.py`

