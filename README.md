# MongoDB Sharded Cluster Big Data Pipeline

A production-ready distributed big data processing pipeline using MongoDB sharded cluster, Pandas, and Streamlit for analytics visualization.

## Architecture

```
                        MONGOS ROUTERS
              (Load Balancing & Query Routing)
                 mongos1:27019  mongos2:27020
                            |
        +-------------------+-------------------+
        |                   |                   |
   +----v-----+        +----v-----+        +----v-----+
   |  SHARD 1 |        |  SHARD 2 |        |  SHARD 3 |
   | Replica  |        | Replica  |        | Replica  |
   |   Set    |        |   Set    |        |   Set    |
   | (3 nodes)|        | (3 nodes)|        | (3 nodes)|
   +----------+        +----------+        +----------+
        |                   |                   |
        +-------------------+-------------------+
                            |
               +------------v------------+
               |     CONFIG SERVERS      |
               |      (3 node RS)        |
               |     Metadata Store      |
               +-------------------------+

DATA PROCESSING LAYER:
+----------------------------------------------------------+
|  Pandas Processing Engine                                 |
|  - Raw Data Ingestion                                     |
|  - Data Cleaning & Validation (Pydantic)                  |
|  - Aggregation (Category, Time, Status, User)             |
+----------------------------------------------------------+

VISUALIZATION LAYER:
+----------------------------------------------------------+
|  Streamlit Dashboard                                      |
|  - Real-time Analytics from Aggregated Collections        |
+----------------------------------------------------------+
```

### Architecture Components

- **3 Shards**: Each shard is a 3-node replica set (1 primary, 2 secondaries)
- **3 Config Servers**: Store cluster metadata in a replica set
- **2 Mongos Routers**: Distribute queries across shards
- **Hash-based Sharding**: Data distributed using hashed shard key
- **Total Containers**: 14 MongoDB containers

## Features

- MongoDB 7.0 sharded cluster with replica sets
- Pandas for data processing
- Pydantic v2 for schema validation
- Mypy for type checking
- PyTest with coverage
- Structured logging with Loguru
- UV for modern Python project management
- Interactive Streamlit dashboard
- Cross-platform support (Windows, macOS, Linux)
- Docker Compose for easy deployment

## Prerequisites

- **Docker Desktop** (Windows/Mac) or **Docker Engine** (Linux)
- **Python 3.10+**
- **Git**
- **8GB RAM minimum** (16GB recommended)
- **20GB free disk space**

### Windows Prerequisites
- Windows 10/11 with WSL2
- Docker Desktop for Windows
- Git Bash or WSL terminal

### Linux/macOS Prerequisites
- Docker and Docker Compose installed
- Bash shell

## Installation

### Step 1: Clone Repository

```bash
git clone https://github.com/yourusername/bigdata-mongodb-project.git
cd bigdata-mongodb-project
```

### Step 2: Install UV

```bash
# Windows (PowerShell)
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Linux/macOS
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or using pip
pip install uv
```

### Step 3: Install Python Dependencies

```bash
uv sync
```

### Step 4: Setup Environment

```bash
# Copy environment template
cp env.template .env

# Edit .env if needed (default values work out of the box)
```

## Setup & Configuration

### 1. Start MongoDB Cluster

```bash
# Navigate to docker directory
cd docker

# Start all containers
docker compose up -d

# Wait for containers to be healthy (about 30 seconds)
docker ps
```

### 2. Initialize Sharding

**Linux/macOS:**
```bash
chmod +x setup-cluster.sh
./setup-cluster.sh
```

**Windows (Git Bash or WSL):**
```bash
bash setup-cluster.sh
```

### 3. Verify Cluster Status

```bash
docker exec mongos1 mongosh --eval "sh.status()"
```

You should see all 3 shards listed and active.

## Dataset Requirements

Place your dataset in `data/raw_data.csv` or generate sample data:

```bash
# Generate 1M rows of sample data
uv run python scripts/generate_data.py
```

**Required columns:**
- `user_id` (int)
- `order_id` (str)
- `product_id` (str)
- `product_name` (str)
- `category` (str)
- `price` (float)
- `quantity` (int)
- `order_date` (str, e.g., "2024-01-15")
- `status` (str)

## Running the Pipeline

### Full Pipeline Execution

```bash
# Run complete pipeline (ingestion -> cleaning -> aggregation)
uv run python scripts/run_pipeline.py
```

This will:
1. Ingest raw data into MongoDB
2. Enable sharding on collection
3. Clean data using Pandas
4. Validate with Pydantic models
5. Generate aggregations
6. Store results in MongoDB

### View Logs

```bash
tail -f pipeline.log
```

## Visualization Dashboard

### Start Dashboard

```bash
uv run streamlit run dashboard/app.py
```

Open browser to: http://localhost:8501

### Dashboard Features

- **Overview**: KPIs, revenue charts, status distribution
- **Category Analysis**: Revenue by category, customer engagement
- **Time Series**: Monthly trends, day-of-week patterns
- **Customer Insights**: Top customers, order frequency
- **Status Distribution**: Order status breakdown

## Testing

### Run All Tests

```bash
uv run pytest
```

### Run Specific Test

```bash
uv run pytest tests/test_ingestion.py -v
```

### Coverage Report

```bash
uv run pytest --cov=src --cov-report=html
# Open htmlcov/index.html
```

## Code Quality

### Type Checking

```bash
uv run mypy src/
```

### Linting

```bash
uv run ruff check src/
```

### Formatting

```bash
uv run black src/ tests/
```

## Project Structure

```
bigdata-mongodb-project/
├── src/                       # Source code
│   ├── __init__.py
│   ├── config.py             # Configuration management
│   ├── models.py             # Pydantic schemas
│   ├── ingestion.py          # Data ingestion
│   ├── cleaning.py           # Pandas cleaning
│   ├── aggregation.py        # Pandas aggregation
│   └── utils.py              # Utilities
├── tests/                     # Test suite
│   ├── test_ingestion.py
│   ├── test_cleaning.py
│   └── test_aggregation.py
├── docker/                    # Docker configuration
│   ├── docker-compose.yml
│   └── setup-cluster.sh
├── dashboard/                 # Streamlit app
│   └── app.py
├── scripts/                   # Executable scripts
│   ├── run_pipeline.py
│   ├── generate_data.py
│   └── setup_database.py
├── data/                      # Data directory
│   └── raw_data.csv
├── docs/                      # Documentation
├── pyproject.toml            # Project metadata
├── env.template              # Environment template
├── README.md                 # This file
└── Makefile                  # Make commands
```

## Key Performance Indicators

After running the pipeline, you'll see:

- **Total rows ingested**: Displayed in logs
- **Cleaning success rate**: Percentage of valid records
- **Execution time**: Per stage and total
- **Aggregation counts**: Per dimension (category, month, etc.)

## Troubleshooting

### MongoDB Connection Issues

```bash
# Check if containers are running
docker ps

# Restart specific container
docker restart mongos1

# View container logs
docker logs mongos1
```

### Sharding Not Working

```bash
# Verify sharding status
docker exec mongos1 mongosh --eval "sh.status()"

# Re-run setup script
bash docker/setup-cluster.sh
```

### Port Already in Use

The default ports are 27019 and 27020. If needed, change in `docker-compose.yml`:
```yaml
ports:
  - "27021:27017"  # Use different host port
```

## Stopping & Cleanup

### Stop Cluster

```bash
# Stop containers (keeps data)
docker compose -f docker/docker-compose.yml down

# Stop and remove volumes (delete all data)
docker compose -f docker/docker-compose.yml down -v
```

### Clean All

```bash
make clean
```

## Learning Outcomes

This project demonstrates:

1. **Distributed Systems**: Sharding, replica sets, horizontal scaling
2. **Big Data Processing**: Pandas for large-scale ETL
3. **Schema Validation**: Pydantic for data quality
4. **Type Safety**: Mypy for production code
5. **Testing**: Comprehensive test coverage
6. **DevOps**: Docker, containerization, orchestration
7. **Data Visualization**: Interactive dashboards
8. **Best Practices**: Project structure, logging, error handling

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing`)
5. Open Pull Request

## License

MIT License - see LICENSE file

## Team Members

- Member 1: [Role]
- Member 2: [Role]
- Member 3: [Role]

## Video Presentation

[Link to YouTube video (unlisted)]

## Support

For issues or questions:
- Open GitHub issue
- Contact: your-email@example.com
