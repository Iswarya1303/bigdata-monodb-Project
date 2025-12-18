# Quick Start Guide

Get your MongoDB sharded cluster and big data pipeline running in under 10 minutes.

## Prerequisites Check

```bash
# Check Docker is installed
docker --version
# Should show: Docker version 20.x.x or higher

# Check Docker is running
docker ps
# Should show running containers or empty list (not error)

# Check Python version
python --version
# Should show: Python 3.10 or higher
```

**Not installed?**
- **Docker**: Download [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- **Python**: Download [Python 3.10+](https://www.python.org/downloads/)

## Step 1: Clone & Setup

```bash
# Clone repository
git clone https://github.com/yourusername/bigdata-mongodb-project.git
cd bigdata-mongodb-project

# Install UV
pip install uv

# Install dependencies
uv sync

# Copy environment file
cp env.template .env
```

## Step 2: Start MongoDB Cluster

### Linux/macOS
```bash
# Start containers
cd docker
docker compose up -d

# Wait 30 seconds, then initialize
cd ..
bash docker/setup-cluster.sh
```

### Windows (Git Bash or WSL)
```bash
cd docker
docker compose up -d
cd ..
bash docker/setup-cluster.sh
```

**Verify it worked:**
```bash
docker exec mongos1 mongosh --eval "sh.status()"
# Should show 3 shards active
```

## Step 3: Run Pipeline

```bash
uv run python scripts/run_pipeline.py
```

**What happens:**
1. Ingests data into MongoDB with sharding
2. Cleans data using Pandas
3. Validates with Pydantic
4. Generates 5 aggregation views
5. Saves to MongoDB

**Expected output:**
```
========================================
STAGE 1: DATA INGESTION
========================================
Total rows in raw collection: 1,000,000

========================================
STAGE 2: DATA CLEANING
========================================
Rows after cleaning: 996,668

========================================
STAGE 3: DATA AGGREGATION
========================================
Category aggregation: 5 categories
Monthly aggregation: 12 months
...

Pipeline completed successfully!
```

## Step 4: View Dashboard

```bash
uv run streamlit run dashboard/app.py
```

Open browser to: **http://localhost:8501**

**Dashboard includes:**
- Executive KPIs
- Revenue trends
- Category analysis
- Customer insights
- Time series

## Common Issues & Fixes

### Issue: "Port 27017 already in use"
```bash
# The project uses ports 27019 and 27020 by default
# If those are also in use, edit docker/docker-compose.yml
```

### Issue: "Cannot connect to Docker daemon"
```bash
# Linux: Start Docker service
sudo systemctl start docker

# Or set DOCKER_HOST for system Docker
export DOCKER_HOST=unix:///run/docker.sock
```

### Issue: "Sharding setup failed"
```bash
# Re-run setup script
bash docker/setup-cluster.sh
```

### Issue: "No module named 'src'"
```bash
# Install dependencies again
uv sync
```

## Quick Verification Commands

```bash
# Check all containers running
docker ps | grep mongo

# Check shard status
docker exec mongos1 mongosh --eval "sh.status()"

# Check data counts
docker exec mongos1 mongosh bigdata_project --eval "db.raw_data.countDocuments({})"

# View aggregations
docker exec mongos1 mongosh bigdata_project --eval "db.aggregated_data_category.find().pretty()"
```

## Run Tests

```bash
# Run all tests
uv run pytest

# With coverage
uv run pytest --cov=src

# Type checking
uv run mypy src/
```

## Cleanup

```bash
# Stop containers (keep data)
docker compose -f docker/docker-compose.yml down

# Stop and DELETE all data
docker compose -f docker/docker-compose.yml down -v
```

## Next Steps

1. **Customize for your dataset**: Edit `src/models.py`
2. **Add more aggregations**: Extend `src/aggregation.py`
3. **Custom visualizations**: Modify `dashboard/app.py`
4. **Production deployment**: Use managed MongoDB Atlas

## Getting Help

- **Documentation**: See [README.md](README.md)
- **Logs**: Check `pipeline.log`
- **Docker logs**: `docker logs <container_name>`
- **Issues**: Open GitHub issue

## Success Checklist

- [ ] 14 MongoDB containers running
- [ ] Sharding enabled and verified
- [ ] 750K+ rows ingested
- [ ] Data cleaned and validated
- [ ] 5 aggregation collections created
- [ ] Dashboard running with visualizations
- [ ] All tests passing
