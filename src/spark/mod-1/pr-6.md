# pr-6: Spark Cluster with Docker Deployment

## Prerequisites
- Docker
- Docker Compose
- Git

## Environment Setup

### 1. Clone the Repository
```bash
git clone <repository-url>
cd <repository-directory>
```

### 2. Navigate to Build Directory
```bash
cd build
```

### 3. Create .env File
Create a `.env` file in the build directory with the following content:
```bash
APP_SRC_PATH=/absolute/path/to/repo/build/src
APP_STORAGE_PATH=/absolute/path/to/repo/build/storage
APP_LOG_PATH=/absolute/path/to/repo/build/logs
APP_METRICS_PATH=/absolute/path/to/repo/build/metrics
```

**Note:** Replace `/absolute/path/to/repo/` with the full path to your project directory.

### 4. Create Required Directories
```bash
mkdir -p src storage logs metrics
```

### 5. Build Docker Images
```bash
docker build -t owshq-spark:3.5 -f Dockerfile.spark .
docker build -t owshq-spark-history-server:3.5 -f Dockerfile.history .
```

### 6. Start Spark Cluster
```bash
docker-compose up -d
```

### 7. Verify Deployment
```bash
docker ps

docker logs spark-master
docker logs spark-worker-1
docker logs spark-worker-2
docker logs spark-history-server
```

### 8. Stop Spark Cluster
```bash
docker-compose down
```

## Cluster Components
- **Spark Master**: Runs on port 8080
- **Spark Workers**: 3 workers configured
- **Spark History Server**: Runs on port 18080

## Accessing Services
- Spark Master UI: http://localhost:8080
- Spark History Server: http://localhost:18080

## Included Technologies
- Spark 3.5.0
- Python 3
- PySpark
- Pandas
- Delta Lake
- Apache Arrow

## Troubleshooting
- Ensure all paths in `.env` are absolute and correct
- Check Docker and Docker Compose versions
- Verify network ports are not in use by other services

## Configuration Files
- `docker-compose.yml`: Defines the multi-container Spark cluster
- `Dockerfile.spark`: Builds the base Spark image
- `Dockerfile.history`: Builds the Spark History Server image
- `config/spark/spark-defaults.conf`: Spark configuration
- `config/spark/log4j2.properties`: Logging configuration
