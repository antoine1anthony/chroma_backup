# ChromaDB Backup and Recovery System

## Overview
This project provides a backup and recovery system for ChromaDB, integrating with PostgreSQL for persistence. It schedules regular exports, performs health checks, and allows for recovery of vector data from PostgreSQL back into ChromaDB.

## Features
- **Automated Export**: Periodically exports ChromaDB collection data to PostgreSQL.
- **Health Monitoring**: Runs health checks on ChromaDB collections.
- **Automated Recovery**: Imports data from PostgreSQL into a new ChromaDB collection if issues are detected.
- **Dockerized Setup**: Uses Docker for PostgreSQL and application containerization.

## Technologies Used
- **Python 3.9**
- **ChromaDB 0.6.3** (Vector Database)
- **PostgreSQL 14**
- **Docker & Docker Compose**
- **Schedule** (Python job scheduling library)
- **Requests** (API interaction)
- **Dotenv** (Environment variable management)
- **Logging** (For application monitoring)

## Installation & Setup

### Prerequisites
- Install [Docker](https://www.docker.com/get-started)
- Install [Docker Compose](https://docs.docker.com/compose/)

### Environment Variables
Create a `.env` file in the project root with the following variables:
```ini
DB_NAME=your_database
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=postgres
CHROMA_COLLECTION_NAME=my_collection
BACKUP_TABLE=chroma_data
NEW_COLLECTION_NAME=imported_collection
CHROMADB_HOST=localhost
CHROMADB_PORT=8000
CHROMADB_USE_SSL=False
```

### Running the Project
1. **Build and start the services**:
   ```sh
   docker compose up --build
   ```
2. **Verify logs**:
   ```sh
   docker logs -f <container_id>
   ```

## Components

### `main.py`
The entry point that initializes the scheduler to:
- Run exports every hour (`export_collection_to_postgres`)
- Check ChromaDB health every 20 minutes (`check_collection_health`)

### `export_import.py`
- **`export_collection_to_postgres()`**: Fetches embeddings from ChromaDB and stores them in PostgreSQL.
- **`import_postgres_to_chroma()`**: Retrieves stored embeddings from PostgreSQL and inserts them into a new ChromaDB collection.
- **`check_collection_health()`**: Verifies if ChromaDB is responding, and triggers import if issues are found.

### `db.py`
- Provides a database connection utility for PostgreSQL.

## Docker Configuration
### `docker-compose.yml`
Defines services:
- **`postgres`**: Runs a PostgreSQL container.
- **`app`**: Runs the Python scheduler and backup logic.

### `Dockerfile`
- Uses Python 3.9 slim image.
- Installs required dependencies.
- Copies application code and runs `main.py`.

## Requirements
Install dependencies locally if needed:
```sh
pip install -r requirements.txt
```

## Usage
This project continuously monitors and backs up ChromaDB collections. If failures occur, it restores data from PostgreSQL. Logs will provide details on execution status.

## License
TBA
