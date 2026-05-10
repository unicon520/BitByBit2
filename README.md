# Local ETL Development Platform

A beginner-friendly, fully containerized ETL environment using Docker, PostgreSQL, and Apache Airflow.

This project gives you an instantly working data engineering workspace. It comes pre-configured with a PostgreSQL database containing sample data and an Airflow instance connected to it.

## 🚀 Quick Start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- Git installed

### Installation Steps

1. **Clone the repository (or navigate to this folder)**
   ```bash
   cd etl-project
   ```

2. **Run the setup script**
   - **Windows:** Open PowerShell and run:
     ```powershell
     .\scripts\setup.ps1
     ```
   - **Mac/Linux:** Open terminal and run:
     ```bash
     sh ./scripts/setup.sh
     ```
   *This script creates the necessary `.env` file and local directories for data persistence.*

3. **Start the environment**
   ```bash
   docker compose up -d
   ```
   *Note: The first time you run this, it will take a few minutes to download the Docker images and initialize the Airflow database.*

---

## 🧭 Accessing the Services

Once all containers are running and healthy, you can access the following services in your web browser:

| Service | URL | Username | Password |
|---|---|---|---|
| **Apache Airflow** | [http://localhost:8080](http://localhost:8080) | `admin` | `admin` |
| **pgAdmin (Database UI)** | [http://localhost:5050](http://localhost:5050) | `admin@admin.com` | `admin` |

### Database Connection Details (for external tools like DBeaver)
- **Host:** `localhost`
- **Port:** `5432`
- **Database Name:** `airflow`
- **Username:** `airflow`
- **Password:** `airflow`

---

## 🛠️ How to Use This Project

### 1. View the Sample Data
A PostgreSQL initialization script (`postgres/init.sql`) automatically runs the first time the database starts. It creates:
- A schema named `sample_data`
- A table `raw_orders` with sample seed data
- An empty table `processed_orders`

You can view these tables using pgAdmin or by connecting directly to the database.

### 2. Run the Example ETL Pipeline
1. Open the Airflow UI at [http://localhost:8080](http://localhost:8080) and log in.
2. Locate the DAG named `example_postgres_etl`.
3. Unpause the DAG using the toggle button on the left.
4. Click the "Trigger DAG" (Play icon) button on the right to run it manually.
5. Watch the tasks succeed. This pipeline reads from `raw_orders`, calculates totals, and inserts the result into `processed_orders`.

### 3. Add New DAGs
Any Python files you place inside the `airflow/dags/` folder will automatically be picked up by Airflow. 
- You do not need to restart Docker to see new DAGs.
- Use `airflow/dags/example_etl_pipeline.py` as a template for building your own pipelines.

### 4. Install Extra Python Packages
If your DAGs require additional Python packages (like `pandas` or `requests`):
1. Add the package names to `airflow/requirements.txt`.
2. Stop the containers: `docker compose down`
3. Rebuild the Airflow image: `docker compose up -d --build`

---

## 🛑 Stopping the Environment

To stop the containers without losing your data:
```bash
docker compose stop
```

To stop and remove the containers, networks, and volumes (this will **delete** your database data):
```bash
docker compose down -v
```

---

## 🔧 Troubleshooting

- **Airflow UI is not loading?**
  Run `docker compose ps` to check if `airflow-webserver` is healthy. It can take up to 2 minutes for Airflow to fully initialize on the first run.
- **Permission Errors on Linux?**
  Make sure you ran `sh ./scripts/setup.sh`. It automatically configures the `AIRFLOW_UID` in your `.env` file to match your host user, preventing permission issues with mounted volumes.
- **Cannot connect to Postgres?**
  Ensure port `5432` is not already in use by a local installation of PostgreSQL on your computer. If it is, either stop your local Postgres or map the container to a different port in `docker-compose.yml` (e.g., `"5433:5432"`).
