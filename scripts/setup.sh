#!/usr/bin/env bash
set -e

echo "Setting up ETL Environment for Mac/Linux..."

# Go to the project root (one directory up from scripts/)
cd "$(dirname "$0")/.."

# 1. Create necessary directories
echo "Creating Airflow and PostgreSQL directories..."
mkdir -p ./airflow/dags ./airflow/logs ./airflow/plugins ./postgres/data

# 2. Check for .env file
if [ ! -f .env ]; then
    if [ -f .env.example ]; then
        cp .env.example .env
        echo "Created .env file from .env.example"
    else
        echo "Warning: .env.example not found. Please create .env manually."
    fi
else
    echo ".env file already exists."
fi

# 3. Set proper permissions and AIRFLOW_UID for Linux environments
if [ "$(uname)" == "Linux" ]; then
    echo "Setting up permissions for Linux..."
    # Create or update AIRFLOW_UID in .env
    USER_ID=$(id -u)
    if grep -q "AIRFLOW_UID=" .env; then
        # Replace existing AIRFLOW_UID using sed
        sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=${USER_ID}/" .env
    else
        echo "AIRFLOW_UID=${USER_ID}" >> .env
    fi
    echo "AIRFLOW_UID set to ${USER_ID} in .env"
fi

echo -e "\nSetup complete! You can now run:"
echo -e "\033[0;32mdocker compose up -d\033[0m"
