<#
.SYNOPSIS
    Setup script for Windows to prepare the environment for Docker Compose.
#>

$ErrorActionPreference = "Stop"

Write-Host "Setting up ETL Environment for Windows..."

# 1. Create necessary directories
Write-Host "Creating Airflow and PostgreSQL directories..."
$dirs = @(
    "..\airflow\dags",
    "..\airflow\logs",
    "..\airflow\plugins",
    "..\postgres\data"
)

foreach ($dir in $dirs) {
    $targetPath = Join-Path -Path $PSScriptRoot -ChildPath $dir
    if (!(Test-Path -Path $targetPath)) {
        New-Item -ItemType Directory -Path $targetPath | Out-Null
        Write-Host "Created $targetPath"
    }
}

# 2. Check for .env file
$envFile = Join-Path -Path $PSScriptRoot -ChildPath "..\.env"
$envExample = Join-Path -Path $PSScriptRoot -ChildPath "..\.env.example"

if (!(Test-Path -Path $envFile)) {
    if (Test-Path -Path $envExample) {
        Copy-Item -Path $envExample -Destination $envFile
        Write-Host "Created .env file from .env.example"
    } else {
        Write-Warning ".env.example not found. Please create .env manually."
    }
} else {
    Write-Host ".env file already exists."
}

Write-Host "`nSetup complete! You can now run:"
Write-Host "docker compose up -d" -ForegroundColor Green
