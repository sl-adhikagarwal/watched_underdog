#!/bin/bash

echo "Starting Intelligent Cloud Storage System..."
echo "================================================"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Please start Docker Desktop first."
    exit 1
fi

echo "Docker is running"
echo ""

# Check if required ports are available
echo "Checking if required ports are available..."
PORTS=(8000 8501 9000 9001 9092 2181)
PORT_CONFLICT=false

for port in "${PORTS[@]}"; do
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1 ; then
        echo "Port $port is already in use"
        PORT_CONFLICT=true
    else
        echo "Port $port is available"
    fi
done

if [ "$PORT_CONFLICT" = true ]; then
    echo ""
    echo "Some ports are already in use. Please stop those services or change the ports in docker-compose.yml"
    echo ""
    read -p "Do you want to continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo ""
echo "Building and starting Docker containers..."
echo "This may take 3-5 minutes on first run..."
echo ""

# Start Docker Compose
docker compose up --build -d

echo ""
echo "Waiting for services to initialize..."
echo ""

# Wait for backend to be healthy
echo "Waiting for backend..."
for i in {1..30}; do
    if curl -s http://localhost:8000/health > /dev/null 2>&1; then
        echo "Backend is ready"
        break
    fi
    echo -n "."
    sleep 2
done

echo ""
echo "Waiting for dashboard..."
for i in {1..30}; do
    if curl -s http://localhost:8501 > /dev/null 2>&1; then
        echo "Dashboard is ready"
        break
    fi
    echo -n "."
    sleep 2
done

echo ""
echo ""
echo "================================================"
echo "System is ready!"
echo "================================================"
echo ""
echo "Dashboard:          http://localhost:8501"
echo "API Docs:           http://localhost:8000/docs"
echo "MinIO Console:      http://localhost:9001"
echo "    Username: minioadmin"
echo "    Password: minioadmin123"
echo ""
echo "================================================"
echo ""
echo "Useful Commands:"
echo "  - View logs:        docker compose logs -f"
echo "  - Stop system:      docker compose down"
echo "  - Restart service:  docker compose restart [service]"
echo "  - View status:      docker compose ps"
echo ""
echo "Press Ctrl+C to stop viewing logs"
echo ""

# Follow logs
docker compose logs -f
