#!/bin/bash

# PostgreSQL Cluster App Deployment Script
# This script sets up the pg_network and deploys the application

set -e  # Exit on error

echo "========================================="
echo "PostgreSQL Cluster App Deployment"
echo "========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

# Step 1: Ensure pg_network exists
echo
echo "Step 1: Checking for pg_network..."
if docker network ls | grep -q pg_network; then
    print_status "pg_network already exists"
else
    print_warning "pg_network not found, creating it..."
    docker network create pg_network
    print_status "pg_network created successfully"
fi

# Step 2: Build the Docker image
echo
echo "Step 2: Building Docker image..."
docker build -t cluster-test-app:latest .

# Step 3: Deploy using docker-compose
echo
echo "Step 3: Deploying with docker-compose..."
docker-compose up -d

# Step 4: Verify deployment
echo
echo "Step 4: Verifying deployment..."
sleep 5  # Give containers time to start

# Check if container is running
CONTAINER_NAME=$(docker-compose ps -q app)
if [ -z "$CONTAINER_NAME" ]; then
    print_error "Container failed to start"
    echo "Checking logs..."
    docker-compose logs app
    exit 1
fi

print_status "Container is running: $CONTAINER_NAME"

# Step 5: Test DNS resolution
echo
echo "Step 5: Testing DNS resolution inside container..."
docker exec "$CONTAINER_NAME" python3 -c "
import socket
hosts = ['postgres-primary', 'pgbouncer', 'postgres-replica-1', 'postgres-replica-2']
all_good = True
for host in hosts:
    try:
        ip = socket.gethostbyname(host)
        print(f'✓ {host} → {ip}')
    except socket.gaierror as e:
        print(f'✗ {host} → FAILED: {e}')
        all_good = False

if all_good:
    print('\\n✅ All hostnames resolve correctly!')
else:
    print('\\n❌ Some hostnames failed to resolve.')
    exit(1)
"

# Step 6: Run the application tests
echo
echo "Step 6: Running application tests..."
echo "========================================="
docker exec "$CONTAINER_NAME" python3 main.py

# Step 7: Show container logs
echo
echo "Step 7: Container logs (last 10 lines):"
echo "========================================="
docker-compose logs --tail=10 app

echo
echo "========================================="
print_status "Deployment completed successfully!"
echo
echo "Next steps:"
echo "1. Set environment variables in Coolify UI:"
echo "   PRIMARY_DB=postgres-primary:5432"
echo "   BOUNCER_DB=pgbouncer:5432"
echo "   REPLICA_DBS=postgres-replica-1:5432,postgres-replica-2:5432"
echo "   DIRECT_PRIMARY_DB=postgres-primary:5432"
echo
echo "2. For manual testing, run:"
echo "   docker exec -it $CONTAINER_NAME python3 test_dns.py"
echo
echo "3. To view logs:"
echo "   docker-compose logs -f app"
echo
echo "4. To stop:"
echo "   docker-compose down"
echo "========================================="
