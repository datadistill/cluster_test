#!/bin/bash

# Quick DNS Verification Script
# Run this inside the app container to verify DNS resolution

echo "========================================="
echo "PostgreSQL Cluster DNS Verification"
echo "========================================="

# Test DNS resolution for key hostnames
echo "Testing DNS resolution..."
echo ""

HOSTNAMES=("postgres-primary" "pgbouncer" "postgres-replica-1" "postgres-replica-2")
ALL_GOOD=true

for host in "${HOSTNAMES[@]}"; do
    if python3 -c "
import socket
try:
    ip = socket.gethostbyname('$host')
    print(f'✓ $host → {ip}')
except socket.gaierror as e:
    print(f'✗ $host → FAILED: {e}')
    exit(1)
" 2>/dev/null; then
        : # Success
    else
        ALL_GOOD=false
    fi
done

echo ""
echo "========================================="

if $ALL_GOOD; then
    echo "✅ SUCCESS: All hostnames resolve correctly!"
    echo ""
    echo "Network configuration is working properly."
    echo "The app container can communicate with the PostgreSQL cluster."
else
    echo "❌ FAILURE: Some hostnames failed to resolve."
    echo ""
    echo "Troubleshooting steps:"
    echo "1. Check if container is on pg_network:"
    echo "   docker inspect \$(hostname) | grep -A 5 Networks"
    echo ""
    echo "2. Verify pg_network exists:"
    echo "   docker network ls | grep pg_network"
    echo ""
    echo "3. Check PostgreSQL containers are running:"
    echo "   docker ps | grep postgres"
    echo ""
    echo "4. Inspect network connections:"
    echo "   docker network inspect pg_network"
    echo ""
    echo "5. Ensure docker-compose.yaml includes pg_network"
    exit 1
fi

# Optional: Test port connectivity
echo ""
echo "Optional: Testing port connectivity (timeout: 2 seconds)..."
echo ""

for host in "${HOSTNAMES[@]}"; do
    if python3 -c "
import socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(2)
try:
    result = sock.connect_ex(('$host', 5432))
    if result == 0:
        print(f'✓ $host:5432 → Reachable')
    else:
        print(f'✗ $host:5432 → Connection refused')
except Exception as e:
    print(f'✗ $host:5432 → Error: {e}')
finally:
    sock.close()
" 2>/dev/null; then
        : # Success
    fi
done

echo ""
echo "========================================="
echo "Verification complete!"
