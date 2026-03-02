# PostgreSQL Cluster Test Application

This application tests connectivity and functionality of a PostgreSQL cluster with PgBouncer and replication.

## Problem Statement

In production environments, the PostgreSQL cluster hostnames (`postgres-primary`, `pgbouncer`, `postgres-replica-*`) don't resolve inside the application container because Docker DNS only works between containers that share a network. The PostgreSQL cluster lives on `pg_network`, but the application isn't connected to it.

### Why Dev Works But Prod Doesn't
In development, Coolify happens to attach both the cluster and the application to its global `coolify` bridge network by coincidence. In production, each Coolify resource gets an isolated UUID network with no overlap, causing DNS resolution to fail.

## Solution

### 1. Docker Compose Configuration
The `docker-compose.yaml` file ensures the application container connects to the `pg_network`:

```yaml
services:
  app:
    build: .
    restart: unless-stopped
    networks:
      - default
      - pg_network

networks:
  pg_network:
    external: true
    name: pg_network
```

### 2. Network Setup
Ensure `pg_network` exists on the host before the application starts:

```bash
docker network create pg_network 2>/dev/null || true
```

Run this once on the server, or add it to your deploy pipeline. The PostgreSQL cluster's deploy script also creates it automatically, so if the cluster is already running, this is already done.

### 3. Environment Variables
In your application's environment variables (Coolify UI), set:

```
PRIMARY_DB=postgres-primary:5432
BOUNCER_DB=pgbouncer:5432
REPLICA_DBS=postgres-replica-1:5432,postgres-replica-2:5432
DIRECT_PRIMARY_DB=postgres-primary:5432
```

These hostnames resolve correctly once the application is on `pg_network`.

## Deployment

### Quick Deployment
Run the deployment script:

```bash
chmod +x deploy.sh
./deploy.sh
```

### Manual Deployment
1. Build the Docker image:
   ```bash
   docker build -t cluster-test-app:latest .
   ```

2. Ensure the network exists:
   ```bash
   docker network create pg_network 2>/dev/null || true
   ```

3. Deploy with Docker Compose:
   ```bash
   docker-compose up -d
   ```

## Verification

After deploying, verify DNS resolution:

```bash
docker exec <your-app-container> python3 -c "
import socket
for host in ['postgres-primary', 'pgbouncer', 'postgres-replica-1']:
    try:
        print(f'{host} → {socket.gethostbyname(host)}')
    except socket.gaierror as e:
        print(f'{host} → FAILED: {e}')
"
```

All three should return an IP address. If they do, the network is correctly set up.

For comprehensive testing:

```bash
docker exec <your-app-container> python3 test_dns.py
```

## Testing the Application

Run the main test suite:

```bash
docker exec <your-app-container> python3 main.py
```

The test suite covers:
1. Pre-flight connectivity to all nodes
2. Authentication tests for every user role
3. Permission verification for each role
4. Replication lifecycle testing
5. Read-only replica validation
6. Replication status monitoring
7. PgBouncer connectivity tests
8. Monitoring service health checks

## Files

- `Dockerfile` - Application container definition
- `docker-compose.yaml` - Multi-container deployment with network configuration
- `main.py` - Main test application
- `requirements.txt` - Python dependencies
- `test_dns.py` - DNS resolution test utility
- `deploy.sh` - Automated deployment script

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `PRIMARY_DB` | Primary database through PgBouncer | `postgres-primary:5432` |
| `BOUNCER_DB` | PgBouncer connection | `pgbouncer:5432` |
| `REPLICA_DBS` | Comma-separated replica connections | `postgres-replica-1:5432,postgres-replica-2:5432` |
| `DIRECT_PRIMARY_DB` | Direct PostgreSQL connection (bypasses PgBouncer) | `postgres-primary:5432` |
| `POSTGRES_EXPORTER_URL` | Monitoring exporter URL | `http://127.0.0.1:9187` |
| `PROMETHEUS_URL` | Prometheus URL | `http://127.0.0.1:9090` |
| `GRAFANA_URL` | Grafana URL | `http://127.0.0.1:3000` |
| `REPLICATION_LAG_WAIT` | Wait time for replication lag (seconds) | `3` |

## Why This Works Everywhere

| Deployment Method | Result |
|-------------------|--------|
| Coolify (Dockerfile only) | Coolify detects `docker-compose.yaml`, uses it, attaches to `pg_network` on every deploy |
| Coolify (compose) | Same — compose controls the network |
| `docker compose up` locally | Works identically |
| `docker run` manually | Add `--network pg_network` to the run command |

The `docker-compose.yaml` is the portable artifact that enforces network membership regardless of how or where the application is deployed.

## Troubleshooting

### DNS Resolution Fails
1. Check if the container is connected to `pg_network`:
   ```bash
   docker network inspect pg_network
   ```

2. Verify the PostgreSQL cluster containers are running:
   ```bash
   docker ps | grep postgres
   ```

3. Check container network connections:
   ```bash
   docker inspect <container-id> | grep -A 5 Networks
   ```

### Application Tests Fail
1. Check environment variables are set correctly:
   ```bash
   docker exec <container-id> env | grep _DB
   ```

2. View application logs:
   ```bash
   docker-compose logs app
   ```

3. Run DNS test manually:
   ```bash
   docker exec <container-id> python3 test_dns.py
   ```

## Cleanup

To stop and remove containers:

```bash
docker-compose down
```

To remove the network (ensure no containers are using it first):

```bash
docker network rm pg_network
```
