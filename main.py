import os
import time
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# --- 1. Setup ---
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger("ClusterTest")

# Credentials
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")

def parse_address(addr_string):
    """Splits 'host:port' into ('host', port)"""
    if not addr_string:
        return None, None
    parts = addr_string.split(":")
    host = parts[0]
    port = int(parts[1]) if len(parts) > 1 else 5432
    return host, port

def get_connection(host, port):
    """Creates a psycopg2 connection"""
    return psycopg2.connect(
        host=host,
        port=port,
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_NAME,
        connect_timeout=5,
        cursor_factory=RealDictCursor
    )

# --- 2. Test Logic ---

def test_primary(host, port):
    logger.info(f"🔎 Testing PRIMARY -> {host}:{port}")
    test_id = int(time.time())
    try:
        with get_connection(host, port) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS cluster_heartbeat (id BIGINT PRIMARY KEY, ts TIMESTAMP DEFAULT NOW());")
                cur.execute("INSERT INTO cluster_heartbeat (id) VALUES (%s);", (test_id,))
                logger.info(f"✅ PRIMARY: Write successful (ID: {test_id})")
                return test_id
    except Exception as e:
        logger.error(f"❌ PRIMARY FAILED ({host}:{port}): {e}")
        return None

def test_replica(host, port, expected_id):
    logger.info(f"🔎 Testing REPLICA -> {host}:{port}")
    try:
        with get_connection(host, port) as conn:
            with conn.cursor() as cur:
                # 1. Check Read-Only state
                cur.execute("SELECT pg_is_in_recovery() as recovery;")
                status = cur.fetchone()
                
                # 2. Check for data
                cur.execute("SELECT id FROM cluster_heartbeat WHERE id = %s;", (expected_id,))
                data = cur.fetchone()

                if status['recovery'] and data:
                    logger.info(f"✅ REPLICA: Sync verified and Read-Only mode confirmed.")
                    return True
                elif not status['recovery']:
                    logger.error(f"❌ REPLICA: Host is NOT in recovery mode (it is a Primary!)")
                else:
                    logger.warning(f"⚠️ REPLICA: Connected, but ID {expected_id} not found yet (lag?)")
    except Exception as e:
        logger.error(f"❌ REPLICA FAILED ({host}:{port}): {e}")
    return False

def test_bouncer(host, port):
    logger.info(f"🔎 Testing PGBOUNCER -> {host}:{port}")
    try:
        with get_connection(host, port) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 as alive;")
                if cur.fetchone()['alive'] == 1:
                    logger.info(f"✅ PGBOUNCER: Connection successful.")
                    return True
    except Exception as e:
        logger.error(f"❌ PGBOUNCER FAILED ({host}:{port}): {e}")
    return False

# --- 3. Main Runner ---

def main():
    # Parse addresses from .env
    p_host, p_port = parse_address(os.getenv("PRIMARY_DB"))
    b_host, b_port = parse_address(os.getenv("BOUNCER_DB"))
    
    # Parse multiple replicas
    replica_list = os.getenv("REPLICA_DBS", "").split(",")
    
    if not p_host:
        logger.error("No PRIMARY_DB defined in .env")
        return

    # Phase 1: Primary
    write_id = test_primary(p_host, p_port)
    if not write_id:
        exit(1)

    # Phase 2: Wait for WAL
    logger.info("Waiting 2 seconds for replication...")
    time.sleep(2)

    # Phase 3: All Replicas
    replica_success = True
    for entry in replica_list:
        if not entry.strip(): continue
        r_host, r_port = parse_address(entry.strip())
        if not test_replica(r_host, r_port, write_id):
            replica_success = False

    # Phase 4: PgBouncer
    bouncer_success = True
    if b_host:
        bouncer_success = test_bouncer(b_host, b_port)

    # Conclusion
    print("\n" + "="*50)
    if replica_success and bouncer_success:
        logger.info("🟢 CLUSTER HEALTHY")
    else:
        logger.error("🔴 CLUSTER ISSUES DETECTED")
        exit(1)

if __name__ == "__main__":
    main()