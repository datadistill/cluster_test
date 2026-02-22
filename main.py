import logging
import os
import time

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

# --- 1. Setup ---
load_dotenv()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("ClusterTest")

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
    return psycopg2.connect(
        host=host,
        port=port,
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_NAME,
        connect_timeout=5,
        cursor_factory=RealDictCursor,
    )


def section(title):
    logger.info("")
    logger.info("=" * 55)
    logger.info(f"  {title}")
    logger.info("=" * 55)


# --- 2. Connectivity Pre-Check ---


def test_connection(host, port, label):
    """Tries to open and immediately close a connection. Returns True/False."""
    try:
        conn = get_connection(host, port)
        conn.close()
        logger.info(f"  ✅ {label} ({host}:{port}) — reachable")
        return True
    except psycopg2.OperationalError as e:
        first_line = str(e).splitlines()[0]
        logger.error(f"  ❌ {label} ({host}:{port}) — UNREACHABLE: {first_line}")
        return False


def preflight_check(p_host, p_port, replicas, b_host, b_port):
    section("PRE-FLIGHT: Testing connectivity to all nodes")
    results = {}

    results["primary"] = test_connection(p_host, p_port, "PRIMARY")

    for label, r_host, r_port in replicas:
        results[label] = test_connection(r_host, r_port, label)

    if b_host:
        results["pgbouncer"] = test_connection(b_host, b_port, "PGBOUNCER")

    failed = [k for k, v in results.items() if not v]
    if failed:
        logger.error("")
        logger.error(
            f"  Cannot proceed — {len(failed)} node(s) unreachable: {', '.join(failed)}"
        )
        logger.error("  Check that:")
        logger.error("    1. Your cluster is running:  docker compose ps")
        logger.error(
            "    2. PRIMARY_DB / REPLICA_DBS use 127.0.0.1, not Docker hostnames"
        )
        logger.error("    3. Ports match what is exposed in docker-compose.yaml")
        return False

    logger.info("")
    logger.info("  All nodes reachable — proceeding with tests.")
    return True


# --- 3. Primary Operations ---


def create_table(host, port):
    section("STEP 1: Create table on PRIMARY")
    try:
        with get_connection(host, port) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("""
                    DROP TABLE IF EXISTS cluster_lifecycle_test;
                    CREATE TABLE cluster_lifecycle_test (
                        id      BIGINT PRIMARY KEY,
                        message TEXT,
                        ts      TIMESTAMP DEFAULT NOW()
                    );
                """)
                logger.info(
                    f"✅ PRIMARY ({host}:{port}): Table 'cluster_lifecycle_test' created."
                )
                return True
    except Exception as e:
        logger.error(f"❌ PRIMARY: Failed to create table: {e}")
        return False


def write_to_primary(host, port, record_id, message):
    section(f"STEP 2: Write record (id={record_id}) to PRIMARY")
    try:
        with get_connection(host, port) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO cluster_lifecycle_test (id, message) VALUES (%s, %s);",
                    (record_id, message),
                )
                logger.info(
                    f"✅ PRIMARY ({host}:{port}): Inserted id={record_id} message='{message}'"
                )
                return True
    except Exception as e:
        logger.error(f"❌ PRIMARY: Write failed: {e}")
        return False


def delete_from_primary(host, port, record_id):
    section(f"STEP 4: Delete record (id={record_id}) from PRIMARY")
    try:
        with get_connection(host, port) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM cluster_lifecycle_test WHERE id = %s;", (record_id,)
                )
                logger.info(f"✅ PRIMARY ({host}:{port}): Deleted id={record_id}")
                return True
    except Exception as e:
        logger.error(f"❌ PRIMARY: Delete failed: {e}")
        return False


# --- 3. Replica Checks ---


def check_replica_has_record(host, port, record_id, replica_label="REPLICA"):
    """Returns True if the record EXISTS on the replica."""
    try:
        with get_connection(host, port) as conn:
            with conn.cursor() as cur:
                # Confirm it is actually a replica
                cur.execute("SELECT pg_is_in_recovery() as recovery;")
                status = cur.fetchone()
                if not status["recovery"]:
                    logger.error(
                        f"❌ {replica_label} ({host}:{port}): NOT in recovery mode — this is a primary!"
                    )
                    return None

                cur.execute(
                    "SELECT id, message, ts FROM cluster_lifecycle_test WHERE id = %s;",
                    (record_id,),
                )
                row = cur.fetchone()
                return row
    except Exception as e:
        logger.error(f"❌ {replica_label} ({host}:{port}): Query failed: {e}")
        return None


def verify_replicas_have_record(replicas, record_id, step_label):
    section(f"STEP {step_label}: Read record (id={record_id}) from ALL REPLICAS")
    all_ok = True
    for label, host, port in replicas:
        row = check_replica_has_record(host, port, record_id, label)
        if row:
            logger.info(
                f"✅ {label} ({host}:{port}): Record found — id={row['id']} message='{row['message']}' ts={row['ts']}"
            )
        elif row is None:
            logger.error(
                f"❌ {label} ({host}:{port}): Could not verify (error or not a replica)"
            )
            all_ok = False
        else:
            logger.warning(
                f"⚠️  {label} ({host}:{port}): Record NOT found (possible replication lag)"
            )
            all_ok = False
    return all_ok


def verify_replicas_missing_record(replicas, record_id):
    section(f"STEP 5: Confirm record (id={record_id}) is GONE from ALL REPLICAS")
    all_ok = True
    for label, host, port in replicas:
        row = check_replica_has_record(host, port, record_id, label)
        if row is None and isinstance(row, type(None)):
            # distinguish error (None returned on exception) from not found (falsy dict)
            # re-check by catching explicitly
            try:
                with get_connection(host, port) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT id FROM cluster_lifecycle_test WHERE id = %s;",
                            (record_id,),
                        )
                        found = cur.fetchone()
                        if not found:
                            logger.info(
                                f"✅ {label} ({host}:{port}): Record correctly absent after delete."
                            )
                        else:
                            logger.warning(
                                f"⚠️  {label} ({host}:{port}): Record still present (replication lag?)"
                            )
                            all_ok = False
            except Exception as e:
                logger.error(f"❌ {label} ({host}:{port}): {e}")
                all_ok = False
        elif not row:
            logger.info(
                f"✅ {label} ({host}:{port}): Record correctly absent after delete."
            )
        else:
            logger.warning(
                f"⚠️  {label} ({host}:{port}): Record still present (replication lag?)"
            )
            all_ok = False
    return all_ok


def test_bouncer(host, port):
    section("BONUS: PgBouncer connectivity check")
    try:
        with get_connection(host, port) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 as alive;")
                if cur.fetchone()["alive"] == 1:
                    logger.info(f"✅ PGBOUNCER ({host}:{port}): Connection successful.")
                    return True
    except Exception as e:
        logger.error(f"❌ PGBOUNCER ({host}:{port}): {e}")
    return False


# --- 4. Main Runner ---


def main():
    p_host, p_port = parse_address(os.getenv("PRIMARY_DB"))
    b_host, b_port = parse_address(os.getenv("BOUNCER_DB"))

    # Support multiple replicas: REPLICA_DBS=127.0.0.1:5434,127.0.0.1:5435
    replicas = []
    for i, entry in enumerate(os.getenv("REPLICA_DBS", "").split(","), start=1):
        if not entry.strip():
            continue
        r_host, r_port = parse_address(entry.strip())
        replicas.append((f"REPLICA-{i}", r_host, r_port))

    if not p_host:
        logger.error("No PRIMARY_DB defined in .env  (e.g. PRIMARY_DB=127.0.0.1:5433)")
        return

    if not replicas:
        logger.error(
            "No REPLICA_DBS defined in .env  (e.g. REPLICA_DBS=127.0.0.1:5434)"
        )
        return

    # Pre-flight — abort immediately if any node is unreachable
    if not preflight_check(p_host, p_port, replicas, b_host, b_port):
        exit(1)

    record_id = int(time.time())
    message = f"hello from lifecycle test at {time.strftime('%Y-%m-%d %H:%M:%S')}"
    lag_wait = int(os.getenv("REPLICATION_LAG_WAIT", "3"))  # seconds to wait for WAL

    results = {}

    # Step 1 — Create table
    results["create"] = create_table(p_host, p_port)
    if not results["create"]:
        logger.error("Aborting — could not create table on primary.")
        return

    # Step 2 — Write to primary
    results["write"] = write_to_primary(p_host, p_port, record_id, message)
    if not results["write"]:
        logger.error("Aborting — could not write to primary.")
        return

    # Wait for WAL replication
    logger.info(f"Waiting {lag_wait}s for WAL to replicate to replicas...")
    time.sleep(lag_wait)

    # Step 3 — Read from replicas (should exist)
    results["read_after_write"] = verify_replicas_have_record(
        replicas, record_id, step_label="3"
    )

    # Step 4 — Delete from primary
    results["delete"] = delete_from_primary(p_host, p_port, record_id)

    # Wait for delete to replicate
    logger.info(f"Waiting {lag_wait}s for delete to replicate to replicas...")
    time.sleep(lag_wait)

    # Step 5 — Read from replicas (should be gone)
    results["read_after_delete"] = verify_replicas_missing_record(replicas, record_id)

    # PgBouncer check
    if b_host:
        results["bouncer"] = test_bouncer(b_host, b_port)

    # --- Final Summary ---
    section("FINAL SUMMARY")
    status_map = {
        "create": "Table creation on primary",
        "write": "Write to primary",
        "read_after_write": "Read from replicas after write",
        "delete": "Delete from primary",
        "read_after_delete": "Replicas reflect deletion",
        "bouncer": "PgBouncer connectivity",
    }
    all_passed = True
    for key, label in status_map.items():
        if key not in results:
            continue
        icon = "✅" if results[key] else "❌"
        if not results[key]:
            all_passed = False
        logger.info(f"  {icon}  {label}")

    logger.info("")
    if all_passed:
        logger.info("🟢  ALL CHECKS PASSED — CLUSTER HEALTHY")
    else:
        logger.error("🔴  SOME CHECKS FAILED — SEE ABOVE")
        exit(1)


if __name__ == "__main__":
    main()
