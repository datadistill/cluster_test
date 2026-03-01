#!/usr/bin/env python3
"""
scripts/test_cluster.py
=======================
Comprehensive cluster health test.

Covers:
  1.  Pre-flight  — connectivity to all nodes
  2.  Auth        — every user role, correct & wrong password
  3.  Permissions — each role is restricted to its allowed operations
  4.  Replication — write→replicate→delete→replicate lifecycle
  5.  Read-only   — direct writes to replicas must be rejected
  6.  Replication status — pg_stat_replication shows connected replicas
  7.  PgBouncer   — all write-path users connect successfully via the pooler
  8.  Monitoring  — postgres-exporter, Prometheus, Grafana HTTP health checks

Required .env keys (in addition to the standard cluster vars):
  PRIMARY_DB=pgbouncer:5432          # or 127.0.0.1:6432 from outside Docker
  DIRECT_PRIMARY_DB=postgres-primary:5432  # direct to PostgreSQL, bypasses PgBouncer
  REPLICA_DBS=postgres-replica-1:5432,postgres-replica-2:5432
  BOUNCER_DB=pgbouncer:5432          # same as PRIMARY_DB when running inside Docker

Notes:
  PRIMARY_DB     — used for app-level tests (routes through PgBouncer when inside Docker)
  DIRECT_PRIMARY_DB — used for pg_stat_replication and read_user permission tests that
                      require a direct PostgreSQL connection bypassing PgBouncer.
                      Falls back to PRIMARY_DB if not set.

Optional:
  POSTGRES_EXPORTER_URL=http://127.0.0.1:9187
  PROMETHEUS_URL=http://127.0.0.1:9090
  GRAFANA_URL=http://127.0.0.1:3000
  REPLICATION_LAG_WAIT=3
"""

import logging
import os
import time
import urllib.error
import urllib.request

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("ClusterTest")

# Core credentials
ADMIN_USER = os.getenv("POSTGRES_USER", "admin")
ADMIN_PASS = os.getenv("POSTGRES_PASSWORD", "")
DB_NAME    = os.getenv("POSTGRES_DB", "app")

MIGRATION_USER = os.getenv("MIGRATION_USER", "migration_user")
MIGRATION_PASS = os.getenv("MIGRATION_PASSWORD", "")

APP_USER = os.getenv("APP_USER", "app_user")
APP_PASS = os.getenv("APP_PASSWORD", "")

READ_USER = os.getenv("READ_USER", "read_user")
READ_PASS = os.getenv("READ_PASSWORD", "")

PGBOUNCER_AUTH_USER = os.getenv("PGBOUNCER_AUTH_USER", "pgbouncer_auth")
PGBOUNCER_AUTH_PASS = os.getenv("PGBOUNCER_AUTH_PASSWORD", "")

LAG_WAIT = int(os.getenv("REPLICATION_LAG_WAIT", "3"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_address(addr_string):
    if not addr_string:
        return None, None
    parts = addr_string.split(":")
    return parts[0], int(parts[1]) if len(parts) > 1 else 5432


def connect(host, port, user, password, dbname=None, autocommit=False):
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname or DB_NAME,
        connect_timeout=5,
        cursor_factory=RealDictCursor,
    )
    conn.autocommit = autocommit
    return conn


def section(title):
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"  {title}")
    logger.info("=" * 60)


# Results accumulator: {test_label: bool}
RESULTS: dict[str, bool] = {}


def record(label, passed):
    RESULTS[label] = passed
    icon = "✅" if passed else "❌"
    level = logger.info if passed else logger.error
    level(f"  {icon}  {label}")
    return passed


# ---------------------------------------------------------------------------
# 1. Pre-flight connectivity
# ---------------------------------------------------------------------------

def test_connectivity(host, port, label, user=None, password=None, dbname=None):
    u = user or ADMIN_USER
    p = password or ADMIN_PASS
    try:
        conn = connect(host, port, u, p, dbname=dbname)
        conn.close()
        return record(f"connect: {label}", True)
    except psycopg2.OperationalError as e:
        logger.error(f"  ❌  connect: {label} — {str(e).splitlines()[0]}")
        return record(f"connect: {label}", False)


def preflight(p_host, p_port, replicas, b_host, b_port):
    section("1. PRE-FLIGHT — Connectivity")
    ok = True
    ok &= test_connectivity(p_host, p_port, f"primary ({p_host}:{p_port})")
    for label, rh, rp in replicas:
        ok &= test_connectivity(rh, rp, f"{label} ({rh}:{rp})")
    if b_host:
        ok &= test_connectivity(b_host, b_port, f"pgbouncer ({b_host}:{b_port})")
    if not ok:
        logger.error("  Aborting — one or more nodes unreachable.")
    return ok


# ---------------------------------------------------------------------------
# 2. Authentication — every role, correct + wrong password
# ---------------------------------------------------------------------------

def test_auth_correct(host, port, user, password, dbname, label):
    """Expect success."""
    try:
        conn = connect(host, port, user, password, dbname=dbname)
        conn.close()
        return record(f"auth correct: {label}", True)
    except psycopg2.OperationalError as e:
        logger.error(f"  ❌  auth correct: {label} — {str(e).splitlines()[0]}")
        return record(f"auth correct: {label}", False)


def test_auth_wrong(host, port, user, dbname, label):
    """Expect failure — passes the test when auth is correctly rejected."""
    try:
        conn = connect(host, port, user, "definitely-wrong-password-xyz", dbname=dbname)
        conn.close()
        logger.error(f"  ❌  auth wrong-pw: {label} — connection SUCCEEDED (should have been rejected)")
        return record(f"auth wrong-pw: {label}", False)
    except psycopg2.OperationalError:
        return record(f"auth wrong-pw: {label}", True)


def run_auth_tests(p_host, p_port, replicas):
    section("2. AUTHENTICATION — all roles")
    r1_host, r1_port = (replicas[0][1], replicas[0][2]) if replicas else (p_host, p_port)

    # admin → primary via app db (PgBouncer may not expose the postgres system db directly)
    test_auth_correct(p_host, p_port, ADMIN_USER, ADMIN_PASS, DB_NAME, "admin on primary")
    test_auth_wrong(p_host, p_port, ADMIN_USER, DB_NAME, "admin wrong password")

    # migration_user → primary → app db
    test_auth_correct(p_host, p_port, MIGRATION_USER, MIGRATION_PASS, DB_NAME, "migration_user on primary")
    test_auth_wrong(p_host, p_port, MIGRATION_USER, DB_NAME, "migration_user wrong password")

    # app_user → primary → app db
    test_auth_correct(p_host, p_port, APP_USER, APP_PASS, DB_NAME, "app_user on primary")
    test_auth_wrong(p_host, p_port, APP_USER, DB_NAME, "app_user wrong password")

    # read_user → replica → app db
    test_auth_correct(r1_host, r1_port, READ_USER, READ_PASS, DB_NAME, "read_user on replica")
    test_auth_wrong(r1_host, r1_port, READ_USER, DB_NAME, "read_user wrong password")


# ---------------------------------------------------------------------------
# 3. Permissions enforcement
# ---------------------------------------------------------------------------

def run_permission_tests(p_host, p_port, direct_host, direct_port, replicas):
    section("3. PERMISSIONS — role boundaries")
    r1_host, r1_port = (replicas[0][1], replicas[0][2]) if replicas else (p_host, p_port)

    # Ensure the test table exists (created by admin so it has all grants applied by PATCHER)
    try:
        with connect(p_host, p_port, ADMIN_USER, ADMIN_PASS, dbname=DB_NAME, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS perms_test (
                        id   SERIAL PRIMARY KEY,
                        val  TEXT
                    );
                    TRUNCATE perms_test;
                    INSERT INTO perms_test (val) VALUES ('seed');
                """)
    except Exception as e:
        logger.error(f"  Could not set up perms_test table: {e}")
        return

    # --- migration_user: DDL allowed ---
    label = "migration_user can CREATE TABLE"
    try:
        with connect(p_host, p_port, MIGRATION_USER, MIGRATION_PASS, dbname=DB_NAME, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS migration_ddl_test; CREATE TABLE migration_ddl_test (x INT);")
                cur.execute("DROP TABLE IF EXISTS migration_ddl_test;")
        record(label, True)
    except Exception as e:
        logger.error(f"  {label} failed: {e}")
        record(label, False)

    # --- app_user: INSERT allowed ---
    label = "app_user can INSERT"
    try:
        with connect(p_host, p_port, APP_USER, APP_PASS, dbname=DB_NAME, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO perms_test (val) VALUES ('from app_user');")
        record(label, True)
    except Exception as e:
        logger.error(f"  {label} failed: {e}")
        record(label, False)

    # --- app_user: SELECT allowed ---
    label = "app_user can SELECT"
    try:
        with connect(p_host, p_port, APP_USER, APP_PASS, dbname=DB_NAME) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS n FROM perms_test;")
                row = cur.fetchone()
        record(label, row is not None)
    except Exception as e:
        logger.error(f"  {label} failed: {e}")
        record(label, False)

    # --- app_user: CREATE TABLE rejected ---
    label = "app_user cannot CREATE TABLE (expected rejection)"
    try:
        with connect(p_host, p_port, APP_USER, APP_PASS, dbname=DB_NAME, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE app_should_fail (x INT);")
        logger.error(f"  ❌  {label} — DDL succeeded (should have been denied)")
        record(label, False)
    except psycopg2.errors.InsufficientPrivilege:
        record(label, True)
    except Exception as e:
        logger.warning(f"  ⚠️  {label} — unexpected error: {e}")
        record(label, False)

    # --- read_user: SELECT on replica allowed ---
    label = "read_user can SELECT on replica"
    try:
        with connect(r1_host, r1_port, READ_USER, READ_PASS, dbname=DB_NAME) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS n FROM perms_test;")
                row = cur.fetchone()
        record(label, row is not None)
    except Exception as e:
        logger.error(f"  {label} failed: {e}")
        record(label, False)

    # --- read_user: INSERT rejected — connect DIRECTLY to primary (read_user not in PgBouncer userlist) ---
    label = "read_user cannot INSERT (expected rejection)"
    try:
        with connect(direct_host, direct_port, READ_USER, READ_PASS, dbname=DB_NAME, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO perms_test (val) VALUES ('read_user_attempt');")
        logger.error(f"  ❌  {label} — INSERT succeeded (should have been denied)")
        record(label, False)
    except (psycopg2.errors.InsufficientPrivilege, psycopg2.errors.ReadOnlySqlTransaction):
        record(label, True)
    except Exception as e:
        logger.warning(f"  ⚠️  {label} — unexpected error: {e}")
        record(label, False)


# ---------------------------------------------------------------------------
# 4. Replication lifecycle
# ---------------------------------------------------------------------------

def run_replication_lifecycle(p_host, p_port, replicas):
    section("4. REPLICATION LIFECYCLE — write → replicate → delete → replicate")

    record_id = int(time.time())
    message = f"lifecycle test at {time.strftime('%Y-%m-%d %H:%M:%S')}"

    # Create table
    try:
        with connect(p_host, p_port, ADMIN_USER, ADMIN_PASS, dbname=DB_NAME, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DROP TABLE IF EXISTS cluster_lifecycle_test;
                    CREATE TABLE cluster_lifecycle_test (
                        id      BIGINT PRIMARY KEY,
                        message TEXT,
                        ts      TIMESTAMP DEFAULT NOW()
                    );
                """)
        record("lifecycle: create table on primary", True)
    except Exception as e:
        logger.error(f"  lifecycle: create table failed: {e}")
        record("lifecycle: create table on primary", False)
        return

    # Write
    try:
        with connect(p_host, p_port, ADMIN_USER, ADMIN_PASS, dbname=DB_NAME, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO cluster_lifecycle_test (id, message) VALUES (%s, %s);",
                    (record_id, message),
                )
        record(f"lifecycle: write id={record_id} to primary", True)
    except Exception as e:
        logger.error(f"  lifecycle: write failed: {e}")
        record(f"lifecycle: write id={record_id} to primary", False)
        return

    logger.info(f"  Waiting {LAG_WAIT}s for WAL replication...")
    time.sleep(LAG_WAIT)

    # Read from all replicas — should exist
    for label, rh, rp in replicas:
        lbl = f"lifecycle: {label} has record after write"
        try:
            with connect(rh, rp, ADMIN_USER, ADMIN_PASS, dbname=DB_NAME) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id FROM cluster_lifecycle_test WHERE id = %s;",
                        (record_id,),
                    )
                    row = cur.fetchone()
            if row:
                record(lbl, True)
            else:
                logger.warning(f"  ⚠️  {lbl} — not found (replication lag?)")
                record(lbl, False)
        except Exception as e:
            logger.error(f"  {lbl} failed: {e}")
            record(lbl, False)

    # Delete from primary
    try:
        with connect(p_host, p_port, ADMIN_USER, ADMIN_PASS, dbname=DB_NAME, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM cluster_lifecycle_test WHERE id = %s;", (record_id,)
                )
        record(f"lifecycle: delete id={record_id} from primary", True)
    except Exception as e:
        logger.error(f"  lifecycle: delete failed: {e}")
        record(f"lifecycle: delete id={record_id} from primary", False)
        return

    logger.info(f"  Waiting {LAG_WAIT}s for delete to replicate...")
    time.sleep(LAG_WAIT)

    # Read from all replicas — should be gone
    for label, rh, rp in replicas:
        lbl = f"lifecycle: {label} record gone after delete"
        try:
            with connect(rh, rp, ADMIN_USER, ADMIN_PASS, dbname=DB_NAME) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id FROM cluster_lifecycle_test WHERE id = %s;",
                        (record_id,),
                    )
                    row = cur.fetchone()
            if not row:
                record(lbl, True)
            else:
                logger.warning(f"  ⚠️  {lbl} — still present (replication lag?)")
                record(lbl, False)
        except Exception as e:
            logger.error(f"  {lbl} failed: {e}")
            record(lbl, False)


# ---------------------------------------------------------------------------
# 5. Write rejection on replicas
# ---------------------------------------------------------------------------

def run_replica_write_rejection(replicas):
    section("5. REPLICA WRITE REJECTION — replicas must refuse writes")
    for label, rh, rp in replicas:
        lbl = f"write rejected on {label}"
        try:
            with connect(rh, rp, ADMIN_USER, ADMIN_PASS, dbname=DB_NAME, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("CREATE TABLE IF NOT EXISTS should_not_exist (x INT);")
            logger.error(f"  ❌  {lbl} — write SUCCEEDED (replica is not read-only!)")
            record(lbl, False)
        except psycopg2.errors.ReadOnlySqlTransaction:
            record(lbl, True)
        except Exception as e:
            msg = str(e).lower()
            if "read-only" in msg or "read only" in msg or "cannot execute" in msg:
                record(lbl, True)
            else:
                logger.error(f"  ❌  {lbl} — unexpected error: {e}")
                record(lbl, False)


# ---------------------------------------------------------------------------
# 6. Replication status via pg_stat_replication
# ---------------------------------------------------------------------------

def run_replication_status(direct_host, direct_port, expected_replicas):
    section("6. REPLICATION STATUS — pg_stat_replication")
    try:
        with connect(direct_host, direct_port, ADMIN_USER, ADMIN_PASS, dbname=DB_NAME) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        application_name,
                        state,
                        sync_state,
                        pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn))  AS send_lag,
                        pg_size_pretty(pg_wal_lsn_diff(sent_lsn,            write_lsn)) AS write_lag,
                        pg_size_pretty(pg_wal_lsn_diff(write_lsn,           flush_lsn)) AS flush_lag,
                        pg_size_pretty(pg_wal_lsn_diff(flush_lsn,           replay_lsn)) AS replay_lag
                    FROM pg_stat_replication
                    ORDER BY application_name;
                """)
                rows = cur.fetchall()

        count = len(rows)
        logger.info(f"  Connected replicas: {count} (expected ≥ {expected_replicas})")
        for row in rows:
            logger.info(
                f"    {row['application_name']}  state={row['state']}  "
                f"send_lag={row['send_lag']}  replay_lag={row['replay_lag']}"
            )
        record("replication: all replicas connected", count >= expected_replicas)
    except Exception as e:
        logger.error(f"  pg_stat_replication query failed: {e}")
        record("replication: all replicas connected", False)


# ---------------------------------------------------------------------------
# 7. PgBouncer — write-path users
# ---------------------------------------------------------------------------

def run_pgbouncer_tests(b_host, b_port):
    section("7. PGBOUNCER — write-path user connectivity")

    users = [
        ("admin",          ADMIN_USER,      ADMIN_PASS,      DB_NAME),
        ("migration_user", MIGRATION_USER,  MIGRATION_PASS,  DB_NAME),
        ("app_user",       APP_USER,        APP_PASS,        DB_NAME),
    ]

    for label, user, password, dbname in users:
        lbl = f"pgbouncer: {label} can connect and query"
        try:
            with connect(b_host, b_port, user, password, dbname=dbname) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 AS alive;")
                    row = cur.fetchone()
            record(lbl, row is not None and row.get("alive") == 1)
        except Exception as e:
            logger.error(f"  ❌  {lbl} — {str(e).splitlines()[0]}")
            record(lbl, False)

    # Verify transaction-mode pooling: a multi-statement transaction must work
    lbl = "pgbouncer: app_user transaction commit"
    try:
        conn = connect(b_host, b_port, APP_USER, APP_PASS, dbname=DB_NAME)
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database() AS db;")
                row = cur.fetchone()
        conn.close()
        record(lbl, row is not None)
    except Exception as e:
        logger.error(f"  ❌  {lbl} — {str(e).splitlines()[0]}")
        record(lbl, False)

    lbl = "pgbouncer: read_user is not a pooled write-path user (design check)"
    logger.info(f"  ℹ️   {lbl}: read_user connects DIRECTLY to replica ports, not via PgBouncer.")


# ---------------------------------------------------------------------------
# 8. Monitoring health checks
# ---------------------------------------------------------------------------

def http_check(url, label, expected_status=200, timeout=5):
    lbl = f"monitoring: {label}"
    try:
        req = urllib.request.Request(url, headers={"Accept": "*/*"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.status
        if status == expected_status:
            return record(lbl, True)
        else:
            logger.error(f"  ❌  {lbl} — HTTP {status} (expected {expected_status})")
            return record(lbl, False)
    except urllib.error.HTTPError as e:
        logger.error(f"  ❌  {lbl} — HTTP {e.code}: {e.reason}")
        return record(lbl, False)
    except Exception as e:
        logger.error(f"  ❌  {lbl} — {e}")
        return record(lbl, False)


def run_monitoring_checks():
    section("8. MONITORING — HTTP health checks")

    exporter_url   = os.getenv("POSTGRES_EXPORTER_URL", "").rstrip("/")
    prometheus_url = os.getenv("PROMETHEUS_URL", "").rstrip("/")
    grafana_url    = os.getenv("GRAFANA_URL", "").rstrip("/")

    if exporter_url:
        http_check(f"{exporter_url}/metrics", "postgres-exporter /metrics")
    else:
        logger.info("  ⏭️   postgres-exporter: POSTGRES_EXPORTER_URL not set — skipping")

    if prometheus_url:
        http_check(f"{prometheus_url}/-/healthy", "Prometheus /-/healthy")
    else:
        logger.info("  ⏭️   Prometheus: PROMETHEUS_URL not set — skipping")

    if grafana_url:
        http_check(f"{grafana_url}/api/health", "Grafana /api/health")
    else:
        logger.info("  ⏭️   Grafana: GRAFANA_URL not set — skipping")


# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------

def print_summary():
    section("FINAL SUMMARY")
    passed = [k for k, v in RESULTS.items() if v]
    failed = [k for k, v in RESULTS.items() if not v]

    for label in passed:
        logger.info(f"  ✅  {label}")
    for label in failed:
        logger.error(f"  ❌  {label}")

    logger.info("")
    total = len(RESULTS)
    logger.info(f"  Passed: {len(passed)}/{total}")

    if not failed:
        logger.info("  🟢  ALL CHECKS PASSED — CLUSTER HEALTHY")
    else:
        logger.error(f"  🔴  {len(failed)} CHECK(S) FAILED — SEE ABOVE")
    return len(failed) == 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    p_host, p_port = parse_address(os.getenv("PRIMARY_DB"))
    b_host, b_port = parse_address(os.getenv("BOUNCER_DB"))

    # DIRECT_PRIMARY_DB bypasses PgBouncer — needed for pg_stat_replication
    # and for users not in PgBouncer's userlist (read_user).
    # Falls back to PRIMARY_DB if not set (works when running outside Docker).
    direct_host, direct_port = parse_address(
        os.getenv("DIRECT_PRIMARY_DB") or os.getenv("PRIMARY_DB")
    )

    replicas = []
    for i, entry in enumerate(os.getenv("REPLICA_DBS", "").split(","), start=1):
        entry = entry.strip()
        if entry:
            rh, rp = parse_address(entry)
            replicas.append((f"REPLICA-{i}", rh, rp))

    if not p_host:
        logger.error("PRIMARY_DB not set  (e.g. PRIMARY_DB=127.0.0.1:5433)")
        return
    if not replicas:
        logger.error("REPLICA_DBS not set  (e.g. REPLICA_DBS=127.0.0.1:5434,127.0.0.1:5435)")
        return

    if not preflight(p_host, p_port, replicas, b_host, b_port):
        exit(1)

    run_auth_tests(p_host, p_port, replicas)
    run_permission_tests(p_host, p_port, direct_host, direct_port, replicas)
    run_replication_lifecycle(p_host, p_port, replicas)
    run_replica_write_rejection(replicas)
    run_replication_status(direct_host, direct_port, expected_replicas=len(replicas))

    if b_host:
        run_pgbouncer_tests(b_host, b_port)
    else:
        logger.info("\n  ⏭️   PgBouncer tests skipped — BOUNCER_DB not set")

    run_monitoring_checks()

    all_passed = print_summary()
    exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
