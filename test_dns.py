#!/usr/bin/env python3
"""
DNS Test Script

This script tests DNS resolution for PostgreSQL cluster hostnames.
Run this inside the container after deployment to verify network connectivity.
"""

import socket
import sys


def test_dns_resolution():
    """Test DNS resolution for PostgreSQL cluster hostnames."""
    hostnames = [
        "postgres-primary",
        "pgbouncer",
        "postgres-replica-1",
        "postgres-replica-2",
    ]

    print("Testing DNS resolution for PostgreSQL cluster hostnames...")
    print("=" * 60)

    all_resolved = True

    for host in hostnames:
        try:
            ip_address = socket.gethostbyname(host)
            print(f"✓ {host:25} → {ip_address}")
        except socket.gaierror as e:
            print(f"✗ {host:25} → FAILED: {e}")
            all_resolved = False

    print("=" * 60)

    if all_resolved:
        print("SUCCESS: All hostnames resolved correctly!")
        return 0
    else:
        print("FAILURE: Some hostnames failed to resolve.")
        print("\nTroubleshooting steps:")
        print("1. Ensure the app container is connected to the 'pg_network'")
        print("2. Verify the PostgreSQL cluster containers are running")
        print("3. Check if 'pg_network' exists: docker network ls | grep pg_network")
        print("4. Verify network connection: docker network inspect pg_network")
        return 1


def test_port_connectivity():
    """Test connectivity to PostgreSQL ports."""
    hostnames = [
        ("postgres-primary", 5432),
        ("pgbouncer", 5432),
        ("postgres-replica-1", 5432),
        ("postgres-replica-2", 5432),
    ]

    print("\nTesting port connectivity...")
    print("=" * 60)

    all_connected = True

    for host, port in hostnames:
        try:
            # Try to resolve hostname first
            socket.gethostbyname(host)

            # Try to connect to the port
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((host, port))
            sock.close()

            if result == 0:
                print(f"✓ {host:25}:{port:<5} → Reachable")
            else:
                print(f"✗ {host:25}:{port:<5} → Connection refused")
                all_connected = False

        except socket.gaierror:
            print(f"✗ {host:25}:{port:<5} → Hostname unresolved")
            all_connected = False
        except socket.timeout:
            print(f"✗ {host:25}:{port:<5} → Connection timeout")
            all_connected = False
        except Exception as e:
            print(f"✗ {host:25}:{port:<5} → Error: {e}")
            all_connected = False

    print("=" * 60)

    if all_connected:
        print("SUCCESS: All ports are reachable!")
        return 0
    else:
        print("FAILURE: Some ports are not reachable.")
        return 1


def main():
    """Main function to run all tests."""
    print("PostgreSQL Cluster DNS & Connectivity Test")
    print("=" * 60)

    dns_result = test_dns_resolution()
    port_result = test_port_connectivity()

    print("\n" + "=" * 60)
    print("SUMMARY:")
    print(f"DNS Resolution: {'PASS' if dns_result == 0 else 'FAIL'}")
    print(f"Port Connectivity: {'PASS' if port_result == 0 else 'FAIL'}")

    if dns_result == 0 and port_result == 0:
        print("\n✅ All tests passed! The network is correctly configured.")
        return 0
    else:
        print("\n❌ Some tests failed. Check the troubleshooting steps above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
