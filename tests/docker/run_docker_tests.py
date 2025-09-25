#!/usr/bin/env python3
"""
Docker integration test runner.

This script runs Docker-based integration tests for all supported database engines.
It can be used standalone or as part of the Docker Compose test suite.
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path


def wait_for_service(service_name: str, max_wait: int = 60) -> bool:
    """Wait for a Docker Compose service to be healthy."""
    print(f"Waiting for {service_name} to be ready...")

    for i in range(max_wait):
        try:
            result = subprocess.run(
                ["docker-compose", "ps", "--services", "--filter", "status=running"],
                capture_output=True,
                text=True,
                check=True,
            )

            if service_name in result.stdout:
                # Check if service is healthy
                health_result = subprocess.run(
                    ["docker-compose", "ps", service_name],
                    capture_output=True,
                    text=True,
                    check=True,
                )

                if "healthy" in health_result.stdout or "Up" in health_result.stdout:
                    print(f"✓ {service_name} is ready")
                    return True

            time.sleep(1)

        except subprocess.CalledProcessError:
            pass

    print(f"✗ {service_name} failed to become ready within {max_wait} seconds")
    return False


def run_tests(engine: str = None, verbose: bool = False) -> int:
    """Run Docker integration tests for specified engine or all engines."""

    # Change to project root directory
    project_root = Path(__file__).parent.parent.parent
    os.chdir(project_root)

    # Determine which tests to run
    if engine:
        test_patterns = [f"tests/docker/test_{engine}_docker.py"]
        services_needed = [engine]
    else:
        test_patterns = ["tests/docker/"]
        services_needed = ["postgres", "mysql"]

    # Start required services
    if services_needed:
        print("Starting required Docker services...")

        try:
            # Start services
            subprocess.run(["docker-compose", "up", "-d"] + services_needed, check=True)

            # Wait for services to be ready
            for service in services_needed:
                if not wait_for_service(service):
                    print(f"Failed to start {service} service")
                    return 1

        except subprocess.CalledProcessError as e:
            print(f"Failed to start Docker services: {e}")
            return 1

    # Run the tests
    print(f"Running Docker integration tests for {engine or 'all engines'}...")

    pytest_args = [
        "python",
        "-m",
        "pytest",
        "-v" if verbose else "-q",
        "--tb=short",
        "-m",
        "integration",
    ] + test_patterns

    # Set environment variables for database connections
    env = os.environ.copy()
    env.update(
        {
            "POSTGRES_HOST": "localhost",
            "POSTGRES_PORT": "5432",
            "POSTGRES_USER": "sqlitch",
            "POSTGRES_PASSWORD": "test",
            "POSTGRES_DB": "sqlitch_test",
            "MYSQL_HOST": "localhost",
            "MYSQL_PORT": "3306",
            "MYSQL_USER": "sqlitch",
            "MYSQL_PASSWORD": "test",
            "MYSQL_DB": "sqlitch_test",
        }
    )

    try:
        result = subprocess.run(pytest_args, env=env)
        return result.returncode

    except KeyboardInterrupt:
        print("\nTest run interrupted by user")
        return 130

    finally:
        # Clean up services if we started them
        if services_needed:
            print("Stopping Docker services...")
            try:
                subprocess.run(
                    ["docker-compose", "down"], check=True, capture_output=True
                )
            except subprocess.CalledProcessError:
                pass


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run Docker-based integration tests for sqlitch"
    )

    parser.add_argument(
        "--engine",
        choices=["postgresql", "mysql"],
        help="Run tests for specific database engine only",
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Run tests in verbose mode"
    )

    parser.add_argument(
        "--list-engines",
        action="store_true",
        help="List available database engines for testing",
    )

    args = parser.parse_args()

    if args.list_engines:
        print("Available database engines for Docker integration testing:")
        print("  - postgresql: PostgreSQL database engine")
        print("  - mysql: MySQL database engine")
        print("")
        print(
            "Note: SQLite tests are in tests/integration/ since SQLite doesn't need Docker"
        )
        return 0

    return run_tests(args.engine, args.verbose)


if __name__ == "__main__":
    sys.exit(main())
