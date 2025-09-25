#!/usr/bin/env python3
"""
Robust installation script for sqlitch that handles common dependency issues.

This script provides fallback mechanisms for problematic dependencies like
psycopg2-binary on macOS and other platforms.
"""

import subprocess
import sys
import platform
import os
from pathlib import Path

def run_command(cmd, check=True, capture_output=True):
    """Run a command and return the result."""
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            check=check, 
            capture_output=capture_output,
            text=True
        )
        return result
    except subprocess.CalledProcessError as e:
        if capture_output:
            print(f"Command failed: {cmd}")
            print(f"Error: {e.stderr}")
        return e

def install_postgresql_macos():
    """Install PostgreSQL on macOS using Homebrew."""
    print("Installing PostgreSQL on macOS...")
    try:
        # Check if brew is available
        subprocess.run(["brew", "--version"], check=True, capture_output=True)
        
        # Install PostgreSQL
        result = run_command("brew install postgresql")
        if result.returncode == 0:
            print("✅ PostgreSQL installed successfully")
            
            # Add to PATH
            pg_bin = subprocess.run(
                ["brew", "--prefix", "postgresql"], 
                capture_output=True, 
                text=True, 
                check=True
            ).stdout.strip() + "/bin"
            
            current_path = os.environ.get("PATH", "")
            if pg_bin not in current_path:
                os.environ["PATH"] = f"{pg_bin}:{current_path}"
                print(f"✅ Added {pg_bin} to PATH")
            
            return True
        else:
            print("❌ Failed to install PostgreSQL")
            return False
            
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ Homebrew not available or PostgreSQL installation failed")
        return False

def install_dependencies():
    """Install sqlitch dependencies with fallback strategies."""
    print("Installing sqlitch dependencies...")
    
    # Upgrade pip first
    print("Upgrading pip...")
    run_command(f"{sys.executable} -m pip install --upgrade pip")
    
    # Clear pip cache to avoid corrupted wheels
    print("Clearing pip cache...")
    run_command(f"{sys.executable} -m pip cache purge", check=False)
    
    # Try to install with pyproject.toml first
    print("Attempting installation with pyproject.toml...")
    result = run_command(f"{sys.executable} -m pip install -e .[dev]", check=False)
    
    if result.returncode == 0:
        print("✅ Installation successful with pyproject.toml")
        return True
    
    # If that fails, try with requirements.txt
    print("Pyproject.toml installation failed, trying requirements.txt...")
    if Path("requirements-dev.txt").exists():
        result = run_command(f"{sys.executable} -m pip install -r requirements-dev.txt", check=False)
        if result.returncode == 0:
            print("✅ Installation successful with requirements-dev.txt")
            return True
    
    # If that fails, try individual packages
    print("Requirements file installation failed, trying individual packages...")
    core_packages = [
        "click>=8.0.0",
        "PyMySQL>=1.0.0", 
        "GitPython>=3.1.0",
        "Jinja2>=3.0.0",
        "configparser>=5.0.0"
    ]
    
    # Install core packages first
    for package in core_packages:
        result = run_command(f"{sys.executable} -m pip install '{package}'", check=False)
        if result.returncode != 0:
            print(f"❌ Failed to install {package}")
            return False
    
    # Try psycopg2-binary with fallback to psycopg2
    print("Installing PostgreSQL driver...")
    result = run_command(f"{sys.executable} -m pip install 'psycopg2-binary>=2.9.0'", check=False)
    if result.returncode != 0:
        print("psycopg2-binary failed, trying psycopg2...")
        result = run_command(f"{sys.executable} -m pip install 'psycopg2>=2.9.0'", check=False)
        if result.returncode != 0:
            print("❌ Failed to install PostgreSQL driver")
            print("You may need to install PostgreSQL development libraries manually")
            return False
    
    # Install development packages
    dev_packages = [
        "pytest>=7.0.0",
        "pytest-cov>=4.0.0",
        "pytest-xdist>=3.0.0",
        "pytest-mock>=3.10.0",
        "mypy>=1.0.0",
        "black>=22.0.0",
        "flake8>=5.0.0",
        "isort>=5.0.0"
    ]
    
    for package in dev_packages:
        result = run_command(f"{sys.executable} -m pip install '{package}'", check=False)
        if result.returncode != 0:
            print(f"⚠️  Warning: Failed to install {package}")
    
    print("✅ Installation completed with fallback method")
    return True

def main():
    """Main installation function."""
    print("🚀 Sqlitch Installation Script")
    print("=" * 50)
    
    print(f"Python version: {sys.version}")
    print(f"Platform: {platform.system()} {platform.release()}")
    print(f"Architecture: {platform.machine()}")
    print()
    
    # Handle macOS PostgreSQL installation
    if platform.system() == "Darwin":
        print("Detected macOS - checking PostgreSQL installation...")
        
        # Check if pg_config is available
        try:
            subprocess.run(["pg_config", "--version"], check=True, capture_output=True)
            print("✅ PostgreSQL development tools already available")
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("PostgreSQL development tools not found")
            if not install_postgresql_macos():
                print("⚠️  Warning: PostgreSQL installation failed")
                print("You may need to install PostgreSQL manually:")
                print("  brew install postgresql")
                print()
    
    # Install dependencies
    if install_dependencies():
        print()
        print("🎉 Installation completed successfully!")
        print()
        print("You can now run:")
        print("  sqlitch --help")
        print("  python -m pytest tests/")
        return 0
    else:
        print()
        print("❌ Installation failed!")
        print()
        print("Manual installation steps:")
        print("1. Install PostgreSQL development libraries:")
        if platform.system() == "Darwin":
            print("   brew install postgresql")
        elif platform.system() == "Linux":
            print("   sudo apt-get install postgresql-dev  # Ubuntu/Debian")
            print("   sudo yum install postgresql-devel    # CentOS/RHEL")
        print("2. Install Python dependencies:")
        print("   pip install -r requirements-dev.txt")
        return 1

if __name__ == "__main__":
    sys.exit(main())