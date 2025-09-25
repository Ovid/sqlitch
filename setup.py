#!/usr/bin/env python3
"""
Setup script for sqlitch - Python port of sqitch database change management tool.

This setup.py is provided as a fallback for environments that don't support
pyproject.toml. The primary configuration is in pyproject.toml.
"""

from setuptools import setup, find_packages
import os

# Read the README file
def read_readme():
    with open("README.md", "r", encoding="utf-8") as fh:
        return fh.read()

# Read requirements from requirements.txt
def read_requirements():
    requirements = []
    if os.path.exists("requirements.txt"):
        with open("requirements.txt", "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line and not line.startswith("#"):
                    requirements.append(line)
    return requirements

setup(
    name="sqlitch",
    version="1.0.0",
    description="Python port of sqitch database change management tool",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    author="Sqlitch Contributors",
    author_email="contributors@sqlitch.org",
    maintainer="Sqlitch Contributors",
    maintainer_email="contributors@sqlitch.org",
    url="https://github.com/sqlitch/sqlitch-python",
    project_urls={
        "Homepage": "https://github.com/sqlitch/sqlitch-python",
        "Documentation": "https://sqlitch.readthedocs.io",
        "Repository": "https://github.com/sqlitch/sqlitch-python",
        "Bug Tracker": "https://github.com/sqlitch/sqlitch-python/issues",
        "Changelog": "https://github.com/sqlitch/sqlitch-python/blob/main/CHANGELOG.md",
    },
    packages=find_packages(exclude=["tests*"]),
    include_package_data=True,
    package_data={
        "sqlitch": ["py.typed"],
    },
    python_requires=">=3.9",
    install_requires=read_requirements(),
    extras_require={
        "oracle": ["cx_Oracle>=8.0.0"],
        "snowflake": ["snowflake-connector-python>=2.7.0"],
        "vertica": ["vertica-python>=1.0.0"],
        "exasol": ["pyexasol>=0.25.0"],
        "firebird": ["fdb>=2.0.0"],
        "all": [
            "cx_Oracle>=8.0.0",
            "snowflake-connector-python>=2.7.0",
            "vertica-python>=1.0.0",
            "pyexasol>=0.25.0",
            "fdb>=2.0.0",
        ],
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "pytest-xdist>=3.0.0",
            "pytest-mock>=3.10.0",
            "mypy>=1.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "isort>=5.0.0",
            "pre-commit>=3.0.0",
            "tox>=4.0.0",
            "docker>=6.0.0",
        ],
        "docs": [
            "sphinx>=5.0.0",
            "sphinx-rtd-theme>=1.2.0",
            "myst-parser>=0.18.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "sqlitch=sqlitch.cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: Software Development :: Version Control",
        "Typing :: Typed",
    ],
    keywords=["database", "migration", "sqitch", "sql", "deployment"],
    license="MIT",
    zip_safe=False,
)