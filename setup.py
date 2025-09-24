"""Setup configuration for sqlitch."""

from setuptools import setup, find_packages

setup(
    name="sqlitch",
    version="1.0.0",
    description="Python port of sqitch database change management tool",
    long_description="Python port of sqitch database change management tool",
    long_description_content_type="text/markdown",
    author="Sqlitch Contributors",
    author_email="contributors@sqlitch.org",
    url="https://github.com/sqlitch/sqlitch-python",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "click>=8.0.0",
        "psycopg2-binary>=2.9.0",
        "PyMySQL>=1.0.0",
        "GitPython>=3.1.0",
        "Jinja2>=3.0.0",
        "configparser>=5.0.0",
    ],
    extras_require={
        "oracle": ["cx_Oracle>=8.0.0"],
        "snowflake": ["snowflake-connector-python>=2.7.0"],
        "vertica": ["vertica-python>=1.0.0"],
        "exasol": ["pyexasol>=0.25.0"],
        "firebird": ["fdb>=2.0.0"],
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "mypy>=1.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "isort>=5.0.0",
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
    ],
)