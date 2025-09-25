# Multi-stage Dockerfile for sqlitch

# Build stage
FROM python:3.11-slim as builder

# Install system dependencies for building
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    libpq-dev \
    libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY pyproject.toml setup.cfg ./
COPY sqlitch/ ./sqlitch/
COPY README.md CHANGELOG.md LICENSE ./

# Install the package
RUN pip install --no-cache-dir build && \
    python -m build && \
    pip install --no-cache-dir dist/*.whl[all]

# Runtime stage
FROM python:3.11-slim as runtime

# Install runtime system dependencies
RUN apt-get update && apt-get install -y \
    libpq5 \
    libmariadb3 \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r sqlitch && useradd -r -g sqlitch sqlitch

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/sqlitch /usr/local/bin/sqlitch

# Set working directory
WORKDIR /workspace

# Change ownership to sqlitch user
RUN chown -R sqlitch:sqlitch /workspace

# Switch to non-root user
USER sqlitch

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD sqlitch --version || exit 1

# Default command
ENTRYPOINT ["sqlitch"]
CMD ["--help"]

# Development stage
FROM runtime as development

# Switch back to root for development tools
USER root

# Install development dependencies
RUN pip install --no-cache-dir \
    pytest \
    pytest-cov \
    black \
    flake8 \
    mypy \
    isort

# Install additional database clients for testing
RUN apt-get update && apt-get install -y \
    postgresql-client \
    mysql-client \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Switch back to sqlitch user
USER sqlitch

# Override entrypoint for development
ENTRYPOINT ["/bin/bash"]