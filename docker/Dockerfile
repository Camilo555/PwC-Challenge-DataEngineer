# Multi-Stage Docker Build for Production Optimization
# Stage 1: Build Dependencies and Cache Layers
FROM python:3.11-slim-bullseye AS base

# Build arguments for cache busting and versioning
ARG BUILD_DATE
ARG VCS_REF
ARG VERSION=1.0.0

# Labels for metadata and identification
LABEL org.opencontainers.image.created=$BUILD_DATE \
      org.opencontainers.image.revision=$VCS_REF \
      org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.title="PwC BMAD Platform" \
      org.opencontainers.image.description="Enterprise Data Platform with 99.99% availability" \
      org.opencontainers.image.vendor="PwC Data Engineering Team"

# System-level optimizations
ENV POETRY_VERSION=1.8.3 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PYTHONPATH=/app/src \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

# Install system dependencies with security hardening
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Essential build tools
    build-essential \
    curl \
    wget \
    git \
    # Java runtime for Spark and Kafka
    openjdk-17-jre-headless \
    # Database connectors
    libpq-dev \
    # Compression libraries
    zlib1g-dev \
    liblz4-dev \
    # Network and security tools
    ca-certificates \
    gnupg \
    # Cleanup in same layer
    && rm -rf /var/lib/apt/lists/* \
    && apt-get purge -y --auto-remove \
    && apt-get clean

# Install Poetry with version pinning
RUN pip install --no-cache-dir "poetry==${POETRY_VERSION}"

# Configure Poetry for production
RUN poetry config virtualenvs.create false \
    && poetry config cache-dir /tmp/poetry_cache

WORKDIR /app

# Stage 2: Dependencies Installation
FROM base AS dependencies

# Copy dependency files first for better caching
COPY pyproject.toml poetry.lock ./

# Install Python dependencies
RUN poetry install --only main --no-interaction --no-ansi --no-dev \
    && rm -rf /tmp/poetry_cache

# Stage 3: Application Builder
FROM dependencies AS builder

# Copy source code
COPY src/ ./src/
COPY tests/ ./tests/
COPY docker/ ./docker/
COPY *.py ./
COPY *.toml ./
COPY *.cfg ./
COPY *.ini ./

# Create optimized Python bytecode
RUN python -m compileall src/ tests/

# Stage 4: Production Runtime
FROM python:3.11-slim-bullseye AS production

# Copy system dependencies from base stage
COPY --from=base /usr/lib /usr/lib
COPY --from=base /usr/local /usr/local
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Create non-root user for security
RUN groupadd --gid 1001 appuser \
    && useradd --uid 1001 --gid appuser --shell /bin/bash --create-home appuser

# Set production environment
ENV ENVIRONMENT=production \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

# Copy Python dependencies
COPY --from=dependencies /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=dependencies /usr/local/bin /usr/local/bin

# Copy application code
COPY --from=builder --chown=appuser:appuser /app .

# Create necessary directories with proper permissions
RUN mkdir -p /app/data/{raw,bronze,silver,gold,warehouse} /app/logs \
    && chown -R appuser:appuser /app \
    && chmod -R 755 /app

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 8000

# Enhanced health check with retry logic
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/api/v1/health || exit 1

# Production command with optimizations
CMD ["uvicorn", "api.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "4", \
     "--worker-class", "uvicorn.workers.UvicornWorker", \
     "--log-level", "info", \
     "--access-log", \
     "--loop", "uvloop", \
     "--http", "httptools"]

# Stage 5: Development Runtime (Optional)
FROM production AS development

USER root

# Install development dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Development tools
    vim \
    htop \
    netcat \
    # Debugging tools
    strace \
    tcpdump \
    && rm -rf /var/lib/apt/lists/*

# Install development Python packages
COPY pyproject.toml poetry.lock ./
RUN pip install poetry \
    && poetry config virtualenvs.create false \
    && poetry install --with dev,test --no-interaction --no-ansi

USER appuser

# Override command for development
CMD ["uvicorn", "api.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--reload", \
     "--log-level", "debug"]
