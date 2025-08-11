# 🚀 Data Engineering Challenge - Retail ETL Pipeline

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5+-orange.svg)](https://spark.apache.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-green.svg)](https://fastapi.tiangolo.com/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## 📋 Overview

Production-ready ETL pipeline for retail data processing using modern data engineering best practices:
- **Bronze-Silver-Gold** architecture with Delta Lake
- **PySpark** for all data transformations
- **Star Schema** data warehouse design
- **Vector Search** with Typesense for semantic product search
- **RESTful API** with FastAPI
- **Docker** containerization

## 🏗️ Architecture

```mermaid
graph LR
    A[Raw Data] --> B[Bronze Layer]
    B --> C[Silver Layer]
    C --> D[Gold Layer]
    D --> E[Data Warehouse]
    E --> F[FastAPI]
    D --> G[Typesense]
    G --> F



🛠️ Tech Stack

ETL Framework: PySpark 3.5+ with Delta Lake
Database: SQLite (dev) / PostgreSQL-ready (prod)
API: FastAPI with Pydantic validation
Vector Search: Typesense with sentence-transformers
Containerization: Docker & Docker Compose
Testing: Pytest with 80%+ coverage target
Code Quality: Black, Ruff, Mypy, Pre-commit hooks

📦 Installation
Prerequisites

Python 3.10+
Poetry (with uv backend recommended)
Docker & Docker Compose
8GB+ RAM (for Spark operations)

Setup

Clone the repository:

bashgit clone https://github.com/Camilo555/PwC-Challenge-DataEngineer.git
cd PwC-Challenge-DataEngineer


Install dependencies:

bashpoetry install --with dev,data

Setup environment variables:

bashcp .env.example .env
# Edit .env with your configurations

Initialize pre-commit hooks:

bashpoetry run pre-commit install
🚀 Quick Start
Run the complete ETL pipeline:
bashmake run-etl
Start the API server:
bashmake api-dev
Run tests:
bashmake test
Start all services with Docker:
bashdocker-compose up -d


📁 Project Structure
de-challenge-retail-etl/
├── src/
│   └── de_challenge/
│       ├── api/          # FastAPI application
│       ├── core/         # Configuration & constants
│       ├── domain/       # Business models (Pydantic)
│       ├── data_access/  # Database models (SQLModel)
│       ├── etl/          # PySpark ETL pipelines
│       └── vector_search/# Typesense integration
├── tests/                # Test suite
├── data/                 # Data directories
│   ├── raw/             # Input files
│   ├── bronze/          # Raw data lake
│   ├── silver/          # Cleaned data
│   ├── gold/            # Business-ready data
│   └── warehouse/       # SQLite database
├── docker/              # Docker configurations
├── docs/                # Documentation
└── scripts/             # Utility scripts



🔄 ETL Pipeline
Bronze Layer (Raw Ingestion)

Ingests CSV, JSON, and PDF files
No transformations, only metadata addition
Delta Lake format with ACID properties

Silver Layer (Cleaning & Validation)

Data cleaning with PySpark
Business validation with Pydantic
Deduplication and standardization

Gold Layer (Star Schema)

Dimension and Fact tables creation
Optimized for analytical queries
Ready for BI tools

🔌 API Endpoints
EndpointMethodDescription/api/v1/salesGETQuery sales fact table/api/v1/productsGETList products/api/v1/searchPOSTVector search for products/api/v1/etl/triggerPOSTManually trigger ETL/docsGETOpenAPI documentation

🧪 Testing
Run the test suite:
bash# Unit tests
poetry run pytest tests/unit/

# Integration tests
poetry run pytest tests/integration/

# With coverage report
poetry run pytest --cov=src/de_challenge --cov-report=html
📊 Data Quality
The pipeline includes comprehensive data quality checks:

Schema validation
Business rule enforcement
Referential integrity
Outlier detection
Completeness metrics

🐳 Docker Deployment
bash# Build and start services
docker-compose up --build

# Services included:
# - API (port 8000)
# - Typesense (port 8108)
# - PostgreSQL (optional, port 5432)
📈 Performance Optimization

Spark adaptive query execution
Delta Lake optimization
API response caching
Connection pooling
Batch processing

🔐 Security

Basic authentication (expandable to JWT)
Input validation with Pydantic
SQL injection prevention
Environment variable management
Docker security best practices

📝 Documentation

Architecture Decision Records
API Documentation
ETL Pipeline Guide
Deployment Guide

🤝 Contributing

Fork the repository
Create a feature branch (git checkout -b feat/amazing-feature)
Commit changes (git commit -m 'feat: add amazing feature')
Push to branch (git push origin feat/amazing-feature)
Open a Pull Request

📄 License
This project is part of the PwC Data Engineering Challenge.
👥 Team

Your Name

🙏 Acknowledgments

PwC for the challenge opportunity
Online Retail II dataset from UCI ML Repository

