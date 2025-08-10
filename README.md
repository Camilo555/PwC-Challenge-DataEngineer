# 🚀 Data Engineering Challenge - Retail ETL Pipeline

A comprehensive data engineering solution implementing **medallion architecture** (Bronze/Silver/Gold), **star schema data warehouse**, and **vector search capabilities** for retail analytics.

## 📋 Features

- **Medallion Architecture**: Bronze → Silver → Gold data layers with Delta Lake
- **ETL Pipeline**: PySpark-based transformations with data quality validation
- **Star Schema**: Optimized analytical data warehouse (1 fact + 6 dimension tables)
- **Vector Search**: Semantic search using Typesense and sentence embeddings
- **REST API**: FastAPI with layered architecture and authentication
- **Docker Orchestration**: Complete containerized deployment
- **Future Ready**: Prepared for Supabase (PostgreSQL) integration

## 🏗️ Architecture

```mermaid
graph TB
    subgraph Sources
        CSV[CSV Files]
        JSON[JSON Files]
        PDF[PDF Files]
    end
    
    subgraph Data Lake
        Bronze[(Bronze Layer)]
        Silver[(Silver Layer)]
        Gold[(Gold Layer)]
    end
    
    subgraph Data Warehouse
        SQLite[(SQLite DB)]
        Star[Star Schema]
    end
    
    subgraph Vector DB
        Typesense[(Typesense)]
    end
    
    Sources --> Bronze
    Bronze --> Silver
    Silver --> Gold
    Gold --> SQLite
    SQLite --> Star
    Star --> FastAPI
    Star --> Typesense
    FastAPI --> Client
