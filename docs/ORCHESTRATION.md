# Orchestration with Dagster

This document describes the Dagster-based orchestration system for the PwC Data Engineering Challenge, including file-triggered data ingestion and external API enrichment.

## Overview

The orchestration system provides:

- **File Drop Sensors**: Automatic pipeline triggering when files are added to the raw data directory
- **External API Enrichment**: Currency rates, country information, and product categorization
- **Multi-Layer Processing**: Bronze (raw) → Silver (cleaned) → Gold (aggregated) data layers
- **Data Quality Monitoring**: Automated data quality assessment and alerting
- **Flexible Scheduling**: Time-based and event-driven pipeline execution

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   File Sensors  │    │  External APIs  │    │   Schedules     │
│                 │    │                 │    │                 │
│ • File Drop     │    │ • Currency API  │    │ • Daily         │
│ • Large Files   │    │ • Country API   │    │ • Weekly        │
│ • Error Retry   │    │ • Product API   │    │ • Quality Check │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
                    ┌─────────────────────┐
                    │   Dagster Jobs      │
                    │                     │
                    │ • ETL Pipeline      │
                    │ • Quality Monitor   │
                    │ • Incremental       │
                    │ • Reprocessing      │
                    └─────────────────────┘
                                 │
                                 ▼
         ┌─────────────────────────────────────────────┐
         │                Assets                       │
         ├─────────────┬─────────────┬─────────────────┤
         │   Bronze    │   Silver    │      Gold       │
         │             │             │                 │
         │ • Raw Data  │ • Cleaned   │ • Aggregations  │
         │ • Enriched  │ • Validated │ • KPIs          │
         │ • Metadata  │ • Derived   │ • Reports       │
         └─────────────┴─────────────┴─────────────────┘
```

## Quick Start

### 1. Install Dependencies

```bash
# Install Dagster and related packages
poetry add dagster dagster-webserver dagster-pandas aiohttp

# Install all dependencies
poetry install
```

### 2. Configure External APIs

Add API keys to your `.env` file:

```bash
# External API Configuration
CURRENCY_API_KEY=your_exchangerate_api_key_here
ENABLE_EXTERNAL_ENRICHMENT=true
ENRICHMENT_BATCH_SIZE=10
```

### 3. Start Dagster

```bash
# Start the Dagster web server and daemon
python scripts/start_dagster.py
```

### 4. Access the UI

Open your browser to `http://localhost:3000` to access the Dagster UI.

## File Sensors

### File Drop Sensor

Monitors the `data/raw/` directory for new CSV files and automatically triggers the ETL pipeline.

**Configuration:**
- **Status**: Running by default
- **Check Interval**: Every 30 seconds
- **File Types**: `*.csv`
- **Trigger**: New files or modified timestamps

**Usage:**
1. Drop a CSV file into `data/raw/`
2. The sensor detects the file within 30 seconds
3. Automatically triggers the full ETL pipeline
4. Processes with external API enrichment (if enabled)

### Large File Sensor

Special handling for files larger than 10MB with optimized processing.

**Features:**
- **Batch Size Optimization**: Automatically adjusts batch sizes based on file size
- **Resource Management**: Disables enrichment for very large files initially
- **Progress Monitoring**: Enhanced logging and metadata tracking

**Activation:**
```bash
# Enable large file sensor in Dagster UI
# Navigate to Sensors → large_file_sensor → Enable
```

### Error File Sensor

Monitors the `data/raw/errors/` directory for files that failed processing and need retry.

**Features:**
- **Retry Logic**: Waits 5 minutes before retrying failed files
- **Conservative Settings**: Uses smaller batch sizes and disables enrichment
- **Error Tracking**: Maintains processing history and error metadata

## External API Enrichment

### Currency Exchange Rates

**Service**: exchangerate-api.com  
**Free Tier**: 1,500 requests/month  
**Data Added**:
- Current exchange rates (GBP → USD, EUR, CAD, AUD, JPY)
- Historical rates (with API key)
- Currency-converted amounts

### Country Information

**Service**: restcountries.com  
**Free**: No API key required  
**Data Added**:
- Official country names
- Country codes (ISO 2/3 letter)
- Region and continent information
- Population, area, timezone
- Geographic coordinates

### Product Categorization

**Service**: Local ML + DataMuse API  
**Features**:
- Category classification (home_garden, fashion, kitchen, etc.)
- Brand detection
- Seasonal product identification
- Complexity scoring
- Related terms and tags

## Assets (Data Layers)

### Bronze Layer (`raw_retail_data` → `bronze_retail_data`)

**Purpose**: Raw data ingestion with basic transformations and external enrichment

**Transformations**:
- Data cleaning (remove nulls, negatives)
- Column standardization
- External API enrichment (currency, country, product)
- Metadata addition (timestamps, source tracking)

**Output**: Enriched raw data ready for further processing

### Silver Layer (`silver_retail_data`)

**Purpose**: Clean, validated, business-ready data

**Transformations**:
- Data quality improvements
- Country name standardization
- Derived dimensions (year, month, quarter, day_of_week)
- Business metrics calculation
- Quality scoring

**Output**: Analysis-ready dataset with high data quality

### Gold Layer (`gold_sales_summary`)

**Purpose**: Aggregated business metrics and KPIs

**Aggregations**:
- Sales by country and time period
- Top products and customers
- Customer segmentation (VIP, Regular, Low Value)
- Overall business KPIs

**Output**: Executive dashboards and reporting data

### Data Quality Metrics (`data_quality_metrics`)

**Purpose**: Comprehensive data quality assessment

**Metrics**:
- **Completeness**: Percentage of non-null values
- **Validity**: Adherence to business rules
- **Uniqueness**: Distinct value counts
- **Consistency**: Data standardization across records
- **Overall Quality Score**: Weighted composite score

## Jobs and Schedules

### ETL Pipeline Job (`retail_etl_pipeline`)

**Description**: Complete end-to-end processing from raw to gold  
**Trigger**: File sensors or manual execution  
**Assets**: All layers (Bronze → Silver → Gold) + Quality metrics

### Incremental Processing (`incremental_processing`)

**Description**: Process only new/changed data  
**Schedule**: Daily at 2 AM (manual activation required)  
**Assets**: Bronze → Silver → Gold (skips raw data ingestion)

### Data Quality Monitoring (`data_quality_monitoring`)

**Description**: Continuous data quality assessment  
**Schedule**: Every 6 hours  
**Alerts**: Quality score below 85%

### Historical Reprocessing (`historical_reprocessing`)

**Description**: Full reprocessing of historical data  
**Schedule**: Weekly on Sunday at 3 AM  
**Use Case**: Schema changes, bug fixes, data corrections

## Configuration

### Environment Variables

```bash
# External API Configuration
CURRENCY_API_KEY=your_api_key
ENABLE_EXTERNAL_ENRICHMENT=true
ENRICHMENT_BATCH_SIZE=10

# Dagster Configuration
DAGSTER_HOME=./dagster_home

# Data Paths
RAW_DATA_PATH=./data/raw
BRONZE_PATH=./data/bronze
SILVER_PATH=./data/silver
GOLD_PATH=./data/gold
```

### Asset Configuration

Each asset accepts configuration parameters:

```python
# Example configuration for bronze layer
{
  "ops": {
    "bronze_retail_data": {
      "config": {
        "input_file": "retail_transactions.csv",
        "enable_enrichment": true,
        "batch_size": 100
      }
    }
  }
}
```

## Monitoring and Observability

### Dagster UI Features

- **Asset Lineage**: Visual representation of data dependencies
- **Run History**: Complete execution logs and status
- **Asset Materialization**: Track when assets were last updated
- **Sensor Status**: Monitor file sensor activity
- **Job Schedules**: Manage automated execution

### Logging

Comprehensive logging at multiple levels:

```python
# Example log output
INFO - File sensor detected: new_data.csv (1.2MB)
INFO - Starting bronze layer processing...
INFO - Applying external API enrichment...
INFO - Enriched 1,500 records with external APIs
INFO - Bronze processing completed: 1,500 records
INFO - Data quality assessment: 94.5% overall score
```

### Metadata

Each asset execution includes rich metadata:

- Processing timestamps
- Record counts
- Quality metrics
- Performance statistics
- Error information

## Error Handling

### File Processing Errors

1. **Detection**: Failed runs are automatically detected
2. **File Movement**: Failed files moved to `data/raw/errors/`
3. **Retry Logic**: Error file sensor attempts retry with conservative settings
4. **Alerting**: Failed runs trigger notifications in Dagster UI

### API Enrichment Errors

1. **Graceful Degradation**: Pipeline continues if enrichment fails
2. **Error Tracking**: Enrichment errors logged in asset metadata
3. **Partial Success**: Some records may be enriched even if others fail
4. **Retry Strategy**: Built-in retry logic with exponential backoff

## Performance Optimization

### Batch Processing

- **Dynamic Batch Sizing**: Automatically adjusts based on file size
- **Memory Management**: Processes large files in chunks
- **Resource Allocation**: Optimizes Spark configuration for workload

### API Rate Limiting

- **Built-in Rate Limiting**: Respects API provider limits
- **Concurrent Requests**: Batches API calls efficiently
- **Error Backoff**: Exponential backoff on API failures

### Caching

- **Metadata Caching**: Avoid redundant API calls for duplicate data
- **Result Persistence**: Store enrichment results for reuse
- **Smart Invalidation**: Cache invalidation based on data freshness

## Best Practices

### File Management

1. **File Naming**: Use descriptive, timestamp-based naming
2. **File Size**: Keep files under 100MB for optimal processing
3. **File Format**: CSV preferred, ensure consistent schema
4. **Error Handling**: Monitor error directory for failed files

### API Usage

1. **API Keys**: Store securely in environment variables
2. **Rate Limits**: Monitor API usage to stay within free tiers
3. **Error Handling**: Always have fallback options
4. **Testing**: Test with small datasets first

### Monitoring

1. **Regular Checks**: Monitor Dagster UI daily
2. **Quality Metrics**: Set up alerts for quality score drops
3. **Performance**: Track processing times and optimize
4. **Logs**: Review logs for warnings and errors

## Troubleshooting

### Common Issues

**File Sensor Not Triggering**
```bash
# Check file permissions
ls -la data/raw/

# Verify sensor status in Dagster UI
# Sensors → file_drop_sensor → Enable
```

**API Enrichment Failing**
```bash
# Test API connectivity
python -c "
from de_challenge.external_apis.enrichment_service import get_enrichment_service
import asyncio
async def test():
    service = get_enrichment_service()
    health = await service.health_check_all()
    print(health)
asyncio.run(test())
"
```

**Memory Issues with Large Files**
```bash
# Enable large file sensor
# Reduce batch size in configuration
# Use incremental processing instead of full reprocessing
```

### Debug Mode

Enable debug logging:

```bash
export DAGSTER_LOG_LEVEL=DEBUG
python scripts/start_dagster.py
```

## Extending the System

### Adding New APIs

1. Create new client in `external_apis/`
2. Add to enrichment service
3. Update asset configuration
4. Test with small datasets

### Custom Assets

1. Define new asset in `orchestration/assets.py`
2. Add dependencies and configuration
3. Include in job definitions
4. Test pipeline integration

### Additional Sensors

1. Create sensor in `orchestration/sensors.py`
2. Define trigger conditions
3. Add to definitions
4. Test sensor logic

## API References

### Free API Services Used

1. **ExchangeRate-API**: https://exchangerate-api.com/
   - Free tier: 1,500 requests/month
   - Signup: https://app.exchangerate-api.com/sign-up

2. **REST Countries**: https://restcountries.com/
   - Completely free
   - No signup required

3. **DataMuse**: https://www.datamuse.com/api/
   - Free word/concept API
   - No API key required

For production usage, consider upgrading to paid tiers for higher rate limits and additional features.