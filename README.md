### Finance Lakehouse Risk & Analytics Platform

An end-to-end financial analytics lakehouse built using Azure Data Lake Gen2, Databricks, Delta Lake, Snowflake, and Power BI.

This project demonstrates how raw transactional data can be ingested, validated, transformed, and served for analytics, risk monitoring, and data quality reporting using a modern lakehouse architecture.

### Architecture Overview
- Data Flow:
   - Raw transaction files land in Azure Data Lake Gen2
   - Databricks Auto Loader ingests data into the Bronze layer
   - Silver layer applies cleansing, validation, deduplication, and data quality rules
   - Invalid records are routed to a Rejected records table with explicit reject reasons
- Gold layer produces analytics-ready aggregates:
   - Daily KPIs
   - Customer risk metrics
   - Data quality metrics
- Curated Gold tables are loaded into Snowflake
- Power BI consumes Snowflake tables for dashboards

### Bronze Layer – Raw Ingestion
- Purpose: Capture raw, immutable transaction data.
- Key Features:
   - Streaming ingestion using Databricks Auto Loader
   - Schema enforcement and evolution
   - Metadata capture (ingestion timestamp, source file)
   - Exactly-once processing semantics
- Output: Delta table containing raw transaction records

### Silver Layer – Cleansing & Validation
- Purpose: Create a trusted dataset for analytics.
- Transformations Applied:
   - Field trimming and standardization
   - Type casting and timestamp normalization
   - Deduplication based on transaction ID
   - Business rule validation
- Outputs:
   - Silver Clean table > trusted transactions
   - Silver Rejected table > invalid records with reject reasons

### Gold Layer – Analytics & Risk Models
1️. KPI_DAILY
- Daily transaction metrics aggregated by date, country, and currency.
- Metrics:
    - Transaction count
    - Total transaction amount
    - Average transaction amount

2️. CUSTOMER_RISK_DAILY
- Customer-level behavioral risk metrics.
- Features:
    - Daily transaction count
    - Daily spend
    - Maximum transaction amount
    - Risk score and risk band classification (LOW / MEDIUM / HIGH)

3️. REJECTS_DAILY
- Aggregated data quality metrics.
- Metrics:
    - Rejected record count by date and reject reason
    - Enables monitoring of upstream data issues

### Snowflake Integration
Gold Delta tables are loaded into Snowflake using the Spark–Snowflake connector.

- Schema: FINANCE_LAKEHOUSE.GOLD
- Tables:
   - KPI_DAILY
   - CUSTOMER_RISK_DAILY
   - REJECTS_DAILY
- Design Notes:
    - Idempotent reloads supported via truncate-and-load strategy
    - Role-based access control applied for the Databricks loader user
    - Snowflake acts as the analytics serving layer

### Power BI Dashboards
Power BI connects to Snowflake to provide analytics and monitoring dashboards.

Dashboard Includes:
  - Executive Overview – KPIs and transaction trends
  - Customer Risk Analysis – customer-level risk metrics and segmentation
  - Data Quality Monitoring – rejected records by reason and over time

### Data Quality & Governance

- This project explicitly models data quality as a first-class concern:
- Invalid records are not discarded
- Rejected records are captured with clear reason codes
- Data quality metrics are aggregated and visualized
- Enables operational monitoring and root-cause analysis
- This mirrors real-world production data platforms.

### Tech Stack

- Cloud Storage: Azure Data Lake Gen2
- Processing Engine: Azure Databricks (PySpark)
- Storage Format: Delta Lake
- Data Warehouse: Snowflake
- BI & Analytics: Power BI
- Architecture Pattern: Medallion (Bronze / Silver / Gold)
