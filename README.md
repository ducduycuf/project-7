# Glamira E-commerce Data Pipeline Project

## Overview

Dự án xây dựng end-to-end data pipeline cho Glamira - một nền tảng e-commerce bán trang sức. Pipeline bao gồm thu thập dữ liệu, lưu trữ, xử lý, và visualization để phân tích business performance.

---

## Business Objectives

1. **Revenue Analysis**: Phân tích doanh thu theo thời gian, currency, customer segment
2. **Geographic Distribution**: Hiểu phân bố khách hàng theo địa lý
3. **Time-based Trends**: Phát hiện patterns theo ngày, tuần, tháng
4. **Product Performance**: Đánh giá hiệu suất sản phẩm và categories

_Dashboard - Looker Studio link: https://lookerstudio.google.com/reporting/8f5dc6b2-b6ba-4985-b6e8-18b1fc26e039_
---

## Technology Stack

### Infrastructure
| Component | Technology | Purpose |
|-----------|------------|---------|
| Cloud Platform | Google Cloud Platform (GCP) | Hosting & Services |
| Compute | GCP Virtual Machine | MongoDB hosting |
| Storage | Google Cloud Storage (GCS) | Data Lake |
| Data Warehouse | BigQuery | Analytics & Storage |

### Data Processing
| Component | Technology | Version |
|-----------|------------|---------|
| Source Database | MongoDB | 6.x |
| Transformation | dbt (data build tool) | 1.11.2 |
| Language | Python | 3.10+ |
| Language | SQL | BigQuery SQL |

### Visualization
| Component | Technology | Purpose |
|-----------|------------|---------|
| BI Tool | Looker Studio | Dashboards & Reports |

---

## Data Architecture

### Source Data (MongoDB)

| Collection | Description | Key Fields |
|------------|-------------|------------|
| `main_collection` | All user events | `_id`, `collection`, `order_id`, `cart_products`, `email_address` |
| `ip_locations` | IP to location mapping | `ip`, `country`, `region`, `city` |
| `product_collection` | Product catalog | `product_id`, `name`, `category_name`, `price` |

### Data Warehouse (BigQuery)

#### Raw Layer
```
project6.main_collection      # Raw events
project6.ip_locations         # IP mapping
project6.product_collection   # Product catalog

```

#### Staging Layer
```
glamira_staging.location    
glamira_staging.product        
glamira_staging.order   

```

#### Analytics Layer (Star Schema)

```
                    ┌─────────────────┐
                    │   dim_date      │
                    │   (date_key)    │
                    └────────┬────────┘
                             │
┌─────────────────┐    ┌─────┴─────┐    ┌─────────────────┐
│  dim_customer   │────│fact_sales │────│  dim_product    │
│ (customer_key)  │    │           │    │  (product_key)  │
└─────────────────┘    └─────┬─────┘    └─────────────────┘
                             │
                    ┌────────┴────────┐
                    │  dim_location   │
                    │ (location_key)  │
                    └─────────────────┘
```

### Dimension Tables

| Table | Grain | Key | Description |
|-------|-------|-----|-------------|
| `dim_customer` | 1 row per customer + email (SCD Type 2) | `customer_key` (BIGINT) | Customer info with email history |
| `dim_product` | 1 row per product | `product_key` (BIGINT) | Product catalog |
| `dim_date` | 1 row per calendar date | `date_key` (YYYYMMDD) | Date attributes |
| `dim_location` | 1 row per (country, region, city) | `location_key` (BIGINT) | Geographic info |

### Fact Table

| Table | Grain | Description |
|-------|-------|-------------|
| `fact_sales` | 1 row per (order_id + product_id) | Sales transactions |

**Fact Measures:**
- `order_qty` - Quantity ordered
- `unit_price_local` - Unit price in local currency
- `sales_amount_local` - Total in local currency
- `exchange_rate` - Currency conversion rate
- `sales_amount_usd` - Total converted to USD

---

## Data Pipeline Process

### Phase 1: Data Collection

```
1. Setup GCP Project & VM
         ↓
2. Install MongoDB on VM
         ↓
3. Load Glamira raw data to MongoDB
         ↓
4. Process IP locations (ip2location)
         ↓
5. Crawl product names
         ↓
6. Create Data Dictionary
```

### Phase 2: Data Pipeline

```
1. Export MongoDB → GCS (CSV/JSONL/Parquet)
         ↓
2. Create BigQuery dataset & schemas
         ↓
3. Load GCS → BigQuery (raw layer)
         ↓
4. Setup Cloud Functions (auto-trigger)
         ↓
5. Data Profiling & Quality Check
```

### Phase 3: Transformation & Visualization

```
1. Setup dbt project
         ↓
2. Create staging models (stg_*)
         ↓
3. Build dimension tables (dim_*)
         ↓
4. Build fact table (fact_sales)
         ↓
5. Add dbt tests
         ↓
6. Create Looker dashboards
```

---

## Dashboards

### 1. Revenue Analysis
- Total Revenue (USD)

### 2. Geographic Distribution
- Revenue by Country (Geo Map)

### 3. Product Performance
- Revenue by Product Name
- Quantity by Product Name

---
