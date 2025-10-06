# AWS Payments Data Lake

### Architected and implemented an AWS data lake using Glue and S3 to ingest, transform, and catalog synthetic payments data.

---

## Overview

**Payments Lake** is an end-to-end data lake project designed to simulate a real-world payments data platform.
It demonstrates key data-engineering concepts using AWS services — including raw data ingestion, ETL pipelines, schema discovery, orchestration, and monitoring — all with a realistic medallion-style architecture.

The project uses synthetic payments data generated locally and stored in Amazon S3. AWS Glue performs transformations across **Raw**, **Bronze**, and **Silver** layers, with results queried via **Athena** and monitored using **CloudWatch**, **EventBridge**, and **SNS**.

---

## Architecture

# **Add diagram from LucidChart**

```
Local Generator  →  S3 (Raw)  →  Glue Crawler (Raw)  →  Glue Job (Bronze)
                                         ↓
                                 S3 (Bronze Parquet)
                                         ↓
                                Glue Crawler (Bronze)
                                         ↓
                                 Glue Job (Silver)
                                         ↓
                                S3 (Silver Curated)
                                         ↓
                                Glue Crawler (Silver)
                                         ↓
                                     Athena (SQL)

        ┌───────────────────── Orchestration (Glue Triggers) ─────────────────────┐
        Raw→Bronze  →  Bronze→Crawler  →  BronzeCrawler→Silver  →  Silver→Crawler
        └────────────────────────────────────────────────────────────────────────┘

        ┌───────────────────── Monitoring & Alerts ──────────────────────────────┐
        Glue Jobs → EventBridge (Job State Change) → SNS → Email Notifications
        └────────────────────────────────────────────────────────────────────────┘
```

---

## Data Lake Zones

| Zone               | Storage Path                                                  | Description                                     |
| ------------------ | ------------------------------------------------------------- | ----------------------------------------------- |
| **Raw**            | `s3://payments-lake-jordanpacho/raw/transactions/`            | Synthetic CSVs ingested daily.                  |
| **Bronze**         | `s3://payments-lake-jordanpacho/bronze/transactions_parquet/` | Type-casted, deduplicated, partitioned Parquet. |
| **Silver**         | `s3://payments-lake-jordanpacho/silver/transactions_curated/` | Validated and curated data ready for analytics. |
| **Audit**          | `s3://payments-lake-jordanpacho/audit/`                       | Invalid records and data-quality summaries.     |
| **Athena Results** | `s3://payments-lake-jordanpacho/athena-results/`              | Athena query outputs.                           |

---

## AWS Services Used

| Service                   | Role                | Description                                                            |
| ------------------------- | ------------------- | ---------------------------------------------------------------------- |
| **Amazon S3**             | Data Storage        | Central lake storage for all zones (raw → curated).                    |
| **AWS Glue Crawlers**     | Schema Discovery    | Scans data in each zone and updates the Glue Data Catalog.             |
| **AWS Glue Jobs**         | ETL (Processing)    | PySpark scripts for cleaning, validating, and writing Parquet outputs. |
| **AWS Glue Triggers**     | Orchestration       | Event-based triggers chaining crawlers and jobs into a single flow.    |
| **AWS Glue Data Catalog** | Metadata Store      | Central schema repository used by Athena.                              |
| **Amazon Athena**         | Query Engine        | Serverless SQL layer for querying curated Parquet data in Silver.      |
| **Amazon EventBridge**    | Monitoring (Events) | Detects job state changes (FAILED) in real time.                       |
| **Amazon SNS**            | Alerting            | Sends email notifications when jobs fail.                              |
| **Amazon CloudWatch**     | Logging & Metrics   | Stores Glue job logs and provides metric-based monitoring.             |
| **Python (Local)**        | Data Generator      | Creates synthetic payments transactions for ingestion.                 |

---

## Data Pipeline Flow

1. **Local Data Generator** produces daily CSVs with schema:
   `txn_id, merchant_id, user_id, amount, currency, status, txn_ts, country`
2. **Raw Zone** – CSVs uploaded to S3.
3. **Raw Crawler** infers schema and populates the Glue Data Catalog.
4. **Bronze Job** – cleans, casts, and deduplicates data → writes Snappy Parquet partitioned by `txn_date`.
5. **Bronze Crawler** – updates the catalog for new partitions.
6. **Silver Job** – validates transactions (amount, status, currency), adds curated fields, and outputs:

   * Curated Silver dataset (valid records)
   * Audit folder (invalids with reasons)
   * Data-quality summary JSON.
7. **Silver Crawler** – catalogs curated Parquet for Athena.
8. **Athena** – queries Silver data directly from S3.
9. **EventBridge + SNS** – sends email alerts on job failures in real time.

---

## Monitoring & Alerting

* **EventBridge Rules**

  * Detect `Glue Job State Change` events where `state = FAILED`.
  * One rule per job (Bronze and Silver).
  * Target: SNS topic `MiniPaymentsAlerts`.

* **SNS Topic**

  * Sends email notifications to subscribed users.

* **CloudWatch Logs**

  * Retains full job logs for debugging and DQ tracking.

---

## Real-World Relevance

This architecture mirrors how production data lakes operate:

| Real-World Concern        | How This Project Demonstrates It                                   |
| ------------------------- | ------------------------------------------------------------------ |
| **Ingestion scalability** | S3 can handle large batch or streaming writes from source systems. |
| **Schema management**     | Glue Crawlers maintain an evolving catalog without manual DDL.     |
| **ETL orchestration**     | Glue Triggers emulate Airflow/Step Function workflows.             |
| **Observability**         | EventBridge + SNS provides real-time job-failure alerts.           |
| **Query-ready data**      | Silver Parquet layer makes analytics serverless via Athena.        |

---

## Potential Upgrades

| Area             | Upgrade                                                     | Benefit                            |
| ---------------- | ----------------------------------------------------------- | ---------------------------------- |
| **Gold Layer**   | Add aggregated datasets (e.g. merchant KPIs, daily totals). | Enables business-ready analytics.  |
| **Data Quality** | Integrate Great Expectations or Deequ.                      | Automated validation reports.      |
| **CI/CD**        | Deploy jobs via AWS CDK or Terraform.                       | Version-controlled infrastructure. |
| **Streaming**    | Add Kinesis Data Firehose or Kafka ingestion.               | Real-time payment data simulation. |
| **Monitoring**   | Push DQ metrics to CloudWatch Dashboards.                   | Visual performance tracking.       |
| **Security**     | Add IAM policies and KMS encryption.                        | Production-grade governance.       |

---

## Repository Structure

```
payments-lake/
├── src/
│   ├── data_generator/
│   │   └── generate_transaction_data.py
│   ├── glue_jobs/
│   │   ├── bronze_job.py
│   │   └── silver_job.py
│   └── utils/
│       └── dq_helpers.py
├── diagrams/
│   └── architecture.png
├── README.md
└── requirements.txt
```

---

## Key Takeaways

* Built a complete, cloud-native data lake architecture on AWS.
* Automated ingestion, transformation, and monitoring end-to-end.
* Demonstrated modern data-engineering concepts (medallion architecture, observability, and serverless analytics).
