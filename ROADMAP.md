# Roadmap

Planned improvements, ordered by impact and dependency.

## Short Term (Next Quarter)

### Incremental Loading (Auto Loader)
Replace full-refresh bronze ingestion with Auto Loader + `MERGE INTO` to support growing data volume without full reprocessing. Target: datasets >5M rows.

### Automated Alerting
Configure Databricks SQL alerts on the governance validation queries:
- Row count mismatch (bronze -> silver)
- Column comment coverage < 100%
- Tag coverage < 100%
- Pipeline failure notification via email/Slack

### CI/CD with Databricks Asset Bundles
Define a `databricks.yml` bundle that deploys notebooks to staging/prod workspaces on merge to `main`. Enable automated testing of pipeline idempotency.

### Genie Agent Evaluation Harness
Build a notebook that runs a curated set of natural-language questions against Genie Agent and evaluates SQL accuracy, measuring:
- Table selection accuracy
- Column mapping accuracy
- Filter condition accuracy
- Join avoidance (should be 100% for OBT)

### Complete the Star Schema
Split the attributes still carried inside `fact_flights` into conformed dimensions: `dim_airline` (national and foreign carriers) and `dim_date` (calendar, weekday, holiday and season flags). This turns the current fact + airport dimension model into a full star schema for BI tools.

## Medium Term (6–12 Months)

### Partitioning / Liquid Clustering
Apply liquid clustering on `icao_airline` + `scheduled_departure_date` to `gold.obt_flights` and `gold.fact_flights` when the dataset exceeds 5M rows. Measure query latency before/after.

### Multi-Environment Setup
Separate `dev`, `staging`, and `prod` catalogs. Pipeline reads source CSVs from a dev volume and writes to `dev` catalog; promotion to staging/prod via bundle deployment.

### Data Freshness Monitoring
Implement a check that monitors `dados.gov.br` for new VRA monthly files. Trigger the bronze ingestion notebook automatically when a new file appears.

### Historical Backfill
Extend the data window beyond 12 months by backfilling historical VRA data from ANAC's archive. Validate that the pipeline handles multi-year data without performance degradation.

## Long Term (12+ Months)

### Real-Time Streaming
Explore Structured Streaming from ANAC's API (if available) to reduce data latency from monthly to near-real-time.

### Additional Datasets
Integrate complementary ANAC datasets:
- Flight delays by cause (if IAC 1504 is reinstated)
- Airport infrastructure metrics
- Passenger volume statistics

### Cost Optimization
At >10M rows, evaluate:
- Spot instance pools for pipeline execution
- Delta Cache for frequent Genie queries
- Z-Ordering on high-cardinality filter columns
