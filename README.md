# airbnb
A project who take the open data of airbnb and use it to makes stats (training )

## 1. Data Source & Scope
- [X] Select cities in France from https://insideairbnb.com/fr/get-the-data/
- [X] Identify required datasets (listings, reviews, calendar, neighbourhoods)
- [x] Define batch frequency (daily batch simulated) weekly  batch sheel 

## 2. Repository & Project Setup
- [ ] Define project structure (src/, data/, sql/, docs/)
- [x] Setup Python environment (requirements / pyproject)
- [X] Add .gitignore and .env.example
- [X] Add README skeleton

## 3. Data Ingestion (Extract)
- [X] create also small scrap for this one  https://www.airroi.com/data-portal/markets
- [ ] monitor the run on the database
- [ ] Handle re-runs (skip or overwrite deterministically)

## 4. Raw Data Storage
- [x] Store raw files as-is on the server (CSV / gzip)
- [x] Keep ingestion metadata on the DB (file size, row count)

## 5. Data Cleaning & Staging (Transform – step 1)
- [ ] Type normalization (price, dates, booleans)
- [ ] Handle missing values
- [x] Deduplicate records using business keys
- [ ] Store clean data in staging format (Parquet)

## 6. Data Quality & Monitoring
- [ ] Compute data quality metrics per run:
  [x] number of input/output rows
  [ ] deploying message into a monitoring
  - number of duplicates removed
  - percentage of null values per column
  - number of unique business keys
- [ ] Store data quality metrics in a dedicated table

## 7. Database & Warehouse Modeling
- [x] Setup database (Postgres)
- [ ] thinking about DB schema
- [ ] Load clean data into warehouse tables

## 8. KPI Definition & Computation
- [ ] Define KPI calculation logic
- [ ] Compute KPI tables:
  - Most common host first names
  - Most positive reviewer first names
  - Multi-listing hosts by first name
  - Median price by neighbourhood
  - Median price by room type
  - (Optional) Availability rate by neighbourhood
  - (Optional) Number of reviews per listing

## 9. Pipeline Orchestration
- [ ] Create a single pipeline entrypoint (run script)
- [ ] Execute steps in order:
      ingest → clean → quality → warehouse → KPI
- [ ] Add basic logging and error handling

## 10. Automation
- [ ] Schedule pipeline (cron or GitHub Actions)
- [ ] Simulate daily batch runs

## 11. Reporting
- [ ] Generate a simple analytical report (markdown or notebook)
- [ ] Include key charts and insights

## 12. Documentation
- [ ] Complete README:
  - project overview
  - pipeline architecture diagram
  - data model
  - KPI definitions
  - example pipeline run