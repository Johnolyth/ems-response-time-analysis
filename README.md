# EMS Response Time Case Study

## Overview
This repository contains the data pipeline and analysis framework for an EMS response time case study focused on understanding response time distributions, identifying tail delays (p95), and analyzing contributing components across jurisdictions.

The project is structured as a reproducible workflow that transforms raw EMS data into standardized datasets and produces analytical outputs used for case study reporting.

---

## Objectives
- Build a reproducible EMS data pipeline
- Standardize and canonicalize EMS datasets
- Analyze response time distributions (median, p95)
- Identify contributors to long response times (tail analysis)
- Evaluate variation by geography and urbanicity
- Support local jurisdiction-level insights

---

## Project Structure
ems-data-project/
├── scripts/ # Core pipeline and analysis scripts
├── data/ # Raw and processed data (excluded from repo)
├── output/ # Generated outputs (excluded from repo)
├── docs/ # Notes and supporting documentation
├── notebooks/ # Exploratory analysis
├── ref/ # Reference materials
├── admin/ # Project support files
├── .gitignore
├── README.md


---

## Pipeline Overview

The workflow is organized into sequential stages:

### Setup
- `00_setup.R`  
  Initializes environment and project configuration

- `scripts/00_run_pipeline_master.R`  
  Orchestrates full pipeline execution

---

### Data Preparation
- `scripts/02_load_data.R`  
  Loads raw datasets

- `scripts/03_clean_data.R`  
  Cleans and prepares data

- `scripts/03b_standardize_canonical.R`  
  Standardizes fields into canonical schema

- `scripts/03c_local_ingest_combine.R`  
  Integrates local data sources

---

### Canonical Dataset Construction
- `scripts/04_build_canonical.R`  
  Builds unified canonical datasets

- `scripts/04_inventory_QA.R`  
  Data quality checks and validation

- `scripts/04c_build_local_canonical.R`  
  Local dataset construction

- `scripts/04c_build_local_geo.R`  
  Geographic enrichment

---

### Analysis Layer

#### Core Analysis
- `scripts/05_01_response_time_core.R`  
  Core response time calculations

- `scripts/05_01b_response_time_quantiles.R`  
  Quantile analysis (including p95)

---

#### Delay & Tail Analysis
- `scripts/05_00_build_delay_flags_feature.R`  
  Constructs delay indicators

- `scripts/05_01c_response_time_by_delay_flags.R`  
  Stratifies response times by delay presence

- `scripts/05_01d_response_time_no_delay_by_urbanicity.R`  
  Compares no-delay cases across urbanicity

- `scripts/05_02_no_delay_tail_composition.R`  
  Decomposes p95 tail contributions

- `scripts/05_03_suburban_transport_tail_hotspots.R`  
  Identifies transport-driven tail hotspots

- `scripts/05_04_no_delay_transport_ratio_analysis.R`  
  Evaluates transport contribution ratios

---

#### Local / Jurisdiction-Level Analysis
- `scripts/05_10_local_snhd_response_time_quantiles_by_jurisdiction.R`  
  Jurisdiction-level quantile analysis

- `scripts/05_11_local_snhd_tail_component_breakdown.R`  
  Tail component breakdown by jurisdiction

---

## Data

This repository does **not include raw or processed data**.

Data sources (e.g., NEMSIS and local datasets) are excluded due to:
- size constraints
- access restrictions
- data governance considerations

This repository is intended to track:
- code
- pipeline structure
- analytical logic
- reproducibility of methods

---

## Key Concepts

- **System Response Time**  
  Total time from dispatch to arrival on scene

- **Component Times**
  - Dispatch center time
  - Chute time
  - Scene response time
  - Transport time

- **p95 (95th percentile)**  
  Used to evaluate tail performance and identify extreme delays

---

## Current Status

Active development:
- Pipeline established and functional
- Core and local analyses implemented
- Expanding case study insights and validation

---

## Notes

This project is part of a broader case study examining EMS response performance and identifying operational improvement opportunities.
