# EMS Response Time Analysis

## Overview
This project presents a reproducible analytics pipeline for evaluating EMS response time performance, with a focus on understanding long-tail delays (p95), operational components of response time, and variation across jurisdictions.

Rather than relying on averages alone, this analysis emphasizes percentile-based performance to identify where and why extended response times occur.

---

## Project Summary

This project builds a structured analytics pipeline to evaluate EMS response time performance, with a focus on long-tail delays (p95). 

The core objective is to move beyond average response times and identify which operational components (dispatch, chute, scene, transport) contribute most to extended response times across different geographies and jurisdictions.

---

## Why This Matters
Emergency response performance is often summarized using averages, which can obscure critical delays. In practice, **long-tail response times (p95)** can have significant operational and patient impact.

This project focuses on identifying:
- where extended response times occur
- which components contribute most to those delays
- how patterns vary across geography and operational context

---

## What This Project Demonstrates
- End-to-end data pipeline development in R
- Data cleaning, standardization, and canonical modeling
- Quantile-based performance analysis (p95 focus)
- Feature engineering (delay flags)
- Component-level decomposition of response time
- Multi-level analysis (system-wide and jurisdiction-level)
- Reproducible workflow design using Git and GitHub

---

## Methods at a Glance
- Built a multi-stage pipeline to transform raw EMS data into analysis-ready datasets
- Standardized fields into a canonical schema for consistency across sources
- Computed response time distributions (median, p95)
- Engineered delay flags to isolate structural vs incidental delays
- Decomposed system response time into operational components
- Performed subgroup analysis by urbanicity and jurisdiction

---

## Key Analytical Themes
- **Tail-focused analysis** rather than averages alone  
- **Component decomposition** of system response time  
- **Delay flag stratification** to isolate delay mechanisms  
- **Geographic variation** in response performance  
- **Transport and scene contributions** to long response times  

---

## Repository Structure
- `scripts/` – pipeline and analysis scripts  
- `docs/` – project notes and documentation  
- `ref/` – reference materials  
- `admin/` – administrative/support files  
- `notebooks/` – exploratory analysis  
- `data/` – excluded from this repository  
- `output/` – excluded from this repository  

---

## Pipeline Overview (Visual)

## Pipeline Overview (Diagram)

```mermaid
flowchart TD
    A[Raw Data] --> B[Clean & Standardize]
    B --> C[Canonical Dataset]
    C --> D[Feature Engineering<br/>(Delay Flags)]
    D --> E[Analysis<br/>(p95, Components, Geography)]
    E --> F[Outputs<br/>(Tables, Insights)]
```

### Pipeline Orchestration
- `scripts/00_run_pipeline_master.R` – master pipeline runner  
- `00_setup.R` – environment setup  

### Data Preparation
- `scripts/03_clean_data.R` – data cleaning  
- `scripts/03b_standardize_canonical.R` – canonical field standardization  
- `scripts/03c_local_ingest_combine.R` – local data integration  

### Canonical Dataset Construction
- `scripts/04_build_canonical.R` – canonical dataset build  
- `scripts/04_inventory_QA.R` – data validation and QA  
- `scripts/04c_build_local_geo.R` – geographic enrichment  

### Core Analysis
- `scripts/05_01_response_time_core.R` – response time calculation  
- `scripts/05_01b_response_time_quantiles.R` – quantile and p95 analysis  

### Tail & Delay Analysis
- `scripts/05_00_build_delay_flags_feature.R` – delay feature engineering  
- `scripts/05_02_no_delay_tail_composition.R` – tail decomposition  
- `scripts/05_03_suburban_transport_tail_hotspots.R` – hotspot identification  
- `scripts/05_04_no_delay_transport_ratio_analysis.R` – transport contribution  

### Local / Jurisdiction Analysis
- `scripts/05_10_local_snhd_response_time_quantiles_by_jurisdiction.R`  
- `scripts/05_11_local_snhd_tail_component_breakdown.R`  

---

## Tools Used
- **R** – data pipeline and analysis  
- **Python** – supplemental diagnostics/utilities  
- **Git & GitHub** – version control and project presentation  

---

## Data Availability
This repository does **not include raw or processed datasets**.

Data sources (e.g., NEMSIS and local EMS data) are excluded due to:
- data size constraints  
- access restrictions  
- data governance considerations  

This repository is intended to demonstrate analytical methodology, pipeline design, and reproducibility.

---

## How to Run
1. Configure data sources (not included in this repository)
2. Run: scripts/00_run_pipeline_master.R
3. Outputs will be generated in the `/output` directory

---

## Portfolio Context
This repository represents a portfolio project demonstrating applied analytics, reproducible workflow design, and structured performance analysis using real-world EMS data concepts.

---

## Key Findings (Preliminary)

- Response time performance is heavily influenced by tail behavior (p95), rather than median values alone  
- Suburban areas exhibit elevated tail response times, particularly in scene response and transport components  
- Delay flag stratification helps distinguish structural delays from incidental variability  
- Transport time contributes disproportionately to extended response times in no-delay cases  
- Jurisdiction-level variation suggests localized operational differences in response performance  

---

## Pipeline Overview

The analysis is structured as a multi-stage pipeline that transforms raw EMS data into standardized datasets and produces analytical outputs for response time evaluation.

**Workflow:**

1. **Data Ingestion**
   - Load raw EMS datasets from source systems

2. **Data Cleaning & Standardization**
   - Clean raw fields and normalize formats  
   - Map variables into a canonical schema for consistency  

3. **Canonical Dataset Construction**
   - Build unified datasets combining multiple sources  
   - Apply quality checks and validation  

4. **Feature Engineering**
   - Generate delay flags and derived variables  
   - Prepare data for subgroup and tail analysis  

5. **Analysis Layer**
   - Compute response time distributions (median, p95)  
   - Decompose system response time into components  
   - Perform subgroup analysis by urbanicity and jurisdiction  

6. **Output Generation**
   - Produce summary tables and analytical outputs  
   - Support case study interpretation and reporting  
