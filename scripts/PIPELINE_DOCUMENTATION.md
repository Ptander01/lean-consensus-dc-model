# Lean Consensus Data Center Model - Pipeline Documentation

## Overview

This document provides a comprehensive summary of the data ingestion, processing, analysis, accuracy assessment, and visualization pipeline for the Lean Consensus Data Center Model. The pipeline integrates multiple external data sources with Meta's internal canonical datacenter inventory to create a unified, validated view of global datacenter facilities.

**Last Updated:** December 9, 2025 (Consolidated redundant scripts)

---

## Table of Contents

1. [Ingestion Process](#1-ingestion-process)
2. [Processing Pipeline](#2-processing-pipeline)
3. [Analysis Workflow](#3-analysis-workflow)
4. [Accuracy Assessment](#4-accuracy-assessment)
5. [Visualization](#5-visualization)
6. [Data Schema](#6-data-schema)
7. [File Structure](#7-file-structure)
8. [Consolidated Scripts](#8-consolidated-scripts)

---

## 1. Ingestion Process

### Purpose
Ingest data from six external datacenter intelligence sources into a unified `gold_buildings` feature class with standardized schema.

### Data Sources

| Source | Script | Description | Key Data |
|--------|--------|-------------|----------|
| **DataCenterHawk (DCH)** | `ingest_dch.py` | Hyperscale datacenter database | Facility ID, capacity (kW→MW), status, commissioned year |
| **New Project Media (NPM)** | `ingest_npm.py` | Datacenter project announcements | Project name, total MW, building size, cost, planned operation date |
| **SemiAnalysis** | `ingest_semianalysis_CORRECTED.py` | Building-level capacity projections | Time-series capacity (2023-2032), cluster assignments, Excel date conversion |
| **Synergy** | `ingest_synergy.py` | Datacenter location database | Quarter-based opening dates, owned/leased status, region mapping |
| **WoodMac** | `ingest_woodmac.py` | Energy market datacenter intelligence | Milestone dates (announced→COD), cost, acreage, partner info |

### Common Ingestion Features

1. **Unique ID Generation**: `{source}_{source_unique_id}` format
2. **Campus ID Convention**: `{company_slug}|{city_slug}|{campus_name_slug}`
3. **Status Vocabulary Mapping**: Maps source-specific statuses to standard values:
   - Active, Under Construction, Permitting, Announced, Land Acquisition, Rumor, Unknown
4. **Status Rank Assignment**: Numeric ranking for aggregation (1=Active → 7=Unknown)
5. **Duplicate Prevention**: Checks for existing records before insertion
6. **Progress Logging**: Detailed console output with record counts and validation

### Key Transformations

```python
# Status mapping example (DCH)
STATUS_MAP = {
    'Owned': 'Active',
    'Under Construction': 'Under Construction',
    'Planned': 'Announced',
    None: 'Unknown'
}

# Capacity conversion (kW to MW)
commissioned_mw = cap_comm * 0.001

# Campus ID generation
campus_id = f"{slug(company)}|{slug(city)}|{slug(campus_name)}"
```

---

## 2. Processing Pipeline

### 2.1 Meta Canonical Import (`import_meta_canonical_v2.py`)

**Purpose**: Import Meta's internal datacenter inventory from DAI query export.

**Process**:
1. Load CSV from internal database export
2. Deduplicate by `location_key` with aggregation rules:
   - Latest milestone date
   - Maximum IT load (for upgrades)
   - First geographic data (coordinates, address)
3. Derive geographic region from coordinates:
   - **AMER**: Longitude -180 to -30
   - **EMEA**: Longitude -25 to 65
   - **APAC**: Longitude 65 to 180
4. Create `meta_canonical_v2` feature class
5. Export non-spatial records to separate table for geocoding

**Output Fields**:
- `location_key` (unique identifier)
- `dc_code` (datacenter code)
- `suite` (building designation)
- `it_load` (MW capacity)
- `new_build_status`, `activity_status`
- `region_derived` (AMER/EMEA/APAC)

### 2.2 Campus Rollup (`campus_rollup_new.py`)

**Purpose**: Aggregate building-level records into campus-level summaries.

**Process**:
1. Build source lookup dictionary by `campus_id`
2. Clear existing `gold_campus` table
3. Perform PairwiseDissolve by `campus_id` with statistics:
   - `SUM`: planned_power_mw, uc_power_mw, commissioned_power_mw, full_capacity_mw, facility_sqft, mw_2023-2032
   - `FIRST`: company_clean, campus_name, city, state, country, region
   - `MIN`: actual_live_date, status_rank_tmp
   - `MAX`: cancelled
   - `COUNT`: unique_id (→ building_count)
   - `MEAN`: pue
4. Feature to Point (INSIDE) for representative campus points
5. Map derived fields and insert into `gold_campus`

**Output Schema**:
- Campus identification (campus_id, campus_name, company_clean)
- Location (city, state, country, region)
- Aggregated capacity (planned_power_mw, commissioned_power_mw, full_capacity_mw)
- Time-series capacity (mw_2023 through mw_2032)
- Building count and aggregated status

---

## 3. Analysis Workflow

### 3.1 Unified Accuracy Analysis (`unified_accuracy_analysis.py`)

**Purpose**: Replicate Accenture's manual benchmarking methodology at scale.

**Three Main Tasks**:

#### Task 1: Enhance Spatial Match Data
- Add `distance_to_meta_dc_miles` (convert meters to miles)
- Add `meta_location_name` (from location_key)
- Classify records as Building or Campus level based on suite designation
- Add `campus_name_meta`

#### Task 2: Create Unified Global_DC_All_Sources Table
- 45+ standardized fields matching Accenture schema
- Import Meta canonical locations as "Meta Actuals"
- Import external source data from gold_buildings
- Link spatial match information with confidence scoring:
  - **High**: < 0.5 miles
  - **Medium**: 0.5 - 2.0 miles
  - **Low**: > 2.0 miles

#### Task 3: Capacity Validation Analysis
- Compare external MW estimates against Meta actual IT load
- Calculate percent difference for each source-location pair
- Apply Accenture variance scoring:
  - **Within 15%**: Variance Score 1
  - **15-30%**: Variance Score 2
  - **30-60%**: Variance Score 3
  - **>60%**: Variance Score 4
- Generate summary statistics by source

**Output Tables**:
- `Global_DC_All_Sources` (unified feature class)
- `capacity_validation_report` (MW comparison table)

---

## 4. Accuracy Assessment

### 4.1 Comprehensive Spatial Accuracy Report (`comprehensive_spatial_accuracy_report.py`) ⭐ PRIMARY

**Location**: `scripts/accuracy/comprehensive_spatial_accuracy_report.py`

This is the **primary accuracy analysis script** that performs both campus-level and building-level analysis in a single run.

**Features**:
- **Campus-Level Analysis**: Unique `dc_code` values with valid coordinates
- **Building-Level Analysis**: Composite key `{dc_code}-{datacenter}`
- **Deduplication**: Closest match per building/campus per source
- **Complete Statistical Output**: All metrics in one execution

**Metrics Calculated**:
- **Recall**: % of Meta sites detected by each source
- **Distance Statistics**: min, max, mean, std, MAD, percentiles (P10-P90)
- **Threshold Shares**: % within 100m, 500m, 1km, 5km
- **Match Quality Bands**:
  - Excellent: < 1km
  - Good: 1-3km
  - Fair: 3-5km

**Breakdowns**:
- By region (AMER, EMEA, APAC)
- By build status (Complete Build, Active Build, Future Build)
- Worst-case analysis (top 25 largest distances per source)
- Executive summary with rankings

### Output Files
- `comprehensive_accuracy_report_{timestamp}.txt` - Executive summary
- `campus_level_stats_{timestamp}.csv` - Campus-level results
- `building_level_stats_{timestamp}.csv` - Building-level results
- `by_region_{timestamp}.csv` - Regional breakdown
- `by_status_{timestamp}.csv` - Build status breakdown
- `worst_case_{timestamp}.csv` - Outlier analysis

---

## 5. Visualization

### Script: `plot_spatial_accuracy_LIGHT_THEME.py`

**Purpose**: Generate publication-quality visualizations for spatial accuracy analysis

**Data Preparation**:
1. Load accuracy matches and Meta canonical data
2. Deduplicate to closest match per building/campus per source
3. Calculate denominators (unique campuses and buildings)

### Generated Plots

| Plot | Description | Use Case |
|------|-------------|----------|
| **Plot 1** | Building-level box plot by source | Overall source comparison |
| **Plot 2** | Granularity comparison (campus vs building) | Side-by-side granularity analysis |
| **Plot 3** | By region | Regional performance comparison |
| **Plot 4** | By build status | Status-based accuracy |
| **Plot 5** | Building-level violin plot | Full distribution visualization |
| **Plot 6** | Campus-level violin plot | Campus-level distribution |

### Visual Specifications
- **Theme**: Light (white background)
- **Color Palette**:
  - DataCenterHawk: #5dade2 (Sky blue)
  - Semianalysis: #af7ac5 (Purple)
  - DataCenterMap: #f4d03f (Gold)
  - NewProjectMedia: #ec7063 (Coral)
  - WoodMac: #58d68d (Green)
  - Synergy: #cb4335 (Dark red)
- **Reference Lines**: 1km (green), 3km (orange)
- **Output Format**: PNG, 300 DPI

---

## 6. Data Schema

### gold_buildings (Building-Level)

| Field | Type | Description |
|-------|------|-------------|
| unique_id | TEXT | Source-prefixed unique identifier |
| source | TEXT | Data source name |
| source_unique_id | TEXT | Original source ID |
| campus_id | TEXT | Standardized campus identifier |
| campus_name | TEXT | Campus/project name |
| building_designation | TEXT | Building number/phase |
| company_source | TEXT | Company name from source |
| company_clean | TEXT | Standardized company name |
| city, state, country | TEXT | Location fields |
| region | TEXT | AMER/EMEA/APAC |
| latitude, longitude | DOUBLE | Coordinates |
| facility_status | TEXT | Current status |
| planned_power_mw | DOUBLE | Planned capacity |
| uc_power_mw | DOUBLE | Under construction capacity |
| commissioned_power_mw | DOUBLE | Operational capacity |
| full_capacity_mw | DOUBLE | Total buildout capacity |
| mw_2023 - mw_2032 | DOUBLE | Year-by-year projections |
| total_cost_usd_million | DOUBLE | Project cost |
| total_site_acres | DOUBLE | Site size |
| ingest_date | DATE | Import timestamp |

### gold_campus (Campus-Level)

Same as gold_buildings plus:
- `building_count` - Number of buildings
- `facility_sqft_sum` - Total facility area
- `first_live_date` - Earliest operational date
- `facility_status_agg` - Most advanced status

### meta_canonical_v2 (Meta Internal)

| Field | Type | Description |
|-------|------|-------------|
| location_key | TEXT | Unique location identifier |
| datacenter | TEXT | Building number |
| suite | TEXT | Suite designation |
| dc_code | TEXT | Datacenter code (campus) |
| region_derived | TEXT | Derived from coordinates |
| it_load | DOUBLE | IT load (MW) |
| new_build_status | TEXT | Build status |
| building_type | TEXT | Own/Lease |
| has_coordinates | SHORT | Valid coordinates flag |

---

## 7. File Structure

```
scripts/
├── ingestion/
│   ├── ingest_dch.py              # DataCenterHawk ingestion
│   ├── ingest_npm.py              # New Project Media ingestion
│   ├── ingest_semianalysis_CORRECTED.py  # SemiAnalysis ingestion
│   ├── ingest_synergy.py          # Synergy ingestion
│   └── ingest_woodmac.py          # WoodMac ingestion
│
├── processing/
│   ├── campus_rollup_new.py       # Building → Campus aggregation
│   └── import_meta_canonical_v2.py # Meta internal data import
│
├── analysis/
│   ├── unified_accuracy_analysis.py      # ⭐ Complete accuracy workflow (KEEP)
│   └── campus_level_deep_dive_export.py  # ⭐ Detailed campus comparisons (KEEP)
│
├── accuracy/
│   └── comprehensive_spatial_accuracy_report.py # ⭐ PRIMARY - Both levels (KEEP)
│
├── visualization/
│   └── plot_spatial_accuracy_LIGHT_THEME.py # Accuracy plots
│
├── qa/
│   ├── qa_validation.py           # Data validation
│   └── fix_companies.py           # Company name fixes
│
└── PIPELINE_DOCUMENTATION.md      # This file
```

---

## 8. Consolidated Scripts

### Scripts Removed (Redundant)

The following scripts were removed during consolidation as their functionality is covered by the primary scripts:

| Removed Script | Replaced By | Reason |
|----------------|-------------|--------|
| `accuracy/spatial_accuracy_campus_level.py` | `comprehensive_spatial_accuracy_report.py` | Campus analysis included in comprehensive report |
| `accuracy/spatial_accuracy_building_level_FIXED.py` | `comprehensive_spatial_accuracy_report.py` | Building analysis included in comprehensive report |
| `analysis/deep_dive_site_comparison_export.py` | `campus_level_deep_dive_export.py` | Near-identical functionality |
| `analysis/multi_source_spatial_accuracy.py` | `unified_accuracy_analysis.py` | Creates same output table |
| `analysis/capacity_validation_campus_level.py` | `unified_accuracy_analysis.py` Task 3 | Capacity validation included |

### Primary Scripts to Use

| Script | Purpose | Output |
|--------|---------|--------|
| **`comprehensive_spatial_accuracy_report.py`** | Spatial accuracy at both levels | Statistics CSVs + Executive summary |
| **`unified_accuracy_analysis.py`** | Unified table + capacity validation | `Global_DC_All_Sources`, `capacity_validation_report` |
| **`campus_level_deep_dive_export.py`** | Detailed per-campus analysis | TXT report + detailed CSVs |

---

## Execution Order

For a complete pipeline run, use this **streamlined workflow**:

### Step 1: Ingest External Sources (any order)
```
python ingestion/ingest_dch.py
python ingestion/ingest_npm.py
python ingestion/ingest_semianalysis_CORRECTED.py
python ingestion/ingest_synergy.py
python ingestion/ingest_woodmac.py
```

### Step 2: Import Meta Canonical
```
python processing/import_meta_canonical_v2.py
```

### Step 3: Process Campus Rollup
```
python processing/campus_rollup_new.py
```

### Step 4: Run Accuracy Analysis (choose based on need)

**For comprehensive statistics (RECOMMENDED):**
```
python accuracy/comprehensive_spatial_accuracy_report.py
```

**For unified table + capacity validation:**
```
python analysis/unified_accuracy_analysis.py
```

**For detailed per-campus deep dive:**
```
python analysis/campus_level_deep_dive_export.py
```

### Step 5: Generate Visualizations
```
python visualization/plot_spatial_accuracy_LIGHT_THEME.py
```

---

## Key Metrics Summary

| Metric | Description | Target |
|--------|-------------|--------|
| **Spatial Recall** | % of Meta sites detected by source | Higher is better |
| **Median Distance** | Median spatial offset from Meta actual | < 1km excellent |
| **Capacity Accuracy** | % within 15% of Meta IT load | Higher is better |
| **Match Confidence** | High (<0.5mi), Medium (0.5-2mi), Low (>2mi) | More High is better |

---

*Last Updated: December 9, 2025*
*Author: Meta Data Center GIS Team*
