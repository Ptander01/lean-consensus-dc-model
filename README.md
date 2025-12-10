# Lean Consensus Data Center Model - Pipeline Documentation

## Overview

This document provides a comprehensive summary of the data ingestion, processing, analysis, accuracy assessment, and visualization pipeline for the Lean Consensus Data Center Model. The pipeline integrates multiple external data sources with Meta's internal canonical datacenter inventory to create a unified, validated view of global datacenter facilities.

**Last Updated:** December 10, 2024
**Status:** ✅ LEAN MODEL COMPLETE — Ready to transition to FULL DATA

---

### LEAN MODEL SUMMARY

This "Lean" model focused on **Meta/Oracle facilities only** (~663 records) to:
- ✅ Prove the ingestion pipeline
- ✅ Validate spatial accuracy methodology
- ✅ Benchmark vendor data quality
- ✅ Develop capacity accuracy analysis
- ✅ Create reusable, production-ready scripts

| Deliverable | Status | Key Metric |
|-------------|--------|------------|
| **Spatial Accuracy Analysis** | ✅ Complete | DCH: 89.9% recall, 233m median |
| **Capacity Accuracy Analysis** | ✅ Complete | Semianalysis: 11.9% MAPE (Complete Builds) |
| **Ingestion Pipeline** | ✅ Production-ready | 6 scripts, duplicate prevention |
| **All Scripts Validated** | ✅ No errors | All scripts pass py_compile |

---

### Recent Updates (Dec 9-10, 2024)
- ✅ **Semianalysis ingestion fixed** - Building-level coordinates now properly populated
- ✅ **DataCenterMap ingestion created** - New script with duplicate prevention
- ✅ **Spatial join rebuilt** - `accuracy_analysis_multi_source_REBUILT` uses current gold_buildings
- ✅ **Semianalysis accuracy improved** - Median distance dropped from 841m → 307m (63% improvement!)
- ✅ **All visualization scripts updated** - Now use rebuilt spatial join data
- ✅ **Meta canonical integrity validated** - 10,968.61 MW total, suite→building aggregation verified
- ✅ **Capacity accuracy analysis v2 created** - Apples-to-apples comparison with field definitions
- ✅ **Variance analysis completed** - Identified root causes of capacity error (year alignment, outliers)

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

| Source | Script | Records | Description | Key Data |
|--------|--------|---------|-------------|----------|
| **DataCenterHawk (DCH)** | `ingest_dch.py` | 224 | Hyperscale datacenter database | Facility ID, capacity (kW→MW), status, commissioned year |
| **DataCenterMap (DCM)** | `ingest_dcm.py` | 67 | Datacenter location database | Stage-based status, power MW, building sqft, pue |
| **New Project Media (NPM)** | `ingest_npm.py` | 33 | Datacenter project announcements | Project name, total MW, building size, cost, planned operation date |
| **SemiAnalysis** | `ingest_semianalysis.py` | 178 | Building-level capacity projections | Time-series capacity (2023-2032), cluster assignments, Excel date conversion |
| **Synergy** | `ingest_synergy.py` | 152 | Datacenter location database | Quarter-based opening dates, owned/leased status, region mapping |
| **WoodMac** | `ingest_woodmac.py` | 9 | Energy market datacenter intelligence | Milestone dates (announced→COD), cost, acreage, partner info |

**Total gold_buildings:** 663 records (after deduplication)

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

### 2.2 Meta Deduplication (`meta_deduplicate.py`)

**Purpose**: Deduplicate Meta canonical data from suite-level to building-level for accurate spatial analysis.

**Problem Solved**: The raw Meta canonical data (`meta_canonical_v2`) contains records at the **suite level** (e.g., Suite A, B, C, D within Building 1). For accurate spatial accuracy analysis, we need to deduplicate to the **building level** using a composite key.

**Process**:
1. Add `building_key` field to `meta_canonical_v2`
2. Calculate composite key: `{dc_code}-{datacenter}` (e.g., "ALT-1", "ALT-2")
3. Filter to records with valid coordinates (`has_coordinates = 1`)
4. Dissolve suites to building-level using `building_key`
5. Aggregate fields:
   - `COUNT(location_key)` → `suite_count` (suites per building)
   - `FIRST(dc_code)` → campus identifier
   - `FIRST(datacenter)` → building number
   - `FIRST(region_derived)` → region (AMER/EMEA/APAC)
   - `FIRST(new_build_status)` → build status
   - `SUM(it_load)` → total IT load per building
6. Rename dissolved fields for clarity
7. Validate against expected counts (276 buildings)

**Input**: `meta_canonical_v2` (suite-level records)
**Output**: `meta_canonical_buildings` (building-level, 276 records)

**Expected Distribution**:
| Region | Buildings |
|--------|-----------|
| AMER | 251 |
| APAC | 5 |
| EMEA | 20 |

### 2.3 Campus Rollup (`campus_rollup_new.py`)

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

### 3.1 Multi-Source Spatial Accuracy (`multi_source_spatial_accuracy.py`) ⭐ SPATIAL JOIN

**Purpose**: Perform spatial join between Meta canonical buildings and external vendor data to create the base accuracy analysis dataset.

**Version**: v5 (FIXED) - Uses geodesic distance calculation

**Key Features**:
- **Input**: `meta_canonical_buildings` (276 deduplicated buildings)
- **Join Features**: `gold_buildings` (all external vendor records)
- **Method**: Spatial join with CLOSEST match within 50km search radius
- **Distance**: Haversine formula for accurate geodesic distance in meters
- **No Company Filter**: Matches by proximity only (catches misclassified records)

**Process**:
1. Spatial join Meta buildings to external sources (JOIN_ONE_TO_MANY)
2. Calculate geodesic distance using Haversine formula
3. Analyze recall and distance accuracy by source
4. Generate regional breakdown (AMER, EMEA, APAC)
5. Rank vendors by coverage and spatial accuracy

**Output**: `accuracy_analysis_buildings_v4`

**Metrics Generated**:
- Recall (% of 276 Meta buildings detected)
- Distance accuracy (median, mean, range)
- Threshold analysis (≤100m, ≤500m, ≤1km, ≤5km)
- Regional recall breakdown

### 3.2 Gold Buildings Audit (`gold_buildings_audit.py`) ⭐ QA

**Purpose**: Audit external vendor data quality before accuracy analysis.

**Location**: `scripts/qa/gold_buildings_audit.py`

**Audits Performed**:
- Record counts by source
- Field completeness analysis
- Capacity data availability
- Geographic distribution
- Company name standardization check

### 3.3 Unified Accuracy Analysis (`unified_accuracy_analysis.py`)

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

### 4.2 Capacity Accuracy Analysis ⭐ NEW (Dec 10, 2024)

**Location**: `scripts/qa/`

A suite of scripts for comparing vendor capacity predictions against Meta's actual IT load.

#### Scripts

| Script | Purpose |
|--------|---------|
| `validate_canonical_integrity.py` | Verify Meta suite→building aggregation |
| `capacity_accuracy_analysis_v2.py` | Apples-to-apples capacity comparison |
| `capacity_variance_experiments.py` | Root cause analysis of errors |
| `CAPACITY_FIELD_DEFINITIONS.md` | Field definitions documentation |

#### Key Findings

**Best Result: 11.9% MAPE** for Complete Builds using Semianalysis `mw_2023`

| Field | Complete Builds | All Statuses | Bias |
|-------|-----------------|--------------|------|
| **mw_2023** | **11.9%** 🏆 | 22.1% | -3.1% |
| mw_2024 | 14.7% | 23.5% | -2.3% |
| commissioned_power_mw | 15.7% | 25.1% | +4.0% |

#### Variance Analysis Results

| Source of Error | Impact | Resolution |
|-----------------|--------|------------|
| **Year alignment** | Major (3% improvement) | Use `mw_2023` instead of `mw_2024` |
| **4 outlier buildings** | Major (MAPE → 3.7% without) | FTW-4, VLL-4, ODN-3, LCO-1 |
| **Active builds** | Inherent (~60% MAPE) | Construction is volatile |
| **Utilization factor** | None needed | Bias already near 0% |
| **Spatial matching** | None needed | All matches <1km |

#### Capacity Field Definitions

See `CAPACITY_FIELD_DEFINITIONS.md` for detailed documentation of:
- What each vendor capacity field measures (IT load vs facility power)
- Granularity (building vs campus level)
- Time horizon (current vs forecast)
- PUE adjustment requirements

#### Apples-to-Apples Comparison Approach

**Building-Level (Semianalysis):**
- Vendor: `mw_2023` or `commissioned_power_mw`
- Meta: `meta_canonical_buildings.it_load_total`
- No adjustment needed (both report IT capacity)

**Campus-Level (DataCenterHawk):**
- Vendor: `commissioned_power_mw / 1.3` (PUE adjustment)
- Meta: Aggregated `it_load_total` by `dc_code`
- Result: 38.9% MAPE (DCH underestimates by 20%)

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
│   ├── ingest_dcm.py              # DataCenterMap ingestion (with duplicate prevention)
│   ├── ingest_npm.py              # New Project Media ingestion
│   ├── ingest_semianalysis.py     # SemiAnalysis ingestion (fixed coordinates & company)
│   ├── ingest_synergy.py          # Synergy ingestion
│   └── ingest_woodmac.py          # WoodMac ingestion
│
├── processing/
│   ├── import_meta_canonical_v2.py # Meta internal data import (suite-level)
│   ├── meta_deduplicate.py        # ⭐ Suite → Building deduplication
│   └── campus_rollup_new.py       # Building → Campus aggregation
│
├── analysis/
│   ├── multi_source_spatial_accuracy.py  # ⭐ Spatial join & accuracy (v5)
│   ├── unified_accuracy_analysis.py      # Complete accuracy workflow
│   └── campus_level_deep_dive_export.py  # Detailed campus comparisons
│
├── accuracy/
│   └── comprehensive_spatial_accuracy_report.py # PRIMARY - Both levels
│
├── visualization/
│   └── plot_spatial_accuracy_LIGHT_THEME.py # Accuracy plots
│
├── qa/
│   ├── gold_buildings_audit.py    # ⭐ External data quality audit (NEW)
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
python ingestion/ingest_dcm.py
python ingestion/ingest_npm.py
python ingestion/ingest_semianalysis.py
python ingestion/ingest_synergy.py
python ingestion/ingest_woodmac.py
```

**Note:** All ingestion scripts include **duplicate prevention** - safe to re-run without creating duplicates.

### Step 2: Import Meta Canonical
```
python processing/import_meta_canonical_v2.py
```

### Step 3: Deduplicate Meta to Building-Level ⭐ NEW
```
python processing/meta_deduplicate.py
```
This creates `meta_canonical_buildings` (276 buildings) from suite-level data.

### Step 4: Process Campus Rollup
```
python processing/campus_rollup_new.py
```

### Step 4a: Audit External Data Quality (OPTIONAL)
```
python qa/gold_buildings_audit.py
```
Generates data quality report for `gold_buildings` by source (record counts, field completeness, capacity data availability).

### Step 5: Perform Spatial Join & Initial Accuracy Analysis
```
python analysis/multi_source_spatial_accuracy.py
```
Creates `accuracy_analysis_buildings_v4` with spatial matches between Meta buildings and external sources.

### Step 6: Run Comprehensive Accuracy Analysis (choose based on need)

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

### Step 7: Generate Visualizations
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

## 9. Feature Classes Reference

### Geodatabase: `Default.gdb`

| Feature Class | Records | Purpose | Updated |
|---------------|---------|---------|---------|
| `meta_canonical_v2` | 1,068 | Suite-level Meta ground truth | Stable |
| `meta_canonical_buildings` | 276 | Building-level (dissolved from suites) | Stable |
| `gold_buildings` | 663 | Harmonized vendor buildings | Dec 9, 2024 |
| `gold_campus` | 229 | Campus-level rollup | Dec 9, 2024 |
| `accuracy_analysis_multi_source` | 13,210 | OLD spatial join (deprecated) | Dec 8, 2024 |
| `accuracy_analysis_multi_source_REBUILT` | 5,167 | **CURRENT spatial join** | Dec 10, 2024 |
| `accuracy_analysis_buildings_v4` | 276 | Single closest match per building | Dec 9, 2024 |
| `accuracy_analysis_buildings_v5` | 276 | Latest single match version | Dec 10, 2024 |

**⚠️ Important:** Use `accuracy_analysis_multi_source_REBUILT` for accuracy analysis - it contains the current building-level coordinates from `gold_buildings`.

---

## 10. Spatial Accuracy Results (Dec 10, 2024)

### Vendor Rankings

| Rank | Source | Recall | Median Distance | Consistency (MAD) |
|------|--------|--------|-----------------|-------------------|
| 🥇 | **DataCenterHawk** | 89.9% | 233m | 204m |
| 🥈 | **Semianalysis** | 88.8% | 307m | 285m |
| 🥉 | **NewProjectMedia** | 73.2% | 1,002m | 763m |
| 4 | **DataCenterMap** | 69.6% | 677m | 586m |
| 5 | **WoodMac** | 30.1% | 1,436m | 837m |
| 6 | **Synergy** | 58.0% | 5,772m | 2,045m |

### Semianalysis Improvement (Dec 8 → Dec 10)

| Metric | Before Fix | After Fix | Improvement |
|--------|------------|-----------|-------------|
| **Median Distance** | 841m | **307m** | **63% better** |
| **Recall** | 86.2% | 88.8% | +2.6% |
| **Consistency (MAD)** | 350m | 285m | -65m |
| **Ranking** | #4 | **#2** | ⬆️ 2 spots |

### Regional Coverage

| Region | Buildings | DCHawk | Semianalysis | DCMap | NPM | WoodMac | Synergy |
|--------|-----------|--------|--------------|-------|-----|---------|---------|
| **AMER** | 251 | 90.4% | 89.2% | 68.1% | 80.5% | 33.1% | 57.4% |
| **APAC** | 5 | 100% | 100% | 100% | 0% | 0% | 100% |
| **EMEA** | 20 | 80% | 80% | 80% | 0% | 0% | 55% |

**⚠️ NewProjectMedia & WoodMac are US-only** (0% recall in APAC/EMEA)

### Recommendations for Consensus Model

**Spatial Coordinate Priority:**
1. DataCenterHawk (233m median)
2. Semianalysis (307m median)
3. DataCenterMap (677m median)

**Capacity Data Priority:**
- Current: DataCenterHawk `commissioned_power_mw`
- Forecast: Semianalysis `mw_2032`

**Exclusions:**
- Synergy: EXCLUDE from spatial consensus (5,772m median - too inaccurate)
- Use Synergy for transparency reporting only

---

## 11. Changelog

### December 10, 2024
- ✅ Rebuilt `accuracy_analysis_multi_source_REBUILT` with current gold_buildings coordinates
- ✅ Updated `comprehensive_spatial_accuracy_report.py` to use REBUILT table
- ✅ Updated `plot_spatial_accuracy_LIGHT_THEME.py` to use REBUILT table
- ✅ Verified Semianalysis spatial accuracy improved (841m → 307m)
- ✅ Generated new accuracy reports and visualizations

### December 9, 2024
- ✅ Fixed Semianalysis ingestion - populated `latitude`, `longitude`, `gold_lat`, `gold_lon`, `company_clean`
- ✅ Renamed `ingest_semianalysis_CORRECTED.py` → `ingest_semianalysis.py`
- ✅ Created `ingest_dcm.py` with duplicate prevention
- ✅ Fixed data quality issues (178 Semianalysis records, 67 DCM duplicates removed)
- ✅ All required fields now 100% populated
- ✅ All regions properly mapped (NorthAmerica → AMER, etc.)

### December 8, 2024
- Initial spatial accuracy analysis
- Identified Semianalysis coordinate issues (841m median - suboptimal)

---

*Last Updated: December 10, 2024*
*Author: Meta Data Center GIS Team*
