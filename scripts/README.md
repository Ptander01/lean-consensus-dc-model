# Scripts

This folder contains all Python scripts for the DC GIS project.

## Ingestion Scripts
- `ingest_semianalysis.py` - Process Semianalysis year forecasts
- `ingest_datacentermap.py` - Import DataCenterMap data
- ... (add more as you upload)

## Processing Scripts  
- `Campus_Rollup_new.py` - Aggregate buildings to campus level
- `fix_regions.py` - Standardize region naming

AI Data Center Consensus GIS Project - Post-Schema Export Phase
🎯 Project Goal
Build an interactive ESRI Experience/ArcGIS dashboard to consolidate multiple external "data scout" sources about global AI/data center construction. Provide geographic search/parcel context, company filters, capacity rollups, and an editable layer for user intel. Produce one harmonized "gold" dataset queryable by source, company, status, market, and capacity/timeline.
Geodatabase Path: C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb
✅ CURRENT STATE - ALL 6 SOURCES SUCCESSFULLY INGESTED
Gold Feature Classes:
1. gold_buildings (599 records, 237 unique campus_ids)
Geometry: Point (WGS84, WKID: 4326)
Purpose: Granular building-level records
Record Level: ALL records marked as record_level = 'Building'
All 6 Sources Ingested:
✅ DataCenterHawk: 224 records (37.4%)
✅ Synergy: 152 records (25.4%)
✅ DataCenterMap: 134 records (22.4%)
✅ Semianalysis: 47 records (7.8%) - Meta/Oracle AI buildout forecasts
✅ NewProjectMedia: 33 records (5.5%)
✅ WoodMac: 9 records (1.5%)
2. gold_campus (237 records)
Geometry: Point (WGS84) - representative point from dissolved buildings
Purpose: Aggregated campus-level rollup
Record Level: ALL records marked as record_level = 'Campus'
Capacity: 31,780 MW total (mean: 448 MW per campus)
Regional Distribution:
AMER: 139 campuses (58.6%)
NorthAmerica: 37 campuses (15.6%) ← NEEDS CONSOLIDATION
EMEA: 33 campuses (13.9%)
APAC: 18 campuses (7.6%)
OTHER: 10 campuses (4.2%)
Company Distribution:
Meta: 163 campuses (68.8%)
Oracle: 70 campuses (29.5%)
Mortenson: 3 campuses (1.3%) ← NEEDS CORRECTION (contractor, not owner)
Citizens Energy Corporation: 1 campus (0.4%)
🗂️ Complete Schema Reference (50+ Fields)
Core Identity:
unique_id, source, source_unique_id, campus_id, record_level, ingest_date, date_reported
Company & Naming:
company_source, company_clean, campus_name, building_designation
Geography:
address, postal_code, city, market, state, state_abbr, county, country, region, latitude, longitude
Capacity (MW):
planned_power_mw, uc_power_mw, commissioned_power_mw, full_capacity_mw, planned_plus_uc_mw
Year-by-Year Forecasts: mw_2023 through mw_2032 (from Semianalysis)
Facility Details:
facility_sqft, whitespace_sqft, pue, available_power_kw
Status & Dates:
facility_status (Active, Under Construction, Permitting, Announced, etc.)
status_rank_tmp, actual_live_date, construction_started, announced, land_acquisition, cod
Cost & Land:
total_cost_usd_million, land_cost_usd_million, total_site_acres, data_center_acres
Campus-Only Fields:
building_count, first_live_date, facility_status_agg, pue_avg, facility_sqft_sum, whitespace_sqft_sum
Full schema documentation: See schema_exports/gold_buildings_schema.md and schema_exports/gold_campus_schema.md
🛠️ Key Scripts & Workflows
Working Scripts:
Campus_Rollup_new.py - Aggregates buildings → campuses
Location: C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\scripts\
Process: Dissolve → Feature To Point → Statistics → Insert to gold_campus
Includes: Cost/acreage fields + year MW fields (mw_2023-2032)
ingest_semianalysis.py - Semianalysis ingestion
FIXED: Changed full_capacity mapping from date field to total_planned_mw
Pivots time-series data (4,064 records → 47 buildings with year forecasts)
export_schema.py ✅ NEW - COMPLETED
Exports geodatabase schemas to version-controllable formats
Outputs: JSON, Markdown, CSV, XML for both feature classes
Location: C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\scripts\export_schema.py
Output folder: schema_exports/ (9 files generated)
Deduplication Strategy:
User Preference: Ingest ALL sources, handle deduplication LATER
Method: Automated prioritization ranking + dashboard filters
Current Status: Many intentional duplicates exist (same facilities from different sources)
✅ COMPLETED WORK
Phase 1: Data Ingestion (COMPLETE)
✅ Ingested all 6 sources into gold_buildings
✅ Created campus rollup workflow with cost/acreage + year MW fields
✅ Fixed Semianalysis capacity issue (Excel serial dates incorrectly mapped to MW field)
✅ Verified year-by-year forecast data preservation (2023-2032)
Phase 2: Initial QA (COMPLETE)
✅ Outlier detection revealed Excel date issue in Semianalysis
✅ Fixed capacity data: 874,197 MW → 31,780 MW (realistic)
✅ Verified all 6 sources present with correct record counts
✅ Confirmed year-by-year forecasts preserved (620 MW in 2023 → 9,745 MW in 2032)
Phase 3: GitHub Setup & Schema Export (COMPLETE) ✅ NEW
✅ Learned GitHub concepts (repo, commit, branch, push, pull, clone)
✅ Created .gitignore file for ArcGIS projects
✅ Built comprehensive schema export script (export_schema.py)
✅ Successfully exported both feature class schemas to 4 formats:
JSON (machine-readable)
Markdown (GitHub-friendly documentation)
CSV (Excel-compatible field lists)
XML (ArcGIS workspace documents)
✅ Generated schema_exports/ folder with 9 files:
README.md
gold_buildings_schema.json, .md, .csv, _workspace.xml
gold_campus_schema.json, .md, .csv, _workspace.xml
geodatabase_domains.json
✅ Fixed Python f-string syntax errors (triple backticks in triple-quoted strings)
✅ Verified script execution in ArcGIS Pro (ran successfully 2025-01-21 14:23:05)
