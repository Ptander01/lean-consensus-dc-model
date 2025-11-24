# lean-consensus-dc-model

AI/Data Center GIS consolidation project - harmonizing 6 data sources for Meta/Oracle buildout tracking

## 🎯 Project Goal

Build an interactive ESRI Experience Builder dashboard to:

- Consolidate multiple data scout sources (DataCenterHawk, Synergy, Semianalysis, etc.)
- Provide geographic search and parcel context
- Enable filtering by company, status, market, capacity, and timeline
- Support user-contributed intelligence via editable layers

## 📊 Current Status

**Data Ingested:** 6 sources  
**Total Records:** 599 buildings, 237 campuses  
**Total Capacity:** 31,780 MW  
**Primary Companies:** Meta (68.8%), Oracle (29.5%)

## 📊 Schema Documentation

Complete schema documentation is available in the `schema_exports/` folder:

- **[gold_buildings Schema](scripts/Schema_Exports/gold_buildings_schema.md)** - 599 building records, 50+ fields
- **[gold_campus Schema](scripts/Schema_Exports/gold_campus_schema.md)** - 237 campus rollups with aggregations
- **[Schema Exports README](scripts/Schema_Exports/README.md)** - Overview of all export formats

### Quick Reference

- **JSON:** Machine-readable schema for APIs and automation
- **Markdown:** Human-readable documentation (renders beautifully on GitHub)
- **CSV:** Excel-compatible field lists
- **XML:** ArcGIS-compatible workspace documents for schema import

## 🗂️ Repository Structure
```
lean-consensus-dc-model/
├── scripts/ # Python data processing scripts
│ ├── Schema_Exports/ # Exported schemas (JSON, MD, CSV, XML)
│ ├── ingest_.py # Data ingestion scripts
│ ├── Campus_Rollup_new.py # Campus aggregation
│ ├── QA_.py # Quality assurance scripts
│ └── helper_Scripts.py # Utility functions
├── docs/ # Documentation (coming soon)
├── config/ # Configuration files (coming soon)
└── README.md # This file
```

## 🔧 Key Scripts

### **Data Ingestion**
- `ingest_dch.py` - DataCenterHawk import
- `Ingest_Synergy.py` - Synergy data import
- `Ingest_SemiAnalysis.py` - Time-series forecast data (2023-2032)
- `Ingest_NPM.py` - NewProjectMedia import
- `Ingest_Woodmac.py` - WoodMac data import

### **Processing**
- `Campus_Rollup_new.py` - Aggregates building-level data to campus rollups
- `ETL Script.py` - Extract, Transform, Load workflow

### **Quality Assurance**
- `QA_Region_Country.py` - Region/country validation
- `QA_Validation_Final.py` - Final data quality checks

### **Utilities**
- `helper_Scripts.py` - Common functions and utilities

## 🗄️ Database

**Geodatabase Path:** `C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb`

**Feature Classes:**
- `gold_buildings` (599 records) - Building-level granular data
- `gold_campus` (237 records) - Aggregated campus-level rollups

## 📝 Documentation

Full documentation includes:
- Schema reference (50+ fields) - See `scripts/Schema_Exports/`
- Data source descriptions (6 sources)
- QA checklists and validation procedures
- Workflow guides

## 🚧 In Progress

- [x ] Region name standardization (AMER vs NorthAmerica)
- [ x] Company name corrections (Mortenson → Meta)
- [ ] Duplicate detection analysis
- [ ] ESRI Experience Builder dashboard development

## 🎯 Data Sources

1. **DataCenterHawk** - 224 records (37.4%)
2. **Synergy** - 152 records (25.4%)
3. **DataCenterMap** - 134 records (22.4%)
4. **Semianalysis** - 47 records (7.8%) - Meta/Oracle forecasts
5. **NewProjectMedia** - 33 records (5.5%)
6. **WoodMac** - 9 records (1.5%)

## 📈 Key Metrics

- **Total Capacity:** 31,780 MW across 237 campuses
- **Geographic Coverage:** AMER (58.6%), EMEA (13.9%), APAC (7.6%)
- **Forecast Growth:** 15.7x capacity increase from 2023 to 2032
- **Project Costs:** $32.4B in documented investments

---

**Last Updated:** November 21, 2025  
**Status:** Active Development
