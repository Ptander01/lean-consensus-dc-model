# lean-consensus-dc-model
AI/Data Center GIS consolidation project -               harmonizing 6 data sources for Meta/Oracle buildout tracking
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

- **[gold_buildings Schema](schema_exports/gold_buildings_schema.md)** - 599 building records, 50+ fields
- **[gold_campus Schema](schema_exports/gold_campus_schema.md)** - 237 campus rollups with aggregations
- **[Schema Exports README](schema_exports/README.md)** - Overview of all export formats

### Quick Reference
- **JSON:** Machine-readable schema for APIs and automation
- **Markdown:** Human-readable documentation (renders beautifully on GitHub)
- **CSV:** Excel-compatible field lists
- **XML:** ArcGIS-compatible workspace documents for schema import

## 🗂️ Repository Structure
├── scripts/ # Python data processing scripts
├── docs/ # Documentation and schema references
├── config/ # Configuration and field mappings
└── data/samples/ # Sample datasets for testing

## 🔧 Key Scripts

- **`Campus_Rollup_new.py`** - Aggregates building-level data to campus rollups
- **`ingest_semianalysis.py`** - Processes time-series forecast data (2023-2032)
- **`fix_regions.py`** - Standardizes region naming conventions

## 🗄️ Database

**Geodatabase Path:** `C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb`

**Feature Classes:**
- `gold_buildings` (599 records) - Building-level granular data
- `gold_campus` (237 records) - Aggregated campus-level rollups

## 📝 Documentation

See `/docs` folder for:
- Schema reference (50+ fields)
- Data source descriptions
- QA checklists
- Workflow guides

## 🚧 In Progress

- [ ] Region name standardization (AMER vs NorthAmerica)
- [ ] Company name corrections (Mortenson → Meta)
- [ ] Duplicate detection analysis
- [ ] ESRI Experience Builder dashboard development

---

**Last Updated:** [Today's date]
