# 📋 AI Context Prompt — Data Center Consensus GIS Model (v13.0 - LEAN MODEL COMPLETE)

**Last Updated:** December 10, 2024, 8:55 PM EST
**Status:** ✅ LEAN MODEL COMPLETE — Ready to transition to FULL DATA

---

# Data Center Consensus GIS Model — Pipeline Development Context

## 🎯 Project Overview

Building a **production-ready data pipeline** for Meta's Infrastructure Planning team that:
1. Ingests 6 external vendor data sources tracking global data center construction
2. Harmonizes them into standardized "gold" feature classes
3. Compares to Meta's canonical facility database for accuracy benchmarking
4. Produces repeatable, documented workflows for ongoing vendor evaluation

**This is a data pipeline project, not a one-off analysis.** Scripts must be dependable and repeatable.

---

## 🏁 LEAN MODEL STATUS: COMPLETE

This "Lean" model focused on **Meta/Oracle facilities only** (~663 records) to:
- ✅ Prove the ingestion pipeline
- ✅ Validate spatial accuracy methodology
- ✅ Benchmark vendor data quality
- ✅ Develop capacity accuracy analysis
- ✅ Create reusable, production-ready scripts

### Key Deliverables Achieved

| Deliverable | Status | Key Metric |
|-------------|--------|------------|
| **Spatial Accuracy Analysis** | ✅ Complete | DCH: 89.9% recall, 233m median |
| **Capacity Accuracy Analysis** | ✅ Complete | Semianalysis: 11.9% MAPE (Complete Builds) |
| **Ingestion Pipeline** | ✅ Production-ready | 6 scripts, duplicate prevention |
| **Documentation** | ✅ Complete | AI Context + Pipeline docs |

### Next Phase: FULL DATA (~25k records, all hyperscalers)

To transition to full data for the ESRI XB product:
1. Load full data extracts (all companies, not just Meta/Oracle)
2. Update `SOURCE_FC` paths in ingestion scripts
3. Re-run full ingestion pipeline
4. Develop multi-company consensus/deduplication logic
5. Build XB-ready feature classes with rich attributes

---

## 📂 Geodatabase & Feature Classes

**Location:** `C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb`

### Core Feature Classes
| Feature Class | Records | Status | Purpose |
|---------------|---------|--------|---------|
| `meta_canonical_v2` | 1,218 | ✅ Stable | Suite-level ground truth (has `it_load`) |
| `meta_canonical_buildings` | 276 | ✅ Stable | Building-level (has `it_load_total`) |
| `gold_buildings` | 663 | ✅ **CLEAN** | Harmonized vendor buildings |
| `gold_campus` | 229 | ✅ Complete | Campus-level rollup |
| `accuracy_analysis_multi_source_REBUILT` | 5,167 | ✅ **CURRENT** | Spatial join for accuracy analysis |

**Vendor Breakdown (gold_buildings):**
- DataCenterHawk: 224
- Semianalysis: 178
- Synergy: 152
- DataCenterMap: 67
- NewProjectMedia: 33
- WoodMac: 9

---

## 🎉 Major Achievements (Dec 9-10, 2024)

### 1. Semianalysis Spatial Accuracy Fixed
| Metric | Before Fix | After Fix | Improvement |
|--------|------------|-----------|-------------|
| **Median Distance** | 841m | **307m** | **63% better!** |
| **Recall** | 86.2% | 88.8% | +2.6% |
| **Ranking** | #4 | **#2** | ⬆️ 2 spots |

### 2. All Ingestion Scripts Updated
- ✅ `ingest_semianalysis.py` - Fixed coordinates, company_clean, duplicate prevention
- ✅ `ingest_dcm.py` - NEW script with duplicate prevention
- ✅ All scripts now safe to re-run without creating duplicates

### 3. Spatial Analysis Rebuilt
- ✅ `accuracy_analysis_multi_source_REBUILT` - Fresh spatial join with current coordinates
- ✅ `comprehensive_spatial_accuracy_report.py` - Updated to use REBUILT table
- ✅ `plot_spatial_accuracy_LIGHT_THEME.py` - Updated to use REBUILT table

### 4. Capacity Accuracy Analysis Complete ✅ NEW
- ✅ `validate_canonical_integrity.py` - Meta data verified (10,968.61 MW total)
- ✅ `capacity_accuracy_analysis_v2.py` - Apples-to-apples comparison created
- ✅ `capacity_variance_experiments.py` - Root cause analysis complete
- ✅ `CAPACITY_FIELD_DEFINITIONS.md` - Field definitions documented

---

## 📊 Capacity Accuracy Results (Dec 10, 2024) ✅ NEW

### Key Finding: **11.9% MAPE** for Complete Builds (Semianalysis mw_2023)

### Building-Level (Semianalysis vs Meta IT Load)

| Field | Complete Builds | All Statuses | Bias |
|-------|-----------------|--------------|------|
| **mw_2023** | **11.9%** 🏆 | 22.1% | -3.1% |
| mw_2024 | 14.7% | 23.5% | -2.3% |
| commissioned_power_mw | 15.7% | 25.1% | +4.0% |

### Variance Analysis Findings

| Source of Error | Impact | Resolution |
|-----------------|--------|------------|
| **Year alignment** | Major | Use `mw_2023` (matches Meta data timing) |
| **4 outlier buildings** | Major | MAPE drops to 3.7% without them |
| **Active builds** | Inherent | ~60% MAPE expected (construction volatile) |
| **Utilization factor** | None | Bias already near 0% |
| **Spatial matching** | None | All matches <1km |

### Outlier Buildings (Need Investigation)
- **FTW-4, VLL-4**: Over-estimates (+500%) - likely wrong match
- **ODN-3, LCO-1**: Under-estimates (-65%) - likely expansion phases

### Campus-Level (DCH with PUE Adjustment)
| Source | MAPE | Bias | Note |
|--------|------|------|------|
| DataCenterHawk / 1.3 | 38.9% | -20.0% | DCH underestimates |

### Capacity Accuracy Recommendations
1. **Use Semianalysis `mw_2023`** for Complete Build capacity (~12% MAPE)
2. **Trust Complete Builds** - Active builds inherently volatile
3. **Don't compare future forecasts** (mw_2030+) to current IT load
4. **Investigate 4 outliers** for manual data reconciliation

---

## 📊 Current Spatial Accuracy Results (Dec 10, 2024)

### Vendor Rankings
| Rank | Source | Recall | Median Distance | MAD |
|------|--------|--------|-----------------|-----|
| 🥇 | **DataCenterHawk** | 89.9% | 233m | 204m |
| 🥈 | **Semianalysis** | 88.8% | 307m | 285m |
| 🥉 | **NewProjectMedia** | 73.2% | 1,002m | 763m |
| 4 | **DataCenterMap** | 69.6% | 677m | 586m |
| 5 | **WoodMac** | 30.1% | 1,436m | 837m |
| 6 | **Synergy** | 58.0% | 5,772m | 2,045m |

### Regional Coverage
| Region | Buildings | DCHawk | Semianalysis | DCMap | NPM | WoodMac | Synergy |
|--------|-----------|--------|--------------|-------|-----|---------|---------|
| **AMER** | 251 | 90.4% | 89.2% | 68.1% | 80.5% | 33.1% | 57.4% |
| **APAC** | 5 | 100% | 100% | 100% | 0% | 0% | 100% |
| **EMEA** | 20 | 80% | 80% | 80% | 0% | 0% | 55% |

**⚠️ NewProjectMedia & WoodMac are US-ONLY** (0% recall in APAC/EMEA)

### By Build Status
| Status | DCHawk | Semianalysis | Best Median |
|--------|--------|--------------|-------------|
| **Complete Build** | 100% recall | 97.3% recall | 35-36m |
| **Active Build** | 78.2% recall | 78.2% recall | 485-645m |
| **Future Build** | 100% recall | 100% recall | 934m |

---

## 📜 Production Scripts - Workflow-Oriented Structure

### Folder Structure
```
scripts/
├── 00_docs/                    # Documentation (read first)
├── 01_ingestion/               # Step 1: Load external data
├── 02_processing/              # Step 2: Transform & aggregate
├── 03_spatial_join/            # Step 3: Create spatial matches
├── 04_validation/              # Step 4: Validate data quality
├── 05_accuracy_analysis/       # Step 5: Measure accuracy
├── 06_consensus/               # Step 6: Build consensus model
├── 07_visualization/           # Step 7: Create outputs
├── _archive/                   # Deprecated scripts
└── _utils/                     # Shared helpers
```

### Production Scripts by Workflow Step

**00_docs/ (Documentation)**
| File | Purpose |
|------|---------|
| `AI_CONTEXT_PROMPT.md` | Context for AI continuation |
| `PIPELINE_DOCUMENTATION.md` | Full pipeline reference |
| `CAPACITY_FIELD_DEFINITIONS.md` | Field definitions |

**01_ingestion/ (Step 1: Load Data)**
| Script | Purpose |
|--------|---------|
| `ingest_dch.py` | DataCenterHawk ingestion |
| `ingest_dcm.py` | DataCenterMap ingestion |
| `ingest_npm.py` | NewProjectMedia ingestion |
| `ingest_semianalysis.py` | Semianalysis ingestion |
| `ingest_synergy.py` | Synergy ingestion |
| `ingest_woodmac.py` | WoodMac ingestion |

**02_processing/ (Step 2: Transform)**
| Script | Purpose |
|--------|---------|
| `import_meta_canonical_v2.py` | Import Meta ground truth |
| `meta_deduplicate.py` | Suite → Building deduplication |
| `campus_rollup_new.py` | Building → Campus aggregation |

**03_spatial_join/ (Step 3: Match)**
| Script | Purpose |
|--------|---------|
| `multi_source_spatial_accuracy.py` | Create spatial join between Meta & vendors |

**04_validation/ (Step 4: QA)**
| Script | Purpose |
|--------|---------|
| `validate_gold_buildings_data.py` | Validate gold_buildings data |
| `validate_canonical_integrity.py` | Verify Meta suite→building aggregation |
| `gold_buildings_audit.py` | Audit external data quality |
| `attribute_accuracy_audit.py` | Field completeness analysis |
| `fix_companies.py` | Fix company name issues |
| `fix_regions.py` | Fix region mapping issues |

**05_accuracy_analysis/ (Step 5: Measure)**
| Script | Purpose |
|--------|---------|
| `comprehensive_spatial_accuracy_report.py` | Full spatial accuracy analysis |
| `capacity_accuracy_analysis_v2.py` | Capacity comparison (apples-to-apples) |
| `capacity_variance_experiments.py` | Root cause analysis |
| `unified_accuracy_analysis.py` | Unified analysis output |

**06_consensus/ (Step 6: Dedupe)**
| Script | Purpose |
|--------|---------|
| `consensus_dedupe.py` | Create consensus layers |
| `spatial_clustering.py` | Cluster nearby points |
| `validate_clusters.py` | Validate cluster output |

**07_visualization/ (Step 7: Output)**
| Script | Purpose |
|--------|---------|
| `plot_spatial_accuracy_LIGHT_THEME.py` | Generate accuracy plots |

---

## 🔴 OUTSTANDING TASKS (Continue Here!)

### ✅ COMPLETED Task 1: Validate Meta Canonical Data Integrity
**Status: COMPLETE**
- Meta canonical data verified: 10,968.61 MW total
- Suite→Building aggregation: ✅ PASS (all 318 buildings match)
- 3 buildings have mixed suite statuses (expected)

### ✅ COMPLETED Task 2: Run Capacity Accuracy Analysis
**Status: COMPLETE**
- **Best result: 11.9% MAPE** for Complete Builds using Semianalysis `mw_2023`
- Variance analysis identified 4 outlier buildings
- Without outliers: 3.7% MAPE (excellent)

### Task 3: Run Consensus Dedupe Script
**Priority: MEDIUM - For dashboarding**

Create deduplicated consensus layers:

```bash
python scripts/consensus/consensus_dedupe.py
```

**Creates:**
- `consensus_buildings` - One record per building (best source selected)
- `consensus_campus` - One record per campus (aggregated)

### Task 4: Investigate Capacity Outliers (Optional)
**Priority: LOW - Manual data reconciliation**

Four buildings have >50% capacity error:
- **FTW-4, VLL-4**: Over-estimates (+500%) - likely wrong spatial match
- **ODN-3, LCO-1**: Under-estimates (-65%) - likely expansion phases not captured

---

## 📁 Key Output Files

### Reports (in `/outputs/accuracy/`)
- `HOLISTIC_SPATIAL_ACCURACY_REPORT_20251210.txt` - Executive summary
- `comprehensive_report_20251210_*.txt` - Detailed analysis
- `building_level_stats_*.csv` - Building statistics
- `stats_by_region_*.csv` - Regional breakdown
- `stats_by_buildstatus_*.csv` - Status breakdown

### Attribute Audit (in `/outputs/attribute_audit/`)
- `attribute_audit_report_*.txt` - Field completeness report
- `field_completeness_by_source_*.csv` - Detailed by source
- `best_source_by_field_*.csv` - Recommended sources

### Visualizations (in `/outputs/accuracy/plots/`)
- `building_level_by_source_*.png`
- `granularity_comparison_*.png`
- `by_region_*.png`
- `by_buildstatus_*.png`

---

## 🎯 Recommendations for Consensus Model

### Spatial Coordinate Priority
1. **DataCenterHawk** (233m median) - Best accuracy
2. **Semianalysis** (307m median) - Second best
3. **DataCenterMap** (677m median) - Fallback

### Capacity Data Priority
- **Current Capacity:** DataCenterHawk `commissioned_power_mw`
- **Forecast Capacity:** Semianalysis `mw_2032`

### Exclusions
- **Synergy:** EXCLUDE from spatial consensus (5.7km median)
- Use Synergy for transparency reporting only

---

## 📋 Complete Schema Documentation (74 Fields in gold_buildings)

### Required Fields (11):
- unique_id, source, campus_id, campus_name
- latitude, longitude
- company_clean
- city, country, region
- ingest_date

### Valid Values:
- `source`: DataCenterHawk, Semianalysis, DataCenterMap, Synergy, NewProjectMedia, WoodMac
- `region`: AMER, EMEA, APAC

### Key Capacity Fields:
- `commissioned_power_mw` - Current operational
- `uc_power_mw` - Under construction
- `planned_power_mw` - Planned
- `full_capacity_mw` - Total
- `mw_2023` through `mw_2032` - Semianalysis forecasts

---

## 🔧 Execution Order (Full Pipeline)

### Step 1: Ingestion
```bash
python 01_ingestion/ingest_dch.py
python 01_ingestion/ingest_dcm.py
python 01_ingestion/ingest_npm.py
python 01_ingestion/ingest_semianalysis.py
python 01_ingestion/ingest_synergy.py
python 01_ingestion/ingest_woodmac.py
```

### Step 2: Processing
```bash
python 02_processing/import_meta_canonical_v2.py
python 02_processing/meta_deduplicate.py
python 02_processing/campus_rollup_new.py
```

### Step 3: Spatial Join
```bash
python 03_spatial_join/multi_source_spatial_accuracy.py
```

### Step 4: Validation
```bash
python 04_validation/validate_gold_buildings_data.py
python 04_validation/validate_canonical_integrity.py
```

### Step 5: Accuracy Analysis
```bash
python 05_accuracy_analysis/comprehensive_spatial_accuracy_report.py
python 05_accuracy_analysis/capacity_accuracy_analysis_v2.py
```

### Step 6: Consensus (when ready)
```bash
python 06_consensus/consensus_dedupe.py
```

### Step 7: Visualization
```bash
python 07_visualization/plot_spatial_accuracy_LIGHT_THEME.py
```

---

## 💡 Context for AI Assistant

**Current state:**
- **LEAN MODEL COMPLETE** - Ready to transition to full data
- Dataset is CLEAN and PRODUCTION-READY
- Spatial accuracy analysis complete with excellent results
- Capacity accuracy analysis complete (11.9% MAPE for Complete Builds)
- Semianalysis now #2 ranked source (was #4)

**Completed work:**
- ✅ All 6 ingestion scripts production-ready with duplicate prevention
- ✅ Spatial accuracy benchmarked (DCH: 89.9% recall, 233m median)
- ✅ Capacity accuracy benchmarked (Semianalysis: 11.9% MAPE)
- ✅ Meta canonical data integrity verified (10,968.61 MW)

**Next phase (FULL DATA):**
1. Load full data extracts (~25k records, all hyperscalers)
2. Update SOURCE_FC paths in ingestion scripts
3. Re-run full ingestion pipeline
4. Develop multi-company consensus/deduplication logic
5. Create company name standardization mapping
6. Build XB-ready feature classes with rich attributes

**Key feature classes (Lean Model):**
- `gold_buildings` - Harmonized vendor data (663 records)
- `meta_canonical_buildings` - Meta ground truth (276 buildings, has `it_load_total`)
- `accuracy_analysis_multi_source_REBUILT` - Use this for accuracy analysis

**What works well:**
- DataCenterHawk: Best overall (89.9% recall, 233m median)
- Semianalysis: Excellent after fix (88.8% recall, 307m median)
- Semianalysis capacity: 11.9% MAPE for Complete Builds

**Known limitations:**
- NewProjectMedia & WoodMac: US-only (0% APAC/EMEA)
- Synergy: Poor spatial accuracy (5.7km median) - exclude from spatial consensus

---

## 📊 Attribute Field Completeness Summary

**Best overall completeness:**
1. DataCenterHawk: ~25% (strongest on coordinates, status, timeline)
2. Semianalysis: ~20% (unique forecast capacity data mw_2023-2032)
3. WoodMac: ~18% (unique cost and land data)

**Best source by field category:**
- Capacity Current: DataCenterHawk
- Capacity Forecast: Semianalysis (ONLY source)
- Cost Data: WoodMac, NewProjectMedia
- Timeline: DataCenterHawk, WoodMac
- Facility Details: DataCenterHawk, DataCenterMap

---

**Copy this entire prompt into a new chat for seamless continuation!** 🚀

---

*Last Updated: December 10, 2024 8:55 PM EST*
