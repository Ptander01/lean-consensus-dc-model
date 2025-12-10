# Scripts Folder Audit & Reorganization Plan

**Date:** December 10, 2024

---

## Current State: 8 Folders, 44 Python Files

### Current Folder Structure (Category-Based)
```
scripts/
├── accuracy/           (1 file)  - Spatial accuracy reports
├── analysis/           (4 files) - Various analysis scripts
├── consensus/          (3 files) - Deduplication logic
├── ingestion/          (8 files) - Data source ingestion
├── processing/         (5 files) - Data transformation
├── qa/                 (14 files)- Quality assurance
├── utils/              (3 files) - Helper functions
├── visualization/      (1 file)  - Plotting
├── __ad_hoc__/         (1 file)  - One-off analysis
└── (root)              (4 files) - Docs + misc
```

**Problems:**
1. Category names don't indicate workflow order
2. `qa/` is a catch-all with 14 files doing very different things
3. `accuracy/` vs `analysis/` distinction is unclear
4. Redundant scripts (v1 vs v2 versions)

---

## Script Redundancy Analysis

### 🔴 REDUNDANT - Safe to Delete (6 files)

| Script | Reason | Superseded By |
|--------|--------|---------------|
| `qa/capacity_accuracy_analysis.py` | v1 version | `capacity_accuracy_analysis_v2.py` |
| `qa/qa_validation.py` | Old validation | `validate_gold_buildings_data.py` |
| `qa/qa_validation_final.py` | Duplicate | `validate_gold_buildings_data.py` |
| `analysis/capacity_data_diagnostic.py` | Old diagnostic | Capacity analysis v2 |
| `ingestion/add_woodmac_campus.py` | One-time fix | Not needed |
| `ingestion/add_woodmac_schema.py` | One-time fix | Not needed |

### 🟡 REVIEW - Possibly Redundant (4 files)

| Script | Current Folder | Question |
|--------|----------------|----------|
| `analysis/multi_source_spatial_accuracy.py` | analysis/ | Superseded by comprehensive report? |
| `analysis/unified_accuracy_analysis.py` | analysis/ | Still used for Global_DC_All_Sources? |
| `analysis/campus_level_deep_dive_export.py` | analysis/ | One-time deep dive? |
| `processing/diagnose_new_canonical_v2.py` | processing/ | One-time diagnostic? |

### 🟢 KEEP - Production Scripts (28 files)

**Ingestion (6):**
- `ingest_dch.py`, `ingest_dcm.py`, `ingest_npm.py`
- `ingest_semianalysis.py`, `ingest_synergy.py`, `ingest_woodmac.py`

**Processing (3):**
- `import_meta_canonical_v2.py`, `meta_deduplicate.py`, `campus_rollup_new.py`

**Analysis (5):**
- `comprehensive_spatial_accuracy_report.py`
- `capacity_accuracy_analysis_v2.py`
- `capacity_variance_experiments.py`
- `validate_canonical_integrity.py`
- `attribute_accuracy_audit.py`

**QA/Validation (4):**
- `validate_gold_buildings_data.py`
- `gold_buildings_audit.py`
- `fix_companies.py`, `fix_regions.py`

**Consensus (3):**
- `consensus_dedupe.py`, `spatial_clustering.py`, `validate_clusters.py`

**Visualization (1):**
- `plot_spatial_accuracy_LIGHT_THEME.py`

**Utils (3):**
- `helper_scripts.py`, `import_meta_canonical.py`, `load_helpers.py`

**Docs (3):**
- `AI_CONTEXT_PROMPT.md`, `PIPELINE_DOCUMENTATION.md`, `CAPACITY_FIELD_DEFINITIONS.md`

---

## Proposed New Structure: Workflow-Oriented

### Naming Convention
Use **numbered prefixes** to indicate workflow order:

```
scripts/
│
├── 00_docs/                        # Documentation (read first)
│   ├── AI_CONTEXT_PROMPT.md
│   ├── PIPELINE_DOCUMENTATION.md
│   └── CAPACITY_FIELD_DEFINITIONS.md
│
├── 01_ingestion/                   # Step 1: Load external data
│   ├── ingest_dch.py
│   ├── ingest_dcm.py
│   ├── ingest_npm.py
│   ├── ingest_semianalysis.py
│   ├── ingest_synergy.py
│   └── ingest_woodmac.py
│
├── 02_processing/                  # Step 2: Transform & aggregate
│   ├── import_meta_canonical.py
│   ├── deduplicate_meta_buildings.py    # renamed from meta_deduplicate.py
│   └── rollup_campus.py                  # renamed from campus_rollup_new.py
│
├── 03_spatial_join/                # Step 3: Create spatial matches
│   └── create_spatial_join.py           # extracted from multi_source_spatial_accuracy.py
│
├── 04_validation/                  # Step 4: Validate data quality
│   ├── validate_gold_buildings.py       # renamed
│   ├── validate_meta_integrity.py       # renamed from validate_canonical_integrity.py
│   ├── audit_gold_buildings.py          # renamed from gold_buildings_audit.py
│   └── audit_field_completeness.py      # renamed from attribute_accuracy_audit.py
│
├── 05_accuracy_analysis/           # Step 5: Measure accuracy
│   ├── spatial_accuracy_report.py       # renamed from comprehensive_spatial_accuracy_report.py
│   ├── capacity_accuracy_report.py      # renamed from capacity_accuracy_analysis_v2.py
│   └── capacity_variance_analysis.py    # renamed from capacity_variance_experiments.py
│
├── 06_consensus/                   # Step 6: Build consensus model
│   ├── dedupe_consensus.py              # renamed from consensus_dedupe.py
│   ├── cluster_spatial.py               # renamed from spatial_clustering.py
│   └── validate_clusters.py
│
├── 07_visualization/               # Step 7: Create outputs
│   └── plot_accuracy.py                  # renamed from plot_spatial_accuracy_LIGHT_THEME.py
│
├── _archive/                       # Old/deprecated scripts
│   ├── capacity_accuracy_analysis_v1.py
│   ├── qa_validation.py
│   └── (other deprecated)
│
└── _utils/                         # Shared helpers
    ├── helpers.py
    └── config.py                   # Future: centralized paths/settings
```

---

## Benefits of Workflow-Oriented Structure

| Benefit | Description |
|---------|-------------|
| **Intuitive Order** | Numbers show exactly which scripts run first |
| **Self-Documenting** | Folder names describe the pipeline stage |
| **Easy Onboarding** | New team members understand flow immediately |
| **Clear Dependencies** | Step 3 depends on Steps 1-2 being complete |
| **Archive Pattern** | `_archive/` keeps old scripts without clutter |

---

## Migration Steps

### Phase 1: Delete Redundant (Quick Win)
1. Delete 6 redundant scripts
2. Move deprecated to `_archive/`

### Phase 2: Rename Folders
1. Create new numbered folders
2. Move scripts to new locations
3. Update imports if needed

### Phase 3: Rename Scripts (Optional)
1. Use clearer, action-oriented names
2. Remove version suffixes (v2, _new, _final)
3. Update documentation references

---

## Recommendation

**Start with Phase 1** (delete redundant) now, then decide if you want the full reorganization for the Full Data project. The numbered folder structure is a nice-to-have but requires updating documentation and any script imports.

Would you like me to:
1. **Delete the 6 redundant scripts** now?
2. **Implement the full reorganization** with numbered folders?
3. **Just delete redundant + create `_archive/`** as a compromise?
