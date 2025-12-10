# Capacity Field Definitions Matrix

## Purpose
This document defines what each capacity field measures across all data sources to ensure apples-to-apples comparisons in capacity accuracy analysis.

**Last Updated:** December 10, 2024

---

## Meta Ground Truth

| Field | Table | Unit | What It Measures | Time Horizon |
|-------|-------|------|------------------|--------------|
| `it_load` | meta_canonical_v2 | MW | **Actual IT server load** (current draw) | Current |
| `it_load_total` | meta_canonical_buildings | MW | Sum of suite-level IT loads per building | Current |

**Key Point:** Meta's `it_load` is **actual server load**, NOT facility power or design capacity. This is critical for comparisons.

---

## Vendor Field Definitions

### DataCenterHawk (DCH)

| Field | Source Field | Unit Conversion | What It Measures | Granularity | Time Horizon |
|-------|--------------|-----------------|------------------|-------------|--------------|
| `commissioned_power_mw` | `capacity_commissioned_power` | kW → MW (×0.001) | **Facility power capacity** (design) | Campus | Current |
| `planned_power_mw` | `capacity_planned_power` | kW → MW (×0.001) | Planned facility additions | Campus | Future |
| `uc_power_mw` | `capacity_under_construction_power` | kW → MW (×0.001) | Under construction capacity | Campus | Near-term |
| `full_capacity_mw` | Derived | Sum of above | Total buildout potential | Campus | Future |

**⚠️ DCH Comparison Note:**
- DCH reports **facility power capacity** (total building power draw including cooling, lighting, etc.)
- To compare with Meta IT load: `IT_Load ≈ Facility_Power / PUE` where PUE typically = 1.2-1.4
- DCH data is at **CAMPUS level**, not building level

---

### Semianalysis

| Field | Source Field | Unit Conversion | What It Measures | Granularity | Time Horizon |
|-------|--------------|-----------------|------------------|-------------|--------------|
| `commissioned_power_mw` | `installed_capacity_mw` | Already MW | **IT capacity** (current installed) | Building | Current |
| `planned_power_mw` | `total_planned_mw` | Already MW | Planned IT capacity | Building | Future |
| `uc_power_mw` | `total_under_construction_mw` | Already MW | Under construction IT capacity | Building | Near-term |
| `mw_2023` - `mw_2032` | `mw_YYYY` columns | Already MW | **IT capacity forecast by year** | Building | Annual forecast |

**✅ Semianalysis Comparison Note:**
- Semianalysis reports **IT capacity** (same definition as Meta)
- `mw_YYYY` fields are **directly comparable** to Meta `it_load_total`
- Data is at **BUILDING level** - matches Meta building granularity
- **Best source for capacity comparison**

---

### DataCenterMap (DCM)

| Field | Source Field | Unit Conversion | What It Measures | Granularity | Time Horizon |
|-------|--------------|-----------------|------------------|-------------|--------------|
| `commissioned_power_mw` | `power_mw` (if operational) | Already MW | Design power capacity | Both | Current |
| `planned_power_mw` | `power_mw` (if planned/land banked) | Already MW | Planned capacity | Both | Future |
| `uc_power_mw` | `power_mw` (if under construction) | Already MW | Under construction | Both | Near-term |

**⚠️ DCM Comparison Note:**
- Single `power_mw` field routed to different columns based on status
- Mix of campus and building level records (check `record_level` field)
- Unclear if this is facility power or IT capacity

---

### NewProjectMedia (NPM)

| Field | Source Field | Unit Conversion | What It Measures | Granularity | Time Horizon |
|-------|--------------|-----------------|------------------|-------------|--------------|
| `full_capacity_mw` | `total_mws` | Already MW | **Total project capacity** (design) | Building | Future (planned) |

**⚠️ NPM Comparison Note:**
- Only provides total project capacity, no current vs future split
- US-only coverage
- Likely represents design/nameplate capacity, not IT load

---

### WoodMac

| Field | Source Field | Unit Conversion | What It Measures | Granularity | Time Horizon |
|-------|--------------|-----------------|------------------|-------------|--------------|
| `commissioned_power_mw` | `existing_mw` | Already MW | Existing built capacity | Building | Current |
| `planned_power_mw` | `new_mw` | Already MW | New expansion capacity | Building | Future |
| `full_capacity_mw` | Derived | existing + new | Post-expansion total | Building | Future |

**⚠️ WoodMac Comparison Note:**
- Focus on tracking expansions
- US-only coverage (9 records)
- Small sample size limits usefulness

---

### Synergy

| Field | Source Field | Unit Conversion | What It Measures | Granularity | Time Horizon |
|-------|--------------|-----------------|------------------|-------------|--------------|
| N/A | `quantity` (NOT mapped) | N/A | **Deliberately excluded** | Building | N/A |

**❌ Synergy Note:**
- Synergy capacity field deliberately NOT ingested
- Focus is on facility attributes, not capacity
- Cannot be used for capacity comparison

---

## Apples-to-Apples Comparison Matrix

### Best Comparisons (Same Definition)

| Comparison | Vendor Field | Meta Field | Notes |
|------------|--------------|------------|-------|
| ✅ **BEST** | Semianalysis `mw_2024`/`mw_2025` | `it_load_total` | Both = IT capacity, building level |
| ✅ **GOOD** | Semianalysis `commissioned_power_mw` | `it_load_total` | Both = IT capacity, building level |

### Comparable with Adjustment

| Comparison | Vendor Field | Meta Field | Adjustment Needed |
|------------|--------------|------------|-------------------|
| ⚠️ | DCH `commissioned_power_mw` | `it_load_total` | Divide by PUE (~1.3), aggregate to campus |
| ⚠️ | DCM `commissioned_power_mw` | `it_load_total` | Filter to building level only |

### Not Comparable

| Comparison | Vendor Field | Meta Field | Reason |
|------------|--------------|------------|--------|
| ❌ | NPM `full_capacity_mw` | `it_load_total` | NPM = future design, Meta = current load |
| ❌ | Any `mw_2030`/`mw_2032` | `it_load_total` | Future forecast vs current state |
| ❌ | Synergy | Any | No capacity data |

---

## Recommended Comparison Approach

### 1. Building-Level Comparison (Semianalysis only)
```
Vendor: Semianalysis mw_2024 or commissioned_power_mw
Meta: meta_canonical_buildings.it_load_total
Method: 1:1 spatial match (closest within 5km)
Expected MAPE: <15% (same definition)
```

### 2. Campus-Level Comparison (DCH)
```
Vendor: gold_campus (DCH records only)
Meta: Aggregate it_load_total by dc_code (campus)
Adjustment: Vendor / 1.3 (approximate PUE adjustment)
Method: Match by campus_id or spatial proximity
Expected MAPE: 20-40% (facility vs IT load conversion)
```

### 3. Time-Horizon Validation
```
For CURRENT state: Use commissioned_power_mw or mw_2024
For FORECAST accuracy: Compare mw_2025+ against future builds only
DO NOT compare future forecasts to current IT load
```

---

## Key Terminology

| Term | Definition |
|------|------------|
| **IT Load** | Actual power consumed by IT equipment (servers, storage, network) |
| **Facility Power** | Total building power including IT load + cooling + lighting + infrastructure |
| **PUE** | Power Usage Effectiveness = Facility Power / IT Load (typically 1.2-1.5) |
| **Design Capacity** | Maximum rated capacity of facility (often higher than actual load) |
| **Commissioned** | Built and operational |
| **Utilization** | Actual load / Design capacity (typically 60-80%) |

---

## Summary

**For accurate capacity comparison:**
1. **USE** Semianalysis `mw_2024`/`commissioned_power_mw` for building-level comparison
2. **ADJUST** DCH data by PUE factor (~1.3) for facility-to-IT conversion
3. **AGGREGATE** Meta buildings to campus level when comparing to DCH
4. **FILTER** DCM to building-level records only
5. **EXCLUDE** Synergy, NPM, WoodMac from capacity accuracy (wrong definition or too few records)
6. **NEVER** compare future forecasts (mw_2030+) to current IT load
