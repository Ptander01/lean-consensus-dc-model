# gold_buildings Schema

**Exported:** 2025-11-21 16:50:45  
**Geometry Type:** Point  
**Spatial Reference:** GCS_WGS_1984 (WKID: 4326)  
**Record Count:** 599

---

## Fields (73 total)

| Field Name | Type | Length | Precision | Scale | Nullable | Alias | Domain |
|------------|------|--------|-----------|-------|----------|-------|--------|
| `OBJECTID` | OID | 4 | - | - | No | - | - |
| `Shape` | Geometry | - | - | - | Yes | - | - |
| `unique_id` | String | 64 | - | - | Yes | Unique ID | - |
| `source` | String | 64 | - | - | Yes | Source | - |
| `source_unique_id` | String | 64 | - | - | Yes | Source Unique ID | - |
| `date_reported` | Date | 8 | - | - | Yes | Date Reported | - |
| `record_level` | String | 16 | - | - | Yes | Record Level | dm_record_level |
| `campus_id` | String | 128 | - | - | Yes | Campus ID | - |
| `campus_name` | String | 128 | - | - | Yes | Campus/Project Name | - |
| `company_source` | String | 128 | - | - | Yes | Company (Source) | - |
| `company_clean` | String | 128 | - | - | Yes | Company (Clean) | - |
| `building_designation` | String | 64 | - | - | Yes | Building Designation | - |
| `address` | String | 255 | - | - | Yes | Address | - |
| `postal_code` | String | 16 | - | - | Yes | Zip/Postal | - |
| `city` | String | 128 | - | - | Yes | City | - |
| `market` | String | 128 | - | - | Yes | Market | - |
| `state` | String | 64 | - | - | Yes | State | - |
| `state_abbr` | String | 8 | - | - | Yes | State Abbr | - |
| `county` | String | 128 | - | - | Yes | County | - |
| `country` | String | 64 | - | - | Yes | Country | - |
| `region` | String | 16 | - | - | Yes | Region | - |
| `latitude` | Double | 8 | - | - | Yes | Latitude | - |
| `longitude` | Double | 8 | - | - | Yes | Longitude | - |
| `planned_power_mw` | Double | 8 | - | - | Yes | Planned Power (MW) | - |
| `uc_power_mw` | Double | 8 | - | - | Yes | Under Construction Power (MW) | - |
| `commissioned_power_mw` | Double | 8 | - | - | Yes | Commissioned Power (MW) | - |
| `full_capacity_mw` | Double | 8 | - | - | Yes | Full Capacity (MW) | - |
| `planned_plus_uc_mw` | Double | 8 | - | - | Yes | Planned + UC (MW) | - |
| `mw_2023` | Double | 8 | - | - | Yes | 2023 | - |
| `mw_2024` | Double | 8 | - | - | Yes | 2024 | - |
| `mw_2025` | Double | 8 | - | - | Yes | 2025 | - |
| `mw_2026` | Double | 8 | - | - | Yes | 2026 | - |
| `mw_2027` | Double | 8 | - | - | Yes | 2027 | - |
| `mw_2028` | Double | 8 | - | - | Yes | 2028 | - |
| `mw_2029` | Double | 8 | - | - | Yes | 2029 | - |
| `mw_2030` | Double | 8 | - | - | Yes | 2030 | - |
| `mw_2031` | Double | 8 | - | - | Yes | 2031 | - |
| `mw_2032` | Double | 8 | - | - | Yes | 2032 | - |
| `pue` | Double | 8 | - | - | Yes | PUE | - |
| `actual_live_date` | Date | 8 | - | - | Yes | Actual Live Date | - |
| `construction_started` | Date | 8 | - | - | Yes | Construction Started | - |
| `announced` | Date | 8 | - | - | Yes | Announced | - |
| `land_acquisition` | Date | 8 | - | - | Yes | Land Acquisition | - |
| `permitting` | Date | 8 | - | - | Yes | Permitting | - |
| `cod` | Date | 8 | - | - | Yes | COD | - |
| `facility_status` | String | 32 | - | - | Yes | Facility Status | dm_facility_status |
| `cancelled` | SmallInteger | 2 | - | - | Yes | Cancelled | - |
| `facility_sqft` | Double | 8 | - | - | Yes | Facility Sq Ft | - |
| `whitespace_sqft` | Double | 8 | - | - | Yes | Whitespace Sq Ft | - |
| `available_power_kw` | Double | 8 | - | - | Yes | Available Power (kW) | - |
| `substation_count` | SmallInteger | 2 | - | - | Yes | Substation Count | - |
| `onsite_substation` | SmallInteger | 2 | - | - | Yes | Onsite Substation | - |
| `power_provider` | String | 128 | - | - | Yes | Power Provider | - |
| `power_grid` | String | 128 | - | - | Yes | Power Grid | - |
| `tier_design` | String | 32 | - | - | Yes | Tier Design | - |
| `type_category` | String | 32 | - | - | Yes | Type (Colo/Hyperscale) | dm_type_category |
| `owned_leased` | String | 32 | - | - | Yes | Owned/Leased | dm_owned_leased |
| `purpose` | String | 32 | - | - | Yes | Purpose Built or Retrofit | dm_purpose |
| `feed_config` | String | 16 | - | - | Yes | Feed Config | dm_feed_config |
| `ecosystem_ixps` | SmallInteger | 2 | - | - | Yes | Ecosystem IXPs | - |
| `ecosystem_cloud` | SmallInteger | 2 | - | - | Yes | Ecosystem Cloud | - |
| `ecosystem_children` | SmallInteger | 2 | - | - | Yes | Ecosystem Children | - |
| `ecosystem_networkproviders` | SmallInteger | 2 | - | - | Yes | Ecosystem Net Providers | - |
| `ecosystem_networkpresence` | SmallInteger | 2 | - | - | Yes | Ecosystem Net Presence | - |
| `ecosystem_serviceproviders` | SmallInteger | 2 | - | - | Yes | Ecosystem Service Providers | - |
| `additional_references` | String | 512 | - | - | Yes | References | - |
| `notes` | String | 1024 | - | - | Yes | Notes | - |
| `ingest_date` | Date | 8 | - | - | Yes | Ingest Date | - |
| `status_rank_tmp` | SmallInteger | 2 | - | - | Yes | - | - |
| `total_cost_usd_million` | Double | 8 | - | - | Yes | Total Project Cost (USD Millions) | - |
| `land_cost_usd_million` | Double | 8 | - | - | Yes | Land Acquisition Cost (USD Millions) | - |
| `total_site_acres` | Double | 8 | - | - | Yes | Total Site Acreage | - |
| `data_center_acres` | Double | 8 | - | - | Yes | Data Center Footprint Acreage | - |

---

## Spatial Reference Details

**Name:** GCS_WGS_1984  
**Type:** Geographic  
**WKID:** 4326

### Well-Known Text (WKT)

```
GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision
```

---

## Field Type Summary

- **Date:** 8 fields
- **Double:** 25 fields
- **Geometry:** 1 fields
- **OID:** 1 fields
- **SmallInteger:** 10 fields
- **String:** 28 fields

---

*Generated by export_schema.py on 2025-11-21 16:50:45*
