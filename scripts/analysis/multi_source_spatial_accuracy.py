import arcpy
import pandas as pd
from datetime import datetime
import math

# ============================================================================
# MULTI-SOURCE SPATIAL ACCURACY ANALYSIS (v3 - Fixed Field Detection)
# ============================================================================

# Configuration
gdb_path = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
meta_canonical = "meta_canonical_v2"
gold_buildings = "gold_buildings"
output_matches = "accuracy_analysis_multi_source"
output_summary = "accuracy_summary_by_source"

print("="*80)
print("MULTI-SOURCE SPATIAL ACCURACY ANALYSIS (v3)")
print("="*80)
print(f"\nCanonical baseline: {meta_canonical}")
print(f"External data: {gold_buildings}")
print(f"\n" + "="*80)

# Clear workspace cache
arcpy.ClearWorkspaceCache_management()

# ============================================================================
# STEP 1: SPATIAL JOIN - META CANONICAL TO EXTERNAL DATA
# ============================================================================
print("\n[STEP 1] Performing spatial join (one-to-many)...")

# Create temporary feature class for spatial join results
temp_join = "temp_spatial_join"
if arcpy.Exists(temp_join):
    arcpy.Delete_management(temp_join)

# Spatial join: Find ALL external records within 5km of each Meta site
arcpy.analysis.SpatialJoin(
    target_features=meta_canonical,
    join_features=gold_buildings,
    out_feature_class=temp_join,
    join_operation="JOIN_ONE_TO_MANY",
    join_type="KEEP_ALL",
    match_option="WITHIN_A_DISTANCE",
    search_radius="5 Kilometers"
)

print(f"  Spatial join complete: {temp_join}")

# Count results
join_count = int(arcpy.GetCount_management(temp_join)[0])
print(f"  Total matches found: {join_count:,}")

# ============================================================================
# STEP 1.5: INSPECT FIELD NAMES
# ============================================================================
print("\n[STEP 1.5] Inspecting spatial join field names...")

all_fields = [f.name for f in arcpy.ListFields(temp_join)]
print(f"  Total fields: {len(all_fields)}")

# List all fields to see what we have
print(f"\n  All field names (first 30):")
for i, field in enumerate(all_fields[:30]):
    print(f"    {i+1}. {field}")

# Smart field detection - try multiple patterns
lat_field_external = None
lon_field_external = None

# Try different patterns in order of preference
patterns = [
    ('latitude_1', 'longitude_1'),  # Most common suffix
    ('latitude', 'longitude'),       # No suffix
    ('lat_1', 'lon_1'),              # Abbreviated with suffix
    ('lat', 'lon')                   # Abbreviated no suffix
]

for lat_pattern, lon_pattern in patterns:
    if lat_pattern in all_fields and lon_pattern in all_fields:
        lat_field_external = lat_pattern
        lon_field_external = lon_pattern
        break

# If still not found, search for any field containing these strings
if not lat_field_external:
    for field in all_fields:
        if 'latitude' in field.lower() and field not in ['SHAPE@Y', 'latitude']:
            lat_field_external = field
            break
    if not lat_field_external:
        lat_field_external = 'latitude'  # Default fallback

if not lon_field_external:
    for field in all_fields:
        if 'longitude' in field.lower() and field not in ['SHAPE@X', 'longitude']:
            lon_field_external = field
            break
    if not lon_field_external:
        lon_field_external = 'longitude'  # Default fallback

print(f"\n  Detected coordinate field names:")
print(f"    Meta canonical: SHAPE@X (lon), SHAPE@Y (lat)")
print(f"    External data: {lon_field_external} (lon), {lat_field_external} (lat)")

# Verify fields exist
if lat_field_external not in all_fields or lon_field_external not in all_fields:
    print(f"\n  ERROR: Could not find latitude/longitude fields!")
    print(f"  Searched for: {lat_field_external}, {lon_field_external}")
    print(f"  Available fields containing 'lat' or 'lon':")
    for field in all_fields:
        if 'lat' in field.lower() or 'lon' in field.lower():
            print(f"    - {field}")
    raise Exception("Cannot proceed without coordinate fields")

# ============================================================================
# STEP 2: CALCULATE GEODESIC DISTANCES
# ============================================================================
print("\n[STEP 2] Calculating geodesic distances...")

# Add distance field
if 'distance_m' not in all_fields:
    arcpy.AddField_management(temp_join, "distance_m", "DOUBLE", field_alias="Distance (meters)")

# Calculate geodesic distance
def calculate_geodesic_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two lat/lon points in meters."""
    if any(v is None for v in [lat1, lon1, lat2, lon2]):
        return None

    # Convert to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371000  # Radius of Earth in meters

    return c * r

# Update distances
update_count = 0
null_count = 0

# Build cursor fields list - all must be valid strings
cursor_fields = ['SHAPE@X', 'SHAPE@Y', lat_field_external, lon_field_external, 'distance_m']

print(f"  Using cursor fields: {cursor_fields}")

with arcpy.da.UpdateCursor(temp_join, cursor_fields) as cursor:
    for row in cursor:
        meta_lon, meta_lat = row[0], row[1]  # Meta canonical coordinates from SHAPE
        ext_lat, ext_lon = row[2], row[3]    # External data coordinates from fields

        if ext_lat is not None and ext_lon is not None:
            distance = calculate_geodesic_distance(meta_lat, meta_lon, ext_lat, ext_lon)
            row[4] = distance
            cursor.updateRow(row)
            update_count += 1
        else:
            null_count += 1

print(f"  Calculated distances for {update_count:,} matches")
print(f"  Skipped {null_count:,} records with null external coordinates")

# ============================================================================
# STEP 3: FILTER TO COMPANY = META
# ============================================================================
print("\n[STEP 3] Filtering to external records claiming 'Meta' company...")

# Find the company field name - try different patterns
company_field = None
if 'company_clean_1' in all_fields:
    company_field = 'company_clean_1'
elif 'company_clean' in all_fields:
    company_field = 'company_clean'
else:
    # Search for any field with 'company' in name
    for field in all_fields:
        if 'company' in field.lower():
            company_field = field
            break

if not company_field:
    print("  ERROR: Could not find company field!")
    raise Exception("Cannot proceed without company field")

print(f"  Using company field: {company_field}")

# Create output feature class with filtered results
if arcpy.Exists(output_matches):
    arcpy.Delete_management(output_matches)

# Filter: company contains "Meta"
where_clause = f"{company_field} LIKE '%Meta%' OR {company_field} LIKE '%META%' OR {company_field} LIKE '%meta%'"

arcpy.analysis.Select(
    in_features=temp_join,
    out_feature_class=output_matches,
    where_clause=where_clause
)

filtered_count = int(arcpy.GetCount_management(output_matches)[0])
print(f"  Filtered to {filtered_count:,} matches where external data claims 'Meta'")

# ============================================================================
# STEP 4: BUILD FIELD NAME MAPPING
# ============================================================================
print("\n[STEP 4] Building field name mapping...")

# Get all fields from output
output_fields = [f.name for f in arcpy.ListFields(output_matches)]

# Build comprehensive field mapping
field_map = {}

# Meta canonical fields (no suffix - from target feature)
meta_fields = ['location_key', 'datacenter', 'suite', 'dc_code', 'region_derived',
               'it_load', 'new_build_status', 'building_type', 'activity_status']

for field in meta_fields:
    if field in output_fields:
        field_map[field] = field

# External data fields - try with and without _1 suffix
external_base_names = ['source', 'unique_id', 'company_clean', 'campus_name',
                       'city', 'state', 'country', 'full_capacity_mw', 'facility_status']

for base_name in external_base_names:
    # Try with _1 suffix first (most common)
    if f"{base_name}_1" in output_fields:
        field_map[f"{base_name}_ext"] = f"{base_name}_1"
    # Try without suffix
    elif base_name in output_fields:
        field_map[f"{base_name}_ext"] = base_name
    # Try searching for partial match
    else:
        for field in output_fields:
            if base_name in field.lower():
                field_map[f"{base_name}_ext"] = field
                break

# Distance field
field_map['distance'] = 'distance_m'

print(f"  Mapped {len(field_map)} fields:")
for key, value in list(field_map.items())[:10]:
    print(f"    {key} -> {value}")

# ============================================================================
# STEP 5: EXPORT TO DATAFRAME
# ============================================================================
print("\n[STEP 5] Exporting data for analysis...")

# Build fields list from mapping
fields_to_export = list(field_map.values())

# Read into pandas
data = []
with arcpy.da.SearchCursor(output_matches, fields_to_export) as cursor:
    for row in cursor:
        data.append(row)

# Create dataframe with mapped column names
df = pd.DataFrame(data, columns=list(field_map.keys()))

print(f"  Exported {len(df):,} records to dataframe")

# Show sample data
print(f"\n  Sample records:")
print(df.head(3))

# ============================================================================
# STEP 6: CALCULATE METRICS BY SOURCE
# ============================================================================
print("\n[STEP 6] Calculating metrics by source...")
print("\n" + "-"*80)

# Get total Meta canonical locations (exclude UNKNOWN region with 0,0 coords)
meta_valid_count = int(arcpy.GetCount_management(meta_canonical)[0])
with arcpy.da.SearchCursor(meta_canonical, ['region_derived']) as cursor:
    meta_valid_count = sum(1 for row in cursor if row[0] != 'UNKNOWN')

total_meta_sites = meta_valid_count
print(f"Total Meta canonical locations (valid coords): {total_meta_sites:,}")

# Get breakdown of Meta sites by region and build status
meta_by_region = {}
meta_by_status = {}

with arcpy.da.SearchCursor(meta_canonical, ['region_derived', 'new_build_status']) as cursor:
    for row in cursor:
        region = row[0] if row[0] else 'UNKNOWN'
        status = row[1] if row[1] else 'Unknown'

        # Skip UNKNOWN regions (0,0 coords)
        if region == 'UNKNOWN':
            continue

        meta_by_region[region] = meta_by_region.get(region, 0) + 1
        meta_by_status[status] = meta_by_status.get(status, 0) + 1

print(f"\nMeta sites by region (valid coords only):")
for region, count in sorted(meta_by_region.items()):
    print(f"  {region}: {count}")

print(f"\nMeta sites by build status:")
for status, count in sorted(meta_by_status.items()):
    print(f"  {status}: {count}")

# ============================================================================
# ANALYZE BY SOURCE
# ============================================================================
print("\n" + "="*80)
print("ACCURACY METRICS BY SOURCE")
print("="*80)

sources = ['DataCenterHawk', 'Synergy', 'DataCenterMap', 'Semianalysis', 'NewProjectMedia', 'WoodMac']

summary_results = []

for source in sources:
    print(f"\n{'='*80}")
    print(f"SOURCE: {source}")
    print(f"{'='*80}")

    # Filter to this source
    source_data = df[df['source_ext'] == source].copy()

    if len(source_data) == 0:
        print(f"  No matches found for {source}")

        # Add zero results to summary
        summary_results.append({
            'source': source,
            'meta_sites_detected': 0,
            'total_meta_sites': total_meta_sites,
            'recall_pct': 0,
            'total_matches': 0,
            'mean_distance_m': None,
            'median_distance_m': None,
            'within_100m': 0,
            'within_500m': 0,
            'within_1km': 0,
            'amer_recall': 0,
            'emea_recall': 0,
            'apac_recall': 0,
            'complete_build_recall': 0,
            'active_build_recall': 0,
            'future_build_recall': 0
        })
        continue

    # === RECALL: % of Meta sites detected ===
    unique_meta_detected = source_data['location_key'].nunique()
    recall = (unique_meta_detected / total_meta_sites) * 100

    print(f"\n[RECALL] Detection Rate:")
    print(f"  Meta sites detected: {unique_meta_detected} / {total_meta_sites}")
    print(f"  Recall: {recall:.1f}%")

    # === DISTANCE ACCURACY ===
    distances = source_data['distance'].dropna()

    within_100m = len(distances[distances <= 100]) if len(distances) > 0 else 0
    within_500m = len(distances[distances <= 500]) if len(distances) > 0 else 0
    within_1km = len(distances[distances <= 1000]) if len(distances) > 0 else 0
    within_5km = len(distances[distances <= 5000]) if len(distances) > 0 else 0

    if len(distances) > 0:
        mean_dist = distances.mean()
        median_dist = distances.median()
        min_dist = distances.min()
        max_dist = distances.max()

        print(f"\n[DISTANCE ACCURACY]:")
        print(f"  Mean distance: {mean_dist:.1f} meters")
        print(f"  Median distance: {median_dist:.1f} meters")
        print(f"  Min distance: {min_dist:.1f} meters")
        print(f"  Max distance: {max_dist:.1f} meters")

        print(f"\n  Matches within distance thresholds:")
        print(f"    ≤100m: {within_100m} ({within_100m/len(distances)*100:.1f}%)")
        print(f"    ≤500m: {within_500m} ({within_500m/len(distances)*100:.1f}%)")
        print(f"    ≤1km:  {within_1km} ({within_1km/len(distances)*100:.1f}%)")
        print(f"    ≤5km:  {within_5km} ({within_5km/len(distances)*100:.1f}%)")
    else:
        print(f"\n[DISTANCE ACCURACY]: No valid distance measurements")
        mean_dist = None
        median_dist = None

    # === RECALL BY REGION ===
    print(f"\n[RECALL BY REGION]:")
    amer_recall = 0
    emea_recall = 0
    apac_recall = 0

    for region in ['AMER', 'EMEA', 'APAC']:
        region_meta_total = meta_by_region.get(region, 0)
        if region_meta_total == 0:
            continue

        region_detected = source_data[source_data['region_derived'] == region]['location_key'].nunique()
        region_recall = (region_detected / region_meta_total) * 100

        print(f"  {region}: {region_detected}/{region_meta_total} detected ({region_recall:.1f}%)")

        if region == 'AMER':
            amer_recall = region_recall
        elif region == 'EMEA':
            emea_recall = region_recall
        elif region == 'APAC':
            apac_recall = region_recall

    # === RECALL BY BUILD STATUS ===
    print(f"\n[RECALL BY BUILD STATUS]:")
    complete_recall = 0
    active_recall = 0
    future_recall = 0

    for status in ['Complete Build', 'Active Build', 'Future Build']:
        status_meta_total = meta_by_status.get(status, 0)
        if status_meta_total == 0:
            continue

        status_detected = source_data[source_data['new_build_status'] == status]['location_key'].nunique()
        status_recall = (status_detected / status_meta_total) * 100

        print(f"  {status}: {status_detected}/{status_meta_total} detected ({status_recall:.1f}%)")

        if status == 'Complete Build':
            complete_recall = status_recall
        elif status == 'Active Build':
            active_recall = status_recall
        elif status == 'Future Build':
            future_recall = status_recall

    # Store summary
    summary_results.append({
        'source': source,
        'meta_sites_detected': unique_meta_detected,
        'total_meta_sites': total_meta_sites,
        'recall_pct': recall,
        'total_matches': len(source_data),
        'mean_distance_m': mean_dist,
        'median_distance_m': median_dist,
        'within_100m': within_100m,
        'within_500m': within_500m,
        'within_1km': within_1km,
        'amer_recall': amer_recall,
        'emea_recall': emea_recall,
        'apac_recall': apac_recall,
        'complete_build_recall': complete_recall,
        'active_build_recall': active_recall,
        'future_build_recall': future_recall
    })

# ============================================================================
# FINAL SUMMARY
# ============================================================================
print("\n" + "="*80)
print("SUMMARY TABLE")
print("="*80)

summary_df = pd.DataFrame(summary_results)
print(f"\n{summary_df.to_string(index=False)}")

print("\n" + "="*80)
print("ANALYSIS COMPLETE!")
print("="*80)
print(f"\nOutput feature class: {output_matches}")
print(f"  - Contains {filtered_count:,} match records")
print(f"  - Use this for detailed spatial analysis in ArcGIS Pro")

# Cleanup
arcpy.Delete_management(temp_join)
print(f"\nCleaned up temporary files")
import arcpy
import pandas as pd
from datetime import datetime
import math

# ============================================================================
# MULTI-SOURCE SPATIAL ACCURACY ANALYSIS (v3 - Fixed Field Detection)
# ============================================================================

# Configuration
gdb_path = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
meta_canonical = "meta_canonical_v2"
gold_buildings = "gold_buildings"
output_matches = "accuracy_analysis_multi_source"
output_summary = "accuracy_summary_by_source"

print("="*80)
print("MULTI-SOURCE SPATIAL ACCURACY ANALYSIS (v3)")
print("="*80)
print(f"\nCanonical baseline: {meta_canonical}")
print(f"External data: {gold_buildings}")
print(f"\n" + "="*80)

# Clear workspace cache
arcpy.ClearWorkspaceCache_management()

# ============================================================================
# STEP 1: SPATIAL JOIN - META CANONICAL TO EXTERNAL DATA
# ============================================================================
print("\n[STEP 1] Performing spatial join (one-to-many)...")

# Create temporary feature class for spatial join results
temp_join = "temp_spatial_join"
if arcpy.Exists(temp_join):
    arcpy.Delete_management(temp_join)

# Spatial join: Find ALL external records within 5km of each Meta site
arcpy.analysis.SpatialJoin(
    target_features=meta_canonical,
    join_features=gold_buildings,
    out_feature_class=temp_join,
    join_operation="JOIN_ONE_TO_MANY",
    join_type="KEEP_ALL",
    match_option="WITHIN_A_DISTANCE",
    search_radius="5 Kilometers"
)

print(f"  Spatial join complete: {temp_join}")

# Count results
join_count = int(arcpy.GetCount_management(temp_join)[0])
print(f"  Total matches found: {join_count:,}")

# ============================================================================
# STEP 1.5: INSPECT FIELD NAMES
# ============================================================================
print("\n[STEP 1.5] Inspecting spatial join field names...")

all_fields = [f.name for f in arcpy.ListFields(temp_join)]
print(f"  Total fields: {len(all_fields)}")

# List all fields to see what we have
print(f"\n  All field names (first 30):")
for i, field in enumerate(all_fields[:30]):
    print(f"    {i+1}. {field}")

# Smart field detection - try multiple patterns
lat_field_external = None
lon_field_external = None

# Try different patterns in order of preference
patterns = [
    ('latitude_1', 'longitude_1'),  # Most common suffix
    ('latitude', 'longitude'),       # No suffix
    ('lat_1', 'lon_1'),              # Abbreviated with suffix
    ('lat', 'lon')                   # Abbreviated no suffix
]

for lat_pattern, lon_pattern in patterns:
    if lat_pattern in all_fields and lon_pattern in all_fields:
        lat_field_external = lat_pattern
        lon_field_external = lon_pattern
        break

# If still not found, search for any field containing these strings
if not lat_field_external:
    for field in all_fields:
        if 'latitude' in field.lower() and field not in ['SHAPE@Y', 'latitude']:
            lat_field_external = field
            break
    if not lat_field_external:
        lat_field_external = 'latitude'  # Default fallback

if not lon_field_external:
    for field in all_fields:
        if 'longitude' in field.lower() and field not in ['SHAPE@X', 'longitude']:
            lon_field_external = field
            break
    if not lon_field_external:
        lon_field_external = 'longitude'  # Default fallback

print(f"\n  Detected coordinate field names:")
print(f"    Meta canonical: SHAPE@X (lon), SHAPE@Y (lat)")
print(f"    External data: {lon_field_external} (lon), {lat_field_external} (lat)")

# Verify fields exist
if lat_field_external not in all_fields or lon_field_external not in all_fields:
    print(f"\n  ERROR: Could not find latitude/longitude fields!")
    print(f"  Searched for: {lat_field_external}, {lon_field_external}")
    print(f"  Available fields containing 'lat' or 'lon':")
    for field in all_fields:
        if 'lat' in field.lower() or 'lon' in field.lower():
            print(f"    - {field}")
    raise Exception("Cannot proceed without coordinate fields")

# ============================================================================
# STEP 2: CALCULATE GEODESIC DISTANCES
# ============================================================================
print("\n[STEP 2] Calculating geodesic distances...")

# Add distance field
if 'distance_m' not in all_fields:
    arcpy.AddField_management(temp_join, "distance_m", "DOUBLE", field_alias="Distance (meters)")

# Calculate geodesic distance
def calculate_geodesic_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two lat/lon points in meters."""
    if any(v is None for v in [lat1, lon1, lat2, lon2]):
        return None

    # Convert to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371000  # Radius of Earth in meters

    return c * r

# Update distances
update_count = 0
null_count = 0

# Build cursor fields list - all must be valid strings
cursor_fields = ['SHAPE@X', 'SHAPE@Y', lat_field_external, lon_field_external, 'distance_m']

print(f"  Using cursor fields: {cursor_fields}")

with arcpy.da.UpdateCursor(temp_join, cursor_fields) as cursor:
    for row in cursor:
        meta_lon, meta_lat = row[0], row[1]  # Meta canonical coordinates from SHAPE
        ext_lat, ext_lon = row[2], row[3]    # External data coordinates from fields

        if ext_lat is not None and ext_lon is not None:
            distance = calculate_geodesic_distance(meta_lat, meta_lon, ext_lat, ext_lon)
            row[4] = distance
            cursor.updateRow(row)
            update_count += 1
        else:
            null_count += 1

print(f"  Calculated distances for {update_count:,} matches")
print(f"  Skipped {null_count:,} records with null external coordinates")

# ============================================================================
# STEP 3: FILTER TO COMPANY = META
# ============================================================================
print("\n[STEP 3] Filtering to external records claiming 'Meta' company...")

# Find the company field name - try different patterns
company_field = None
if 'company_clean_1' in all_fields:
    company_field = 'company_clean_1'
elif 'company_clean' in all_fields:
    company_field = 'company_clean'
else:
    # Search for any field with 'company' in name
    for field in all_fields:
        if 'company' in field.lower():
            company_field = field
            break

if not company_field:
    print("  ERROR: Could not find company field!")
    raise Exception("Cannot proceed without company field")

print(f"  Using company field: {company_field}")

# Create output feature class with filtered results
if arcpy.Exists(output_matches):
    arcpy.Delete_management(output_matches)

# Filter: company contains "Meta"
where_clause = f"{company_field} LIKE '%Meta%' OR {company_field} LIKE '%META%' OR {company_field} LIKE '%meta%'"

arcpy.analysis.Select(
    in_features=temp_join,
    out_feature_class=output_matches,
    where_clause=where_clause
)

filtered_count = int(arcpy.GetCount_management(output_matches)[0])
print(f"  Filtered to {filtered_count:,} matches where external data claims 'Meta'")

# ============================================================================
# STEP 4: BUILD FIELD NAME MAPPING
# ============================================================================
print("\n[STEP 4] Building field name mapping...")

# Get all fields from output
output_fields = [f.name for f in arcpy.ListFields(output_matches)]

# Build comprehensive field mapping
field_map = {}

# Meta canonical fields (no suffix - from target feature)
meta_fields = ['location_key', 'datacenter', 'suite', 'dc_code', 'region_derived',
               'it_load', 'new_build_status', 'building_type', 'activity_status']

for field in meta_fields:
    if field in output_fields:
        field_map[field] = field

# External data fields - try with and without _1 suffix
external_base_names = ['source', 'unique_id', 'company_clean', 'campus_name',
                       'city', 'state', 'country', 'full_capacity_mw', 'facility_status']

for base_name in external_base_names:
    # Try with _1 suffix first (most common)
    if f"{base_name}_1" in output_fields:
        field_map[f"{base_name}_ext"] = f"{base_name}_1"
    # Try without suffix
    elif base_name in output_fields:
        field_map[f"{base_name}_ext"] = base_name
    # Try searching for partial match
    else:
        for field in output_fields:
            if base_name in field.lower():
                field_map[f"{base_name}_ext"] = field
                break

# Distance field
field_map['distance'] = 'distance_m'

print(f"  Mapped {len(field_map)} fields:")
for key, value in list(field_map.items())[:10]:
    print(f"    {key} -> {value}")

# ============================================================================
# STEP 5: EXPORT TO DATAFRAME
# ============================================================================
print("\n[STEP 5] Exporting data for analysis...")

# Build fields list from mapping
fields_to_export = list(field_map.values())

# Read into pandas
data = []
with arcpy.da.SearchCursor(output_matches, fields_to_export) as cursor:
    for row in cursor:
        data.append(row)

# Create dataframe with mapped column names
df = pd.DataFrame(data, columns=list(field_map.keys()))

print(f"  Exported {len(df):,} records to dataframe")

# Show sample data
print(f"\n  Sample records:")
print(df.head(3))

# ============================================================================
# STEP 6: CALCULATE METRICS BY SOURCE
# ============================================================================
print("\n[STEP 6] Calculating metrics by source...")
print("\n" + "-"*80)

# Get total Meta canonical locations (exclude UNKNOWN region with 0,0 coords)
meta_valid_count = int(arcpy.GetCount_management(meta_canonical)[0])
with arcpy.da.SearchCursor(meta_canonical, ['region_derived']) as cursor:
    meta_valid_count = sum(1 for row in cursor if row[0] != 'UNKNOWN')

total_meta_sites = meta_valid_count
print(f"Total Meta canonical locations (valid coords): {total_meta_sites:,}")

# Get breakdown of Meta sites by region and build status
meta_by_region = {}
meta_by_status = {}

with arcpy.da.SearchCursor(meta_canonical, ['region_derived', 'new_build_status']) as cursor:
    for row in cursor:
        region = row[0] if row[0] else 'UNKNOWN'
        status = row[1] if row[1] else 'Unknown'

        # Skip UNKNOWN regions (0,0 coords)
        if region == 'UNKNOWN':
            continue

        meta_by_region[region] = meta_by_region.get(region, 0) + 1
        meta_by_status[status] = meta_by_status.get(status, 0) + 1

print(f"\nMeta sites by region (valid coords only):")
for region, count in sorted(meta_by_region.items()):
    print(f"  {region}: {count}")

print(f"\nMeta sites by build status:")
for status, count in sorted(meta_by_status.items()):
    print(f"  {status}: {count}")

# ============================================================================
# ANALYZE BY SOURCE
# ============================================================================
print("\n" + "="*80)
print("ACCURACY METRICS BY SOURCE")
print("="*80)

sources = ['DataCenterHawk', 'Synergy', 'DataCenterMap', 'Semianalysis', 'NewProjectMedia', 'WoodMac']

summary_results = []

for source in sources:
    print(f"\n{'='*80}")
    print(f"SOURCE: {source}")
    print(f"{'='*80}")

    # Filter to this source
    source_data = df[df['source_ext'] == source].copy()

    if len(source_data) == 0:
        print(f"  No matches found for {source}")

        # Add zero results to summary
        summary_results.append({
            'source': source,
            'meta_sites_detected': 0,
            'total_meta_sites': total_meta_sites,
            'recall_pct': 0,
            'total_matches': 0,
            'mean_distance_m': None,
            'median_distance_m': None,
            'within_100m': 0,
            'within_500m': 0,
            'within_1km': 0,
            'amer_recall': 0,
            'emea_recall': 0,
            'apac_recall': 0,
            'complete_build_recall': 0,
            'active_build_recall': 0,
            'future_build_recall': 0
        })
        continue

    # === RECALL: % of Meta sites detected ===
    unique_meta_detected = source_data['location_key'].nunique()
    recall = (unique_meta_detected / total_meta_sites) * 100

    print(f"\n[RECALL] Detection Rate:")
    print(f"  Meta sites detected: {unique_meta_detected} / {total_meta_sites}")
    print(f"  Recall: {recall:.1f}%")

    # === DISTANCE ACCURACY ===
    distances = source_data['distance'].dropna()

    within_100m = len(distances[distances <= 100]) if len(distances) > 0 else 0
    within_500m = len(distances[distances <= 500]) if len(distances) > 0 else 0
    within_1km = len(distances[distances <= 1000]) if len(distances) > 0 else 0
    within_5km = len(distances[distances <= 5000]) if len(distances) > 0 else 0

    if len(distances) > 0:
        mean_dist = distances.mean()
        median_dist = distances.median()
        min_dist = distances.min()
        max_dist = distances.max()

        print(f"\n[DISTANCE ACCURACY]:")
        print(f"  Mean distance: {mean_dist:.1f} meters")
        print(f"  Median distance: {median_dist:.1f} meters")
        print(f"  Min distance: {min_dist:.1f} meters")
        print(f"  Max distance: {max_dist:.1f} meters")

        print(f"\n  Matches within distance thresholds:")
        print(f"    ≤100m: {within_100m} ({within_100m/len(distances)*100:.1f}%)")
        print(f"    ≤500m: {within_500m} ({within_500m/len(distances)*100:.1f}%)")
        print(f"    ≤1km:  {within_1km} ({within_1km/len(distances)*100:.1f}%)")
        print(f"    ≤5km:  {within_5km} ({within_5km/len(distances)*100:.1f}%)")
    else:
        print(f"\n[DISTANCE ACCURACY]: No valid distance measurements")
        mean_dist = None
        median_dist = None

    # === RECALL BY REGION ===
    print(f"\n[RECALL BY REGION]:")
    amer_recall = 0
    emea_recall = 0
    apac_recall = 0

    for region in ['AMER', 'EMEA', 'APAC']:
        region_meta_total = meta_by_region.get(region, 0)
        if region_meta_total == 0:
            continue

        region_detected = source_data[source_data['region_derived'] == region]['location_key'].nunique()
        region_recall = (region_detected / region_meta_total) * 100

        print(f"  {region}: {region_detected}/{region_meta_total} detected ({region_recall:.1f}%)")

        if region == 'AMER':
            amer_recall = region_recall
        elif region == 'EMEA':
            emea_recall = region_recall
        elif region == 'APAC':
            apac_recall = region_recall

    # === RECALL BY BUILD STATUS ===
    print(f"\n[RECALL BY BUILD STATUS]:")
    complete_recall = 0
    active_recall = 0
    future_recall = 0

    for status in ['Complete Build', 'Active Build', 'Future Build']:
        status_meta_total = meta_by_status.get(status, 0)
        if status_meta_total == 0:
            continue

        status_detected = source_data[source_data['new_build_status'] == status]['location_key'].nunique()
        status_recall = (status_detected / status_meta_total) * 100

        print(f"  {status}: {status_detected}/{status_meta_total} detected ({status_recall:.1f}%)")

        if status == 'Complete Build':
            complete_recall = status_recall
        elif status == 'Active Build':
            active_recall = status_recall
        elif status == 'Future Build':
            future_recall = status_recall

    # Store summary
    summary_results.append({
        'source': source,
        'meta_sites_detected': unique_meta_detected,
        'total_meta_sites': total_meta_sites,
        'recall_pct': recall,
        'total_matches': len(source_data),
        'mean_distance_m': mean_dist,
        'median_distance_m': median_dist,
        'within_100m': within_100m,
        'within_500m': within_500m,
        'within_1km': within_1km,
        'amer_recall': amer_recall,
        'emea_recall': emea_recall,
        'apac_recall': apac_recall,
        'complete_build_recall': complete_recall,
        'active_build_recall': active_recall,
        'future_build_recall': future_recall
    })

# ============================================================================
# FINAL SUMMARY
# ============================================================================
print("\n" + "="*80)
print("SUMMARY TABLE")
print("="*80)

summary_df = pd.DataFrame(summary_results)
print(f"\n{summary_df.to_string(index=False)}")

print("\n" + "="*80)
print("ANALYSIS COMPLETE!")
print("="*80)
print(f"\nOutput feature class: {output_matches}")
print(f"  - Contains {filtered_count:,} match records")
print(f"  - Use this for detailed spatial analysis in ArcGIS Pro")

# Cleanup
arcpy.Delete_management(temp_join)
print(f"\nCleaned up temporary files")
