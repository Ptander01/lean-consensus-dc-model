import arcpy
import pandas as pd
from datetime import datetime

# ============================================================================
# IMPORT META CANONICAL V2 - From Deduplicated Query
# ============================================================================
# Purpose: Import comprehensive Meta datacenter inventory from internal database
#          Export, deduplicate by location_key, and derive regions from coordinates
#
# Input: CSV from internal DAI query (25k rows -> 1,831 unique locations)
# Output: Feature class 'meta_canonical_v2' in Default.gdb
# ============================================================================

# Configuration
csv_file = r"C:\Users\ptanderson\Downloads\daiquery-896763959683293-1594552168562015-2025-11-25 10_08am.csv"
gdb_path = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
output_fc = "meta_canonical_v2"

print("="*80)
print("META CANONICAL V2 IMPORT - Comprehensive Datacenter Inventory")
print("="*80)
print(f"\nCSV Source: {csv_file}")
print(f"Target GDB: {gdb_path}")
print(f"Output Feature Class: {output_fc}")
print("\n" + "="*80)

# ============================================================================
# STEP 1: LOAD AND INSPECT CSV
# ============================================================================
print("\n[STEP 1] Loading CSV data...")
df = pd.read_csv(csv_file, encoding='utf-8')
print(f"  Loaded {len(df):,} total rows")
print(f"  Columns: {len(df.columns)}")

# ============================================================================
# STEP 2: DEDUPLICATE BY LOCATION_KEY
# ============================================================================
print("\n[STEP 2] Deduplicating by location_key...")
print(f"  Before deduplication: {len(df):,} rows")
print(f"  Unique location_keys: {df['location_key'].nunique():,}")

# Aggregation rules for deduplication
agg_rules = {
    # Take latest/most recent data
    'milestone_date': 'max',  # Latest milestone
    'new_build_status': 'last',  # Most recent status
    'activity_status': 'last',

    # Take maximum capacity (in case of upgrades)
    'it_load': 'max',

    # Take first available geographic data (shouldn't change)
    'latitude': 'first',
    'longitude': 'first',
    'address': 'first',

    # Take first available metadata
    'datacenter': 'first',
    'region': 'first',  # This is DC code, we'll rename it
    'suite': 'first',
    'building_type': 'first',
    'dc_design_type': 'first',
    'project_p6_id': 'first',

    # Concatenate source info (might have multiple milestones from different sources)
    'source_team': lambda x: '; '.join(x.dropna().unique()) if x.notna().any() else None,
    'source_schedule_name': lambda x: '; '.join(x.dropna().unique()) if x.notna().any() else None
}

# Group by location_key and aggregate
df_deduped = df.groupby('location_key', as_index=False).agg(agg_rules)

print(f"  After deduplication: {len(df_deduped):,} rows")
print(f"  Rows removed: {len(df) - len(df_deduped):,}")

# ============================================================================
# STEP 3: DERIVE GEOGRAPHIC REGION FROM COORDINATES
# ============================================================================
print("\n[STEP 3] Deriving geographic regions from coordinates...")

def assign_region(row):
    """
    Assign region (AMER/EMEA/APAC/OTHER) based on lat/lon coordinates.

    Logic:
    - AMER: Americas (longitude -180 to -30)
    - EMEA: Europe, Middle East, Africa (longitude -25 to 65, latitude -35 to 75)
    - APAC: Asia Pacific (longitude 65 to 180)
    - OTHER: Edge cases or unknown
    """
    lat = row['latitude']
    lon = row['longitude']

    # Handle missing coordinates
    if pd.isna(lat) or pd.isna(lon):
        return 'UNKNOWN'

    # Handle suspect coordinates (0,0) or out of range
    if lat == 0 and lon == 0:
        return 'UNKNOWN'
    if abs(lat) > 90 or abs(lon) > 180:
        return 'INVALID'

    # AMER: Longitude -180 to -30 (Americas)
    if -180 <= lon <= -30:
        return 'AMER'

    # EMEA: Longitude -25 to 65 (Europe, Middle East, Africa)
    elif -25 <= lon <= 65:
        if -35 <= lat <= 75:  # Exclude Antarctica
            return 'EMEA'
        else:
            return 'OTHER'

    # APAC: Longitude 65 to 180 (Asia Pacific)
    elif 65 < lon <= 180:
        return 'APAC'

    # Edge cases
    else:
        return 'OTHER'

# Rename original region to dc_code (datacenter code)
df_deduped['dc_code'] = df_deduped['region']

# Derive new region field
df_deduped['region_derived'] = df_deduped.apply(assign_region, axis=1)

# Count by region
print("\n  Region distribution:")
region_counts = df_deduped['region_derived'].value_counts()
for region, count in region_counts.items():
    pct = (count / len(df_deduped)) * 100
    print(f"    {region}: {count} locations ({pct:.1f}%)")

# ============================================================================
# STEP 4: DATA QUALITY CHECKS
# ============================================================================
print("\n[STEP 4] Data quality checks...")

# Coordinate completeness
has_coords = df_deduped[df_deduped['latitude'].notna() & df_deduped['longitude'].notna()]
missing_coords = len(df_deduped) - len(has_coords)

print(f"  Total locations: {len(df_deduped):,}")
print(f"  Valid coordinates: {len(has_coords):,} ({len(has_coords)/len(df_deduped)*100:.1f}%)")
print(f"  Missing coordinates: {missing_coords} ({missing_coords/len(df_deduped)*100:.1f}%)")

# IT load completeness
has_itload = df_deduped[df_deduped['it_load'].notna()]
print(f"  IT load available: {len(has_itload)} ({len(has_itload)/len(df_deduped)*100:.1f}%)")

if len(has_itload) > 0:
    total_mw = has_itload['it_load'].sum()
    print(f"  Total IT load: {total_mw:,.1f} MW")

# Build status breakdown
if 'new_build_status' in df_deduped.columns:
    print("\n  Build status:")
    status_counts = df_deduped['new_build_status'].value_counts()
    for status, count in status_counts.items():
        pct = (count / len(df_deduped)) * 100
        print(f"    {status}: {count} ({pct:.1f}%)")

# ============================================================================
# STEP 5: PREPARE DATA FOR ARCGIS
# ============================================================================
print("\n[STEP 5] Preparing data for ArcGIS import...")

# Convert milestone_date to datetime if it's not already
if df_deduped['milestone_date'].dtype == 'object':
    df_deduped['milestone_date'] = pd.to_datetime(df_deduped['milestone_date'], errors='coerce')

# Handle null values for ArcGIS
# Replace NaN with None for proper NULL handling in geodatabase
df_deduped = df_deduped.where(pd.notnull(df_deduped), None)

# Filter for records WITH coordinates (we'll still import all, but separate step)
df_with_coords = df_deduped[
    df_deduped['latitude'].notna() &
    df_deduped['longitude'].notna() &
    (df_deduped['region_derived'] != 'INVALID')
].copy()

df_without_coords = df_deduped[
    df_deduped['latitude'].isna() |
    df_deduped['longitude'].isna() |
    (df_deduped['region_derived'] == 'INVALID')
].copy()

print(f"  Records with valid coordinates: {len(df_with_coords):,}")
print(f"  Records without coordinates: {len(df_without_coords):,}")

# ============================================================================
# STEP 6: CREATE FEATURE CLASS (SPATIAL DATA ONLY)
# ============================================================================
print("\n[STEP 6] Creating feature class with spatial data...")

# Delete existing feature class if it exists
if arcpy.Exists(output_fc):
    print(f"  Deleting existing {output_fc}...")
    arcpy.Delete_management(output_fc)

# Create feature class with WGS84 coordinate system
sr = arcpy.SpatialReference(4326)  # WGS 1984
arcpy.CreateFeatureclass_management(
    gdb_path,
    output_fc,
    "POINT",
    spatial_reference=sr
)

print(f"  Created feature class: {output_fc}")

# Add fields
print("  Adding fields...")

fields_to_add = [
    # Unique identifiers
    ('location_key', 'TEXT', 20, 'Location Key'),
    ('datacenter', 'TEXT', 10, 'Datacenter (Building Number)'),
    ('suite', 'TEXT', 5, 'Suite'),
    ('dc_code', 'TEXT', 10, 'Datacenter Code'),

    # Geographic fields
    ('region_derived', 'TEXT', 10, 'Region (AMER/EMEA/APAC)'),
    ('address', 'TEXT', 255, 'Address'),

    # Capacity
    ('it_load', 'DOUBLE', None, 'IT Load (MW)'),

    # Status fields
    ('new_build_status', 'TEXT', 50, 'Build Status'),
    ('building_type', 'TEXT', 20, 'Building Type (own/lease)'),
    ('activity_status', 'TEXT', 50, 'Activity Status'),

    # Design fields
    ('dc_design_type', 'TEXT', 10, 'DC Design Type'),

    # Temporal
    ('milestone_date', 'DATE', None, 'Latest Milestone Date'),

    # Source tracking
    ('project_p6_id', 'TEXT', 50, 'Project P6 ID'),
    ('source_team', 'TEXT', 255, 'Source Team(s)'),
    ('source_schedule', 'TEXT', 255, 'Source Schedule(s)'),

    # Metadata
    ('import_date', 'DATE', None, 'Import Date'),
    ('has_coordinates', 'SHORT', None, 'Has Valid Coordinates (1=Yes, 0=No)')
]

for field_name, field_type, field_length, field_alias in fields_to_add:
    if field_type == 'TEXT':
        arcpy.AddField_management(output_fc, field_name, field_type,
                                  field_length=field_length, field_alias=field_alias)
    else:
        arcpy.AddField_management(output_fc, field_name, field_type,
                                  field_alias=field_alias)

print(f"  Added {len(fields_to_add)} fields")

# ============================================================================
# STEP 7: INSERT SPATIAL RECORDS
# ============================================================================
print("\n[STEP 7] Inserting spatial records...")

fields = ['SHAPE@XY', 'location_key', 'datacenter', 'suite', 'dc_code',
          'region_derived', 'address', 'it_load', 'new_build_status',
          'building_type', 'activity_status', 'dc_design_type',
          'milestone_date', 'project_p6_id', 'source_team',
          'source_schedule', 'import_date', 'has_coordinates']

insert_count = 0
error_count = 0

with arcpy.da.InsertCursor(output_fc, fields) as cursor:
    for idx, row in df_with_coords.iterrows():
        try:
            # Create point geometry
            point = (row['longitude'], row['latitude'])

            # Prepare row values
            values = [
                point,
                row['location_key'],
                str(row['datacenter']) if row['datacenter'] is not None else None,
                row['suite'],
                row['dc_code'],
                row['region_derived'],
                row['address'],
                row['it_load'],
                row['new_build_status'],
                row['building_type'],
                row['activity_status'],
                row['dc_design_type'],
                row['milestone_date'],
                row['project_p6_id'],
                row['source_team'],
                row['source_schedule_name'],
                datetime.now(),
                1  # has_coordinates = True
            ]

            cursor.insertRow(values)
            insert_count += 1

            if insert_count % 100 == 0:
                print(f"    Inserted {insert_count} records...", end='\r')

        except Exception as e:
            error_count += 1
            if error_count <= 5:  # Only print first 5 errors
                print(f"\n    Error inserting record {row['location_key']}: {str(e)}")

print(f"\n  Successfully inserted {insert_count:,} spatial records")
if error_count > 0:
    print(f"  Errors: {error_count}")

# ============================================================================
# STEP 8: EXPORT NON-SPATIAL RECORDS TO TABLE
# ============================================================================
print("\n[STEP 8] Exporting non-spatial records to table...")

if len(df_without_coords) > 0:
    table_name = "meta_canonical_v2_no_coords"

    # Delete existing table if exists
    if arcpy.Exists(table_name):
        arcpy.Delete_management(table_name)

    # Create table
    arcpy.CreateTable_management(gdb_path, table_name)

    # Add same fields as feature class (except SHAPE)
    for field_name, field_type, field_length, field_alias in fields_to_add:
        if field_type == 'TEXT':
            arcpy.AddField_management(table_name, field_name, field_type,
                                      field_length=field_length, field_alias=field_alias)
        else:
            arcpy.AddField_management(table_name, field_name, field_type,
                                      field_alias=field_alias)

    # Insert records
    fields_no_shape = ['location_key', 'datacenter', 'suite', 'dc_code',
                       'region_derived', 'address', 'it_load', 'new_build_status',
                       'building_type', 'activity_status', 'dc_design_type',
                       'milestone_date', 'project_p6_id', 'source_team',
                       'source_schedule', 'import_date', 'has_coordinates']

    insert_count_table = 0
    with arcpy.da.InsertCursor(table_name, fields_no_shape) as cursor:
        for idx, row in df_without_coords.iterrows():
            try:
                values = [
                    row['location_key'],
                    str(row['datacenter']) if row['datacenter'] is not None else None,
                    row['suite'],
                    row['dc_code'],
                    row['region_derived'],
                    row['address'],
                    row['it_load'],
                    row['new_build_status'],
                    row['building_type'],
                    row['activity_status'],
                    row['dc_design_type'],
                    row['milestone_date'],
                    row['project_p6_id'],
                    row['source_team'],
                    row['source_schedule_name'],
                    datetime.now(),
                    0  # has_coordinates = False
                ]

                cursor.insertRow(values)
                insert_count_table += 1

            except Exception as e:
                print(f"    Error inserting table record: {str(e)}")

    print(f"  Created table '{table_name}' with {insert_count_table:,} records")
    print(f"  These locations can be geocoded later using the 'address' field")
else:
    print("  No records without coordinates - skipping table creation")

# ============================================================================
# STEP 9: SUMMARY STATISTICS
# ============================================================================
print("\n" + "="*80)
print("IMPORT SUMMARY")
print("="*80)

print(f"\nInput CSV:")
print(f"  Total rows: {len(df):,}")
print(f"  Unique locations: {df['location_key'].nunique():,}")

print(f"\nAfter deduplication:")
print(f"  Total unique locations: {len(df_deduped):,}")

print(f"\nFeature class '{output_fc}':")
print(f"  Spatial records: {insert_count:,}")
print(f"  Total IT load: {df_with_coords['it_load'].sum():,.1f} MW")

if len(df_without_coords) > 0:
    print(f"\nTable 'meta_canonical_v2_no_coords':")
    print(f"  Non-spatial records: {len(df_without_coords):,}")
    print(f"  Can be geocoded using address field")

print(f"\nRegion breakdown (spatial records):")
region_summary = df_with_coords['region_derived'].value_counts()
for region, count in region_summary.items():
    pct = (count / len(df_with_coords)) * 100
    region_itload = df_with_coords[df_with_coords['region_derived'] == region]['it_load'].sum()
    print(f"  {region}: {count} locations ({pct:.1f}%), {region_itload:,.0f} MW")

print(f"\nBuild status breakdown (spatial records):")
status_summary = df_with_coords['new_build_status'].value_counts()
for status, count in status_summary.items():
    pct = (count / len(df_with_coords)) * 100
    print(f"  {status}: {count} ({pct:.1f}%)")

print("\n" + "="*80)
print("IMPORT COMPLETE!")
print("="*80)
print(f"\nNext steps:")
print(f"  1. Review feature class in ArcGIS Pro: {output_fc}")
print(f"  2. Verify region assignments (region_derived field)")
print(f"  3. Optionally geocode records in 'meta_canonical_v2_no_coords' table")
print(f"  4. Run spatial accuracy analysis against external data (gold_buildings)")
print("\n" + "="*80)
