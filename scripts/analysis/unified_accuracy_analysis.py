import arcpy
import pandas as pd
from datetime import datetime
import math

# ============================================================================
# UNIFIED ACCURACY ANALYSIS - Complete Workflow
# ============================================================================
# Purpose:
#   1. Add missing fields to existing spatial matches
#   2. Create unified Global_DC_All_Sources table
#   3. Perform capacity validation analysis (MW comparison)
#
# Replicates Accenture's manual benchmarking at scale for 1,068 Meta sites
# ============================================================================

# Configuration
gdb_path = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
meta_canonical = "meta_canonical_v2"
gold_buildings = "gold_buildings"
gold_campus = "gold_campus"
accuracy_matches = "accuracy_analysis_multi_source"
output_unified = "Global_DC_All_Sources"
output_capacity_report = "capacity_validation_report"

print("="*80)
print("UNIFIED ACCURACY ANALYSIS - Complete Workflow")
print("="*80)
print(f"\nTimestamp: {datetime.now()}")
print(f"Geodatabase: {gdb_path}")
print("\n" + "="*80)

# ============================================================================
# TASK 1: ADD MISSING FIELDS TO SPATIAL MATCHES
# ============================================================================
print("\n" + "="*80)
print("TASK 1: ENHANCE SPATIAL MATCH DATA")
print("="*80)

print("\n[1.1] Adding distance_to_meta_dc_miles field...")

# Add field if doesn't exist
field_list = [f.name for f in arcpy.ListFields(accuracy_matches)]

if 'distance_to_meta_dc_miles' not in field_list:
    arcpy.AddField_management(accuracy_matches, 'distance_to_meta_dc_miles', 'DOUBLE',
                              field_alias='Distance to Meta DC (miles)', field_precision=10, field_scale=2)

# Convert meters to miles (1 meter = 0.000621371 miles)
count = 0
with arcpy.da.UpdateCursor(accuracy_matches, ['distance_m', 'distance_to_meta_dc_miles']) as cursor:
    for row in cursor:
        if row[0] is not None:
            row[1] = row[0] * 0.000621371
            cursor.updateRow(row)
            count += 1

print(f"  Converted {count:,} distance values to miles")

# ============================================================================
print("\n[1.2] Adding meta_location_name field...")

if 'meta_location_name' not in field_list:
    arcpy.AddField_management(accuracy_matches, 'meta_location_name', 'TEXT',
                              field_length=100, field_alias='Meta Location Name')

# Populate with location_key (unique identifier for Meta sites)
count = 0
with arcpy.da.UpdateCursor(accuracy_matches, ['location_key', 'meta_location_name']) as cursor:
    for row in cursor:
        if row[0] is not None:
            row[1] = row[0]
            cursor.updateRow(row)
            count += 1

print(f"  Populated {count:,} meta_location_name values")

# ============================================================================
print("\n[1.3] Adding points and campus_name fields...")

if 'points' not in field_list:
    arcpy.AddField_management(accuracy_matches, 'points', 'TEXT',
                              field_length=20, field_alias='Points (Building/Campus)')

if 'campus_name_meta' not in field_list:
    arcpy.AddField_management(accuracy_matches, 'campus_name_meta', 'TEXT',
                              field_length=100, field_alias='Meta Campus Name')

# For Meta canonical data, determine if Building or Campus level
# Use dc_code as campus identifier
count_building = 0
count_campus = 0

with arcpy.da.UpdateCursor(accuracy_matches,
                          ['location_key', 'dc_code', 'suite', 'points', 'campus_name_meta']) as cursor:
    for row in cursor:
        location_key = row[0]
        dc_code = row[1]
        suite = row[2]

        # If has suite designation (A, B, C, D), it's a building
        if suite is not None and suite.strip() != '':
            row[3] = 'Building'
            count_building += 1
        else:
            row[3] = 'Campus'
            count_campus += 1

        # Campus name is the datacenter code
        row[4] = dc_code if dc_code else location_key

        cursor.updateRow(row)

print(f"  Classified {count_building:,} as Building-level")
print(f"  Classified {count_campus:,} as Campus-level")

# ============================================================================
# TASK 2: CREATE UNIFIED GLOBAL_DC_ALL_SOURCES TABLE
# ============================================================================
print("\n" + "="*80)
print("TASK 2: CREATE UNIFIED GLOBAL_DC_ALL_SOURCES TABLE")
print("="*80)

# Delete existing if exists
if arcpy.Exists(output_unified):
    arcpy.Delete_management(output_unified)

print("\n[2.1] Creating unified feature class schema...")

# Create new feature class with WGS84
sr = arcpy.SpatialReference(4326)
arcpy.CreateFeatureclass_management(gdb_path, output_unified, "POINT", spatial_reference=sr)

# Define unified schema (123 standardized fields from Accenture analysis)
unified_fields = [
    # Identification
    ('unique_id', 'TEXT', 50, 'Unique ID'),
    ('source', 'TEXT', 50, 'Data Source'),
    ('record_level', 'TEXT', 20, 'Record Level (Building/Campus)'),

    # Location identifiers
    ('location_key', 'TEXT', 20, 'Location Key'),
    ('meta_location_name', 'TEXT', 100, 'Meta Location Name'),
    ('campus_id', 'TEXT', 50, 'Campus ID'),
    ('campus_name', 'TEXT', 100, 'Campus Name'),
    ('building_designation', 'TEXT', 50, 'Building Designation'),
    ('suite', 'TEXT', 10, 'Suite'),

    # Geographic
    ('country', 'TEXT', 50, 'Country'),
    ('state', 'TEXT', 50, 'State'),
    ('city', 'TEXT', 50, 'City'),
    ('market', 'TEXT', 50, 'Market'),
    ('address', 'TEXT', 255, 'Address'),
    ('zip_code', 'TEXT', 10, 'Zip Code'),
    ('latitude', 'DOUBLE', None, 'Latitude'),
    ('longitude', 'DOUBLE', None, 'Longitude'),
    ('region', 'TEXT', 20, 'Region (AMER/EMEA/APAC)'),
    ('dc_code', 'TEXT', 10, 'Datacenter Code'),

    # Company
    ('company_source', 'TEXT', 100, 'Company (from source)'),
    ('company_clean', 'TEXT', 100, 'Company (standardized)'),

    # Capacity - Current State
    ('commissioned_power_mw', 'DOUBLE', None, 'Commissioned Power (MW)'),
    ('operational_power_mw', 'DOUBLE', None, 'Operational Power (MW)'),
    ('it_load_mw', 'DOUBLE', None, 'IT Load (MW)'),

    # Capacity - Under Construction
    ('uc_power_mw', 'DOUBLE', None, 'Under Construction Power (MW)'),

    # Capacity - Planned
    ('planned_power_mw', 'DOUBLE', None, 'Planned Power (MW)'),

    # Capacity - Total/Full Build
    ('full_capacity_mw', 'DOUBLE', None, 'Full Build Capacity (MW)'),

    # Capacity - Year-by-Year Projections (2023-2030)
    ('mw_2023', 'DOUBLE', None, 'MW 2023'),
    ('mw_2024', 'DOUBLE', None, 'MW 2024'),
    ('mw_2025', 'DOUBLE', None, 'MW 2025'),
    ('mw_2026', 'DOUBLE', None, 'MW 2026'),
    ('mw_2027', 'DOUBLE', None, 'MW 2027'),
    ('mw_2028', 'DOUBLE', None, 'MW 2028'),
    ('mw_2029', 'DOUBLE', None, 'MW 2029'),
    ('mw_2030', 'DOUBLE', None, 'MW 2030'),

    # Status
    ('facility_status', 'TEXT', 50, 'Facility Status'),
    ('new_build_status', 'TEXT', 50, 'Build Status'),
    ('activity_status', 'TEXT', 50, 'Activity Status'),
    ('building_type', 'TEXT', 20, 'Building Type (own/lease)'),

    # Design
    ('dc_design_type', 'TEXT', 10, 'DC Design Type'),
    ('dc_product_type', 'TEXT', 50, 'DC Product Type'),

    # Dates
    ('date_reported', 'DATE', None, 'Date Reported'),
    ('milestone_date', 'DATE', None, 'Latest Milestone Date'),
    ('live_date', 'DATE', None, 'Live Date'),

    # Spatial Matching (for Meta records only)
    ('distance_to_meta_dc_miles', 'DOUBLE', None, 'Distance to Meta DC (miles)'),
    ('spatial_match_confidence', 'TEXT', 20, 'Match Confidence (High/Medium/Low)'),

    # Metadata
    ('import_date', 'DATE', None, 'Import Date'),
    ('is_meta_actual', 'SHORT', None, 'Is Meta Actual (1=Yes, 0=No)')
]

for field_name, field_type, field_length, field_alias in unified_fields:
    if field_type == 'TEXT':
        arcpy.AddField_management(output_unified, field_name, field_type,
                                  field_length=field_length, field_alias=field_alias)
    else:
        arcpy.AddField_management(output_unified, field_name, field_type, field_alias=field_alias)

print(f"  Created schema with {len(unified_fields)} standardized fields")

# ============================================================================
print("\n[2.2] Importing Meta canonical locations (Meta Actuals)...")

meta_count = 0
meta_fields = ['SHAPE@XY', 'location_key', 'datacenter', 'suite', 'dc_code',
               'region_derived', 'address', 'it_load', 'new_build_status',
               'building_type', 'activity_status', 'dc_design_type', 'milestone_date']

insert_fields = ['SHAPE@XY', 'unique_id', 'source', 'record_level', 'location_key',
                 'meta_location_name', 'campus_name', 'building_designation', 'suite',
                 'address', 'latitude', 'longitude', 'region', 'dc_code',
                 'company_clean', 'it_load_mw', 'new_build_status', 'building_type',
                 'activity_status', 'dc_design_type', 'milestone_date', 'import_date',
                 'is_meta_actual']

with arcpy.da.SearchCursor(meta_canonical, meta_fields) as search_cursor:
    with arcpy.da.InsertCursor(output_unified, insert_fields) as insert_cursor:
        for row in search_cursor:
            point = row[0]

            # Determine if Building or Campus level
            suite = row[3]
            record_level = 'Building' if (suite and suite.strip()) else 'Campus'

            # Prepare insert values
            values = [
                point,                              # SHAPE@XY
                row[1],                             # unique_id (location_key)
                'Meta Actuals',                     # source
                record_level,                       # record_level
                row[1],                             # location_key
                row[1],                             # meta_location_name
                row[4],                             # campus_name (dc_code)
                f"Building {row[2]}" if row[2] else None,  # building_designation
                row[3],                             # suite
                row[6],                             # address
                point[1] if point else None,        # latitude
                point[0] if point else None,        # longitude
                row[5],                             # region
                row[4],                             # dc_code
                'Meta',                             # company_clean
                row[7],                             # it_load_mw
                row[8],                             # new_build_status
                row[9],                             # building_type
                row[10],                            # activity_status
                row[11],                            # dc_design_type
                row[12],                            # milestone_date
                datetime.now(),                     # import_date
                1                                   # is_meta_actual
            ]

            insert_cursor.insertRow(values)
            meta_count += 1

print(f"  Imported {meta_count:,} Meta canonical locations")

# ============================================================================
print("\n[2.3] Importing external source data (gold_buildings)...")

external_count = 0
external_fields = ['SHAPE@XY', 'unique_id', 'source', 'campus_id', 'campus_name',
                   'building_designation', 'company_clean', 'city', 'state', 'country',
                   'address', 'full_capacity_mw', 'planned_power_mw', 'uc_power_mw',
                   'facility_status', 'building_type', 'date_reported',
                   'mw_2023', 'mw_2024', 'mw_2025', 'mw_2026', 'mw_2027',
                   'mw_2028', 'mw_2029', 'mw_2030']

insert_fields_ext = ['SHAPE@XY', 'unique_id', 'source', 'record_level',
                     'campus_id', 'campus_name', 'building_designation',
                     'company_clean', 'city', 'state', 'country', 'address',
                     'latitude', 'longitude',
                     'full_capacity_mw', 'planned_power_mw', 'uc_power_mw',
                     'facility_status', 'building_type', 'date_reported',
                     'mw_2023', 'mw_2024', 'mw_2025', 'mw_2026', 'mw_2027',
                     'mw_2028', 'mw_2029', 'mw_2030', 'import_date', 'is_meta_actual']

with arcpy.da.SearchCursor(gold_buildings, external_fields) as search_cursor:
    with arcpy.da.InsertCursor(output_unified, insert_fields_ext) as insert_cursor:
        for row in search_cursor:
            point = row[0]

            values = [
                point,                              # SHAPE@XY
                row[1],                             # unique_id
                row[2],                             # source
                'Building',                         # record_level (all gold_buildings are building-level)
                row[3],                             # campus_id
                row[4],                             # campus_name
                row[5],                             # building_designation
                row[6],                             # company_clean
                row[7],                             # city
                row[8],                             # state
                row[9],                             # country
                row[10],                            # address
                point[1] if point else None,        # latitude
                point[0] if point else None,        # longitude
                row[11],                            # full_capacity_mw
                row[12],                            # planned_power_mw
                row[13],                            # uc_power_mw
                row[14],                            # facility_status
                row[15],                            # building_type
                row[16],                            # date_reported
                row[17],                            # mw_2023
                row[18],                            # mw_2024
                row[19],                            # mw_2025
                row[20],                            # mw_2026
                row[21],                            # mw_2027
                row[22],                            # mw_2028
                row[23],                            # mw_2029
                row[24],                            # mw_2030
                datetime.now(),                     # import_date
                0                                   # is_meta_actual
            ]

            insert_cursor.insertRow(values)
            external_count += 1

print(f"  Imported {external_count:,} external source records")

# ============================================================================
print("\n[2.4] Adding spatial match information for Meta records...")

# For external records that match Meta sites, add meta_location_name
# Use the accuracy_matches table which already has spatial joins

match_dict = {}
with arcpy.da.SearchCursor(accuracy_matches,
                          ['unique_id', 'meta_location_name', 'distance_to_meta_dc_miles']) as cursor:
    for row in cursor:
        if row[0] and row[1]:
            # Store closest match only
            if row[0] not in match_dict or row[2] < match_dict[row[0]][1]:
                match_dict[row[0]] = (row[1], row[2])

update_count = 0
with arcpy.da.UpdateCursor(output_unified,
                           ['unique_id', 'meta_location_name', 'distance_to_meta_dc_miles',
                            'spatial_match_confidence', 'is_meta_actual']) as cursor:
    for row in cursor:
        # Skip Meta actuals (they don't need matching)
        if row[4] == 1:
            continue

        unique_id = row[0]
        if unique_id in match_dict:
            row[1] = match_dict[unique_id][0]  # meta_location_name
            row[2] = match_dict[unique_id][1]  # distance_to_meta_dc_miles

            # Assign confidence based on distance
            distance = match_dict[unique_id][1]
            if distance < 0.5:  # < 0.5 miles
                row[3] = 'High'
            elif distance < 2.0:  # < 2 miles
                row[3] = 'Medium'
            else:
                row[3] = 'Low'

            cursor.updateRow(row)
            update_count += 1

print(f"  Added spatial match info to {update_count:,} external records")

print(f"\n[2.5] Unified table complete!")
print(f"  Total records: {meta_count + external_count:,}")
print(f"  Meta Actuals: {meta_count:,}")
print(f"  External Sources: {external_count:,}")

# ============================================================================
# TASK 3: CAPACITY VALIDATION ANALYSIS (MW COMPARISON)
# ============================================================================
print("\n" + "="*80)
print("TASK 3: CAPACITY VALIDATION ANALYSIS")
print("="*80)
print("\nReplicating Accenture's MW benchmarking methodology at scale...")

# Export data for analysis
print("\n[3.1] Extracting capacity comparison data...")

# Get Meta actuals with IT load
meta_capacity = {}
with arcpy.da.SearchCursor(meta_canonical,
                           ['location_key', 'it_load', 'dc_code', 'region_derived']) as cursor:
    for row in cursor:
        if row[1] is not None and row[1] > 0:  # Has IT load data
            meta_capacity[row[0]] = {
                'it_load_actual': row[1],
                'dc_code': row[2],
                'region': row[3]
            }

print(f"  Found {len(meta_capacity):,} Meta locations with capacity data")

# Get external source estimates
external_capacity = {}
with arcpy.da.SearchCursor(accuracy_matches,
                           ['meta_location_name', 'source', 'full_capacity_mw',
                            'distance_to_meta_dc_miles']) as cursor:
    for row in cursor:
        meta_loc = row[0]
        source = row[1]
        ext_mw = row[2]
        distance = row[3]

        if meta_loc and source and ext_mw is not None:
            key = (meta_loc, source)

            # Keep closest match for each source
            if key not in external_capacity or distance < external_capacity[key]['distance']:
                external_capacity[key] = {
                    'external_mw': ext_mw,
                    'distance': distance
                }

print(f"  Found {len(external_capacity):,} external capacity estimates")

# ============================================================================
print("\n[3.2] Calculating variance metrics (Accenture methodology)...")

# Build comparison dataframe
comparisons = []

for meta_loc, meta_data in meta_capacity.items():
    actual_mw = meta_data['it_load_actual']

    # Find external estimates for this location
    for source in ['DataCenterHawk', 'Synergy', 'DataCenterMap',
                   'Semianalysis', 'NewProjectMedia', 'WoodMac']:
        key = (meta_loc, source)

        if key in external_capacity:
            ext_mw = external_capacity[key]['external_mw']
            distance = external_capacity[key]['distance']

            # Calculate percent difference
            pct_diff = abs((ext_mw - actual_mw) / actual_mw) * 100

            # Assign variance category (Accenture scoring)
            if pct_diff <= 15:
                variance_category = 'Within 15%'
                variance_score = 1
            elif pct_diff <= 30:
                variance_category = '15-30%'
                variance_score = 2
            elif pct_diff <= 60:
                variance_category = '30-60%'
                variance_score = 3
            else:
                variance_category = '>60%'
                variance_score = 4

            comparisons.append({
                'meta_location': meta_loc,
                'dc_code': meta_data['dc_code'],
                'region': meta_data['region'],
                'source': source,
                'actual_mw': actual_mw,
                'estimated_mw': ext_mw,
                'absolute_diff_mw': abs(ext_mw - actual_mw),
                'percent_diff': pct_diff,
                'variance_category': variance_category,
                'variance_score': variance_score,
                'match_distance_miles': distance
            })

df_capacity = pd.DataFrame(comparisons)

print(f"  Analyzed {len(comparisons):,} capacity comparisons")
print(f"  Covering {df_capacity['meta_location'].nunique():,} unique Meta locations")

# ============================================================================
print("\n[3.3] Generating summary statistics by source...")

summary_stats = []

for source in df_capacity['source'].unique():
    source_data = df_capacity[df_capacity['source'] == source]

    total_comparisons = len(source_data)
    within_15pct = len(source_data[source_data['variance_category'] == 'Within 15%'])
    within_30pct = len(source_data[source_data['variance_category'].isin(['Within 15%', '15-30%'])])
    over_60pct = len(source_data[source_data['variance_category'] == '>60%'])

    mean_pct_error = source_data['percent_diff'].mean()
    median_pct_error = source_data['percent_diff'].median()
    mean_abs_error = source_data['absolute_diff_mw'].mean()

    summary_stats.append({
        'source': source,
        'total_comparisons': total_comparisons,
        'within_15pct': within_15pct,
        'within_15pct_rate': (within_15pct / total_comparisons * 100) if total_comparisons > 0 else 0,
        'within_30pct': within_30pct,
        'within_30pct_rate': (within_30pct / total_comparisons * 100) if total_comparisons > 0 else 0,
        'over_60pct': over_60pct,
        'over_60pct_rate': (over_60pct / total_comparisons * 100) if total_comparisons > 0 else 0,
        'mean_pct_error': mean_pct_error,
        'median_pct_error': median_pct_error,
        'mean_abs_error_mw': mean_abs_error
    })

df_summary = pd.DataFrame(summary_stats)

print("\n" + "="*80)
print("CAPACITY ACCURACY SUMMARY (Accenture-Style Benchmarking)")
print("="*80)
print(f"\n{df_summary.to_string(index=False)}")

# ============================================================================
print("\n[3.4] Exporting capacity validation report to geodatabase...")

# Create table
if arcpy.Exists(output_capacity_report):
    arcpy.Delete_management(output_capacity_report)

arcpy.CreateTable_management(gdb_path, output_capacity_report)

# Add fields
report_fields = [
    ('meta_location', 'TEXT', 50),
    ('dc_code', 'TEXT', 10),
    ('region', 'TEXT', 20),
    ('source', 'TEXT', 50),
    ('actual_mw', 'DOUBLE', None),
    ('estimated_mw', 'DOUBLE', None),
    ('absolute_diff_mw', 'DOUBLE', None),
    ('percent_diff', 'DOUBLE', None),
    ('variance_category', 'TEXT', 20),
    ('variance_score', 'SHORT', None),
    ('match_distance_miles', 'DOUBLE', None)
]

for field_name, field_type, field_length in report_fields:
    if field_type == 'TEXT':
        arcpy.AddField_management(output_capacity_report, field_name, field_type,
                                  field_length=field_length)
    else:
        arcpy.AddField_management(output_capacity_report, field_name, field_type)

# Insert rows
with arcpy.da.InsertCursor(output_capacity_report, [f[0] for f in report_fields]) as cursor:
    for idx, row in df_capacity.iterrows():
        cursor.insertRow([
            row['meta_location'],
            row['dc_code'],
            row['region'],
            row['source'],
            row['actual_mw'],
            row['estimated_mw'],
            row['absolute_diff_mw'],
            row['percent_diff'],
            row['variance_category'],
            row['variance_score'],
            row['match_distance_miles']
        ])

print(f"  Created table: {output_capacity_report}")
print(f"  Records: {len(df_capacity):,}")

# ============================================================================
# FINAL SUMMARY
# ============================================================================
print("\n" + "="*80)
print("ANALYSIS COMPLETE - ALL TASKS FINISHED")
print("="*80)

print("\n📊 TASK 1: Enhanced Spatial Match Data")
print(f"  ✓ Added distance_to_meta_dc_miles")
print(f"  ✓ Added meta_location_name")
print(f"  ✓ Added points (Building/Campus designation)")
print(f"  ✓ Added campus_name_meta")

print("\n📊 TASK 2: Unified Global_DC_All_Sources Table")
print(f"  ✓ Feature class: {output_unified}")
print(f"  ✓ Total records: {meta_count + external_count:,}")
print(f"    - Meta Actuals: {meta_count:,}")
print(f"    - External Sources: {external_count:,}")
print(f"  ✓ Standardized schema: {len(unified_fields)} fields")
print(f"  ✓ Spatial matches linked: {update_count:,} records")

print("\n📊 TASK 3: Capacity Validation Analysis")
print(f"  ✓ Report table: {output_capacity_report}")
print(f"  ✓ Comparisons analyzed: {len(comparisons):,}")
print(f"  ✓ Meta locations covered: {df_capacity['meta_location'].nunique():,}")
print(f"  ✓ Methodology: Accenture variance scoring replicated")

print("\n🎯 KEY FINDINGS:")
print("\nCapacity Accuracy by Source (% Within 15% of Meta Actual):")
for idx, row in df_summary.iterrows():
    print(f"  {row['source']}: {row['within_15pct_rate']:.1f}% " +
          f"({row['within_15pct']}/{row['total_comparisons']} sites)")

print("\n📁 OUTPUT FILES:")
print(f"  1. {accuracy_matches} (enhanced with new fields)")
print(f"  2. {output_unified} (unified feature class)")
print(f"  3. {output_capacity_report} (capacity validation table)")

print("\n💡 NEXT STEPS:")
print("  1. Review Global_DC_All_Sources in ArcGIS Pro")
print("  2. Validate capacity_validation_report matches Accenture's findings")
print("  3. Use for ESRI Experience Builder dashboard development")
print("  4. Share results with supervisor for review")

print("\n" + "="*80)
import arcpy
import pandas as pd
from datetime import datetime
import math

# ============================================================================
# UNIFIED ACCURACY ANALYSIS - Complete Workflow
# ============================================================================
# Purpose:
#   1. Add missing fields to existing spatial matches
#   2. Create unified Global_DC_All_Sources table
#   3. Perform capacity validation analysis (MW comparison)
#
# Replicates Accenture's manual benchmarking at scale for 1,068 Meta sites
# ============================================================================

# Configuration
gdb_path = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
meta_canonical = "meta_canonical_v2"
gold_buildings = "gold_buildings"
gold_campus = "gold_campus"
accuracy_matches = "accuracy_analysis_multi_source"
output_unified = "Global_DC_All_Sources"
output_capacity_report = "capacity_validation_report"

print("="*80)
print("UNIFIED ACCURACY ANALYSIS - Complete Workflow")
print("="*80)
print(f"\nTimestamp: {datetime.now()}")
print(f"Geodatabase: {gdb_path}")
print("\n" + "="*80)

# ============================================================================
# TASK 1: ADD MISSING FIELDS TO SPATIAL MATCHES
# ============================================================================
print("\n" + "="*80)
print("TASK 1: ENHANCE SPATIAL MATCH DATA")
print("="*80)

print("\n[1.1] Adding distance_to_meta_dc_miles field...")

# Add field if doesn't exist
field_list = [f.name for f in arcpy.ListFields(accuracy_matches)]

if 'distance_to_meta_dc_miles' not in field_list:
    arcpy.AddField_management(accuracy_matches, 'distance_to_meta_dc_miles', 'DOUBLE',
                              field_alias='Distance to Meta DC (miles)', field_precision=10, field_scale=2)

# Convert meters to miles (1 meter = 0.000621371 miles)
count = 0
with arcpy.da.UpdateCursor(accuracy_matches, ['distance_m', 'distance_to_meta_dc_miles']) as cursor:
    for row in cursor:
        if row[0] is not None:
            row[1] = row[0] * 0.000621371
            cursor.updateRow(row)
            count += 1

print(f"  Converted {count:,} distance values to miles")

# ============================================================================
print("\n[1.2] Adding meta_location_name field...")

if 'meta_location_name' not in field_list:
    arcpy.AddField_management(accuracy_matches, 'meta_location_name', 'TEXT',
                              field_length=100, field_alias='Meta Location Name')

# Populate with location_key (unique identifier for Meta sites)
count = 0
with arcpy.da.UpdateCursor(accuracy_matches, ['location_key', 'meta_location_name']) as cursor:
    for row in cursor:
        if row[0] is not None:
            row[1] = row[0]
            cursor.updateRow(row)
            count += 1

print(f"  Populated {count:,} meta_location_name values")

# ============================================================================
print("\n[1.3] Adding points and campus_name fields...")

if 'points' not in field_list:
    arcpy.AddField_management(accuracy_matches, 'points', 'TEXT',
                              field_length=20, field_alias='Points (Building/Campus)')

if 'campus_name_meta' not in field_list:
    arcpy.AddField_management(accuracy_matches, 'campus_name_meta', 'TEXT',
                              field_length=100, field_alias='Meta Campus Name')

# For Meta canonical data, determine if Building or Campus level
# Use dc_code as campus identifier
count_building = 0
count_campus = 0

with arcpy.da.UpdateCursor(accuracy_matches,
                          ['location_key', 'dc_code', 'suite', 'points', 'campus_name_meta']) as cursor:
    for row in cursor:
        location_key = row[0]
        dc_code = row[1]
        suite = row[2]

        # If has suite designation (A, B, C, D), it's a building
        if suite is not None and suite.strip() != '':
            row[3] = 'Building'
            count_building += 1
        else:
            row[3] = 'Campus'
            count_campus += 1

        # Campus name is the datacenter code
        row[4] = dc_code if dc_code else location_key

        cursor.updateRow(row)

print(f"  Classified {count_building:,} as Building-level")
print(f"  Classified {count_campus:,} as Campus-level")

# ============================================================================
# TASK 2: CREATE UNIFIED GLOBAL_DC_ALL_SOURCES TABLE
# ============================================================================
print("\n" + "="*80)
print("TASK 2: CREATE UNIFIED GLOBAL_DC_ALL_SOURCES TABLE")
print("="*80)

# Delete existing if exists
if arcpy.Exists(output_unified):
    arcpy.Delete_management(output_unified)

print("\n[2.1] Creating unified feature class schema...")

# Create new feature class with WGS84
sr = arcpy.SpatialReference(4326)
arcpy.CreateFeatureclass_management(gdb_path, output_unified, "POINT", spatial_reference=sr)

# Define unified schema (123 standardized fields from Accenture analysis)
unified_fields = [
    # Identification
    ('unique_id', 'TEXT', 50, 'Unique ID'),
    ('source', 'TEXT', 50, 'Data Source'),
    ('record_level', 'TEXT', 20, 'Record Level (Building/Campus)'),

    # Location identifiers
    ('location_key', 'TEXT', 20, 'Location Key'),
    ('meta_location_name', 'TEXT', 100, 'Meta Location Name'),
    ('campus_id', 'TEXT', 50, 'Campus ID'),
    ('campus_name', 'TEXT', 100, 'Campus Name'),
    ('building_designation', 'TEXT', 50, 'Building Designation'),
    ('suite', 'TEXT', 10, 'Suite'),

    # Geographic
    ('country', 'TEXT', 50, 'Country'),
    ('state', 'TEXT', 50, 'State'),
    ('city', 'TEXT', 50, 'City'),
    ('market', 'TEXT', 50, 'Market'),
    ('address', 'TEXT', 255, 'Address'),
    ('zip_code', 'TEXT', 10, 'Zip Code'),
    ('latitude', 'DOUBLE', None, 'Latitude'),
    ('longitude', 'DOUBLE', None, 'Longitude'),
    ('region', 'TEXT', 20, 'Region (AMER/EMEA/APAC)'),
    ('dc_code', 'TEXT', 10, 'Datacenter Code'),

    # Company
    ('company_source', 'TEXT', 100, 'Company (from source)'),
    ('company_clean', 'TEXT', 100, 'Company (standardized)'),

    # Capacity - Current State
    ('commissioned_power_mw', 'DOUBLE', None, 'Commissioned Power (MW)'),
    ('operational_power_mw', 'DOUBLE', None, 'Operational Power (MW)'),
    ('it_load_mw', 'DOUBLE', None, 'IT Load (MW)'),

    # Capacity - Under Construction
    ('uc_power_mw', 'DOUBLE', None, 'Under Construction Power (MW)'),

    # Capacity - Planned
    ('planned_power_mw', 'DOUBLE', None, 'Planned Power (MW)'),

    # Capacity - Total/Full Build
    ('full_capacity_mw', 'DOUBLE', None, 'Full Build Capacity (MW)'),

    # Capacity - Year-by-Year Projections (2023-2030)
    ('mw_2023', 'DOUBLE', None, 'MW 2023'),
    ('mw_2024', 'DOUBLE', None, 'MW 2024'),
    ('mw_2025', 'DOUBLE', None, 'MW 2025'),
    ('mw_2026', 'DOUBLE', None, 'MW 2026'),
    ('mw_2027', 'DOUBLE', None, 'MW 2027'),
    ('mw_2028', 'DOUBLE', None, 'MW 2028'),
    ('mw_2029', 'DOUBLE', None, 'MW 2029'),
    ('mw_2030', 'DOUBLE', None, 'MW 2030'),

    # Status
    ('facility_status', 'TEXT', 50, 'Facility Status'),
    ('new_build_status', 'TEXT', 50, 'Build Status'),
    ('activity_status', 'TEXT', 50, 'Activity Status'),
    ('building_type', 'TEXT', 20, 'Building Type (own/lease)'),

    # Design
    ('dc_design_type', 'TEXT', 10, 'DC Design Type'),
    ('dc_product_type', 'TEXT', 50, 'DC Product Type'),

    # Dates
    ('date_reported', 'DATE', None, 'Date Reported'),
    ('milestone_date', 'DATE', None, 'Latest Milestone Date'),
    ('live_date', 'DATE', None, 'Live Date'),

    # Spatial Matching (for Meta records only)
    ('distance_to_meta_dc_miles', 'DOUBLE', None, 'Distance to Meta DC (miles)'),
    ('spatial_match_confidence', 'TEXT', 20, 'Match Confidence (High/Medium/Low)'),

    # Metadata
    ('import_date', 'DATE', None, 'Import Date'),
    ('is_meta_actual', 'SHORT', None, 'Is Meta Actual (1=Yes, 0=No)')
]

for field_name, field_type, field_length, field_alias in unified_fields:
    if field_type == 'TEXT':
        arcpy.AddField_management(output_unified, field_name, field_type,
                                  field_length=field_length, field_alias=field_alias)
    else:
        arcpy.AddField_management(output_unified, field_name, field_type, field_alias=field_alias)

print(f"  Created schema with {len(unified_fields)} standardized fields")

# ============================================================================
print("\n[2.2] Importing Meta canonical locations (Meta Actuals)...")

meta_count = 0
meta_fields = ['SHAPE@XY', 'location_key', 'datacenter', 'suite', 'dc_code',
               'region_derived', 'address', 'it_load', 'new_build_status',
               'building_type', 'activity_status', 'dc_design_type', 'milestone_date']

insert_fields = ['SHAPE@XY', 'unique_id', 'source', 'record_level', 'location_key',
                 'meta_location_name', 'campus_name', 'building_designation', 'suite',
                 'address', 'latitude', 'longitude', 'region', 'dc_code',
                 'company_clean', 'it_load_mw', 'new_build_status', 'building_type',
                 'activity_status', 'dc_design_type', 'milestone_date', 'import_date',
                 'is_meta_actual']

with arcpy.da.SearchCursor(meta_canonical, meta_fields) as search_cursor:
    with arcpy.da.InsertCursor(output_unified, insert_fields) as insert_cursor:
        for row in search_cursor:
            point = row[0]

            # Determine if Building or Campus level
            suite = row[3]
            record_level = 'Building' if (suite and suite.strip()) else 'Campus'

            # Prepare insert values
            values = [
                point,                              # SHAPE@XY
                row[1],                             # unique_id (location_key)
                'Meta Actuals',                     # source
                record_level,                       # record_level
                row[1],                             # location_key
                row[1],                             # meta_location_name
                row[4],                             # campus_name (dc_code)
                f"Building {row[2]}" if row[2] else None,  # building_designation
                row[3],                             # suite
                row[6],                             # address
                point[1] if point else None,        # latitude
                point[0] if point else None,        # longitude
                row[5],                             # region
                row[4],                             # dc_code
                'Meta',                             # company_clean
                row[7],                             # it_load_mw
                row[8],                             # new_build_status
                row[9],                             # building_type
                row[10],                            # activity_status
                row[11],                            # dc_design_type
                row[12],                            # milestone_date
                datetime.now(),                     # import_date
                1                                   # is_meta_actual
            ]

            insert_cursor.insertRow(values)
            meta_count += 1

print(f"  Imported {meta_count:,} Meta canonical locations")

# ============================================================================
print("\n[2.3] Importing external source data (gold_buildings)...")

external_count = 0
external_fields = ['SHAPE@XY', 'unique_id', 'source', 'campus_id', 'campus_name',
                   'building_designation', 'company_clean', 'city', 'state', 'country',
                   'address', 'full_capacity_mw', 'planned_power_mw', 'uc_power_mw',
                   'facility_status', 'building_type', 'date_reported',
                   'mw_2023', 'mw_2024', 'mw_2025', 'mw_2026', 'mw_2027',
                   'mw_2028', 'mw_2029', 'mw_2030']

insert_fields_ext = ['SHAPE@XY', 'unique_id', 'source', 'record_level',
                     'campus_id', 'campus_name', 'building_designation',
                     'company_clean', 'city', 'state', 'country', 'address',
                     'latitude', 'longitude',
                     'full_capacity_mw', 'planned_power_mw', 'uc_power_mw',
                     'facility_status', 'building_type', 'date_reported',
                     'mw_2023', 'mw_2024', 'mw_2025', 'mw_2026', 'mw_2027',
                     'mw_2028', 'mw_2029', 'mw_2030', 'import_date', 'is_meta_actual']

with arcpy.da.SearchCursor(gold_buildings, external_fields) as search_cursor:
    with arcpy.da.InsertCursor(output_unified, insert_fields_ext) as insert_cursor:
        for row in search_cursor:
            point = row[0]

            values = [
                point,                              # SHAPE@XY
                row[1],                             # unique_id
                row[2],                             # source
                'Building',                         # record_level (all gold_buildings are building-level)
                row[3],                             # campus_id
                row[4],                             # campus_name
                row[5],                             # building_designation
                row[6],                             # company_clean
                row[7],                             # city
                row[8],                             # state
                row[9],                             # country
                row[10],                            # address
                point[1] if point else None,        # latitude
                point[0] if point else None,        # longitude
                row[11],                            # full_capacity_mw
                row[12],                            # planned_power_mw
                row[13],                            # uc_power_mw
                row[14],                            # facility_status
                row[15],                            # building_type
                row[16],                            # date_reported
                row[17],                            # mw_2023
                row[18],                            # mw_2024
                row[19],                            # mw_2025
                row[20],                            # mw_2026
                row[21],                            # mw_2027
                row[22],                            # mw_2028
                row[23],                            # mw_2029
                row[24],                            # mw_2030
                datetime.now(),                     # import_date
                0                                   # is_meta_actual
            ]

            insert_cursor.insertRow(values)
            external_count += 1

print(f"  Imported {external_count:,} external source records")

# ============================================================================
print("\n[2.4] Adding spatial match information for Meta records...")

# For external records that match Meta sites, add meta_location_name
# Use the accuracy_matches table which already has spatial joins

match_dict = {}
with arcpy.da.SearchCursor(accuracy_matches,
                          ['unique_id', 'meta_location_name', 'distance_to_meta_dc_miles']) as cursor:
    for row in cursor:
        if row[0] and row[1]:
            # Store closest match only
            if row[0] not in match_dict or row[2] < match_dict[row[0]][1]:
                match_dict[row[0]] = (row[1], row[2])

update_count = 0
with arcpy.da.UpdateCursor(output_unified,
                           ['unique_id', 'meta_location_name', 'distance_to_meta_dc_miles',
                            'spatial_match_confidence', 'is_meta_actual']) as cursor:
    for row in cursor:
        # Skip Meta actuals (they don't need matching)
        if row[4] == 1:
            continue

        unique_id = row[0]
        if unique_id in match_dict:
            row[1] = match_dict[unique_id][0]  # meta_location_name
            row[2] = match_dict[unique_id][1]  # distance_to_meta_dc_miles

            # Assign confidence based on distance
            distance = match_dict[unique_id][1]
            if distance < 0.5:  # < 0.5 miles
                row[3] = 'High'
            elif distance < 2.0:  # < 2 miles
                row[3] = 'Medium'
            else:
                row[3] = 'Low'

            cursor.updateRow(row)
            update_count += 1

print(f"  Added spatial match info to {update_count:,} external records")

print(f"\n[2.5] Unified table complete!")
print(f"  Total records: {meta_count + external_count:,}")
print(f"  Meta Actuals: {meta_count:,}")
print(f"  External Sources: {external_count:,}")

# ============================================================================
# TASK 3: CAPACITY VALIDATION ANALYSIS (MW COMPARISON)
# ============================================================================
print("\n" + "="*80)
print("TASK 3: CAPACITY VALIDATION ANALYSIS")
print("="*80)
print("\nReplicating Accenture's MW benchmarking methodology at scale...")

# Export data for analysis
print("\n[3.1] Extracting capacity comparison data...")

# Get Meta actuals with IT load
meta_capacity = {}
with arcpy.da.SearchCursor(meta_canonical,
                           ['location_key', 'it_load', 'dc_code', 'region_derived']) as cursor:
    for row in cursor:
        if row[1] is not None and row[1] > 0:  # Has IT load data
            meta_capacity[row[0]] = {
                'it_load_actual': row[1],
                'dc_code': row[2],
                'region': row[3]
            }

print(f"  Found {len(meta_capacity):,} Meta locations with capacity data")

# Get external source estimates
external_capacity = {}
with arcpy.da.SearchCursor(accuracy_matches,
                           ['meta_location_name', 'source', 'full_capacity_mw',
                            'distance_to_meta_dc_miles']) as cursor:
    for row in cursor:
        meta_loc = row[0]
        source = row[1]
        ext_mw = row[2]
        distance = row[3]

        if meta_loc and source and ext_mw is not None:
            key = (meta_loc, source)

            # Keep closest match for each source
            if key not in external_capacity or distance < external_capacity[key]['distance']:
                external_capacity[key] = {
                    'external_mw': ext_mw,
                    'distance': distance
                }

print(f"  Found {len(external_capacity):,} external capacity estimates")

# ============================================================================
print("\n[3.2] Calculating variance metrics (Accenture methodology)...")

# Build comparison dataframe
comparisons = []

for meta_loc, meta_data in meta_capacity.items():
    actual_mw = meta_data['it_load_actual']

    # Find external estimates for this location
    for source in ['DataCenterHawk', 'Synergy', 'DataCenterMap',
                   'Semianalysis', 'NewProjectMedia', 'WoodMac']:
        key = (meta_loc, source)

        if key in external_capacity:
            ext_mw = external_capacity[key]['external_mw']
            distance = external_capacity[key]['distance']

            # Calculate percent difference
            pct_diff = abs((ext_mw - actual_mw) / actual_mw) * 100

            # Assign variance category (Accenture scoring)
            if pct_diff <= 15:
                variance_category = 'Within 15%'
                variance_score = 1
            elif pct_diff <= 30:
                variance_category = '15-30%'
                variance_score = 2
            elif pct_diff <= 60:
                variance_category = '30-60%'
                variance_score = 3
            else:
                variance_category = '>60%'
                variance_score = 4

            comparisons.append({
                'meta_location': meta_loc,
                'dc_code': meta_data['dc_code'],
                'region': meta_data['region'],
                'source': source,
                'actual_mw': actual_mw,
                'estimated_mw': ext_mw,
                'absolute_diff_mw': abs(ext_mw - actual_mw),
                'percent_diff': pct_diff,
                'variance_category': variance_category,
                'variance_score': variance_score,
                'match_distance_miles': distance
            })

df_capacity = pd.DataFrame(comparisons)

print(f"  Analyzed {len(comparisons):,} capacity comparisons")
print(f"  Covering {df_capacity['meta_location'].nunique():,} unique Meta locations")

# ============================================================================
print("\n[3.3] Generating summary statistics by source...")

summary_stats = []

for source in df_capacity['source'].unique():
    source_data = df_capacity[df_capacity['source'] == source]

    total_comparisons = len(source_data)
    within_15pct = len(source_data[source_data['variance_category'] == 'Within 15%'])
    within_30pct = len(source_data[source_data['variance_category'].isin(['Within 15%', '15-30%'])])
    over_60pct = len(source_data[source_data['variance_category'] == '>60%'])

    mean_pct_error = source_data['percent_diff'].mean()
    median_pct_error = source_data['percent_diff'].median()
    mean_abs_error = source_data['absolute_diff_mw'].mean()

    summary_stats.append({
        'source': source,
        'total_comparisons': total_comparisons,
        'within_15pct': within_15pct,
        'within_15pct_rate': (within_15pct / total_comparisons * 100) if total_comparisons > 0 else 0,
        'within_30pct': within_30pct,
        'within_30pct_rate': (within_30pct / total_comparisons * 100) if total_comparisons > 0 else 0,
        'over_60pct': over_60pct,
        'over_60pct_rate': (over_60pct / total_comparisons * 100) if total_comparisons > 0 else 0,
        'mean_pct_error': mean_pct_error,
        'median_pct_error': median_pct_error,
        'mean_abs_error_mw': mean_abs_error
    })

df_summary = pd.DataFrame(summary_stats)

print("\n" + "="*80)
print("CAPACITY ACCURACY SUMMARY (Accenture-Style Benchmarking)")
print("="*80)
print(f"\n{df_summary.to_string(index=False)}")

# ============================================================================
print("\n[3.4] Exporting capacity validation report to geodatabase...")

# Create table
if arcpy.Exists(output_capacity_report):
    arcpy.Delete_management(output_capacity_report)

arcpy.CreateTable_management(gdb_path, output_capacity_report)

# Add fields
report_fields = [
    ('meta_location', 'TEXT', 50),
    ('dc_code', 'TEXT', 10),
    ('region', 'TEXT', 20),
    ('source', 'TEXT', 50),
    ('actual_mw', 'DOUBLE', None),
    ('estimated_mw', 'DOUBLE', None),
    ('absolute_diff_mw', 'DOUBLE', None),
    ('percent_diff', 'DOUBLE', None),
    ('variance_category', 'TEXT', 20),
    ('variance_score', 'SHORT', None),
    ('match_distance_miles', 'DOUBLE', None)
]

for field_name, field_type, field_length in report_fields:
    if field_type == 'TEXT':
        arcpy.AddField_management(output_capacity_report, field_name, field_type,
                                  field_length=field_length)
    else:
        arcpy.AddField_management(output_capacity_report, field_name, field_type)

# Insert rows
with arcpy.da.InsertCursor(output_capacity_report, [f[0] for f in report_fields]) as cursor:
    for idx, row in df_capacity.iterrows():
        cursor.insertRow([
            row['meta_location'],
            row['dc_code'],
            row['region'],
            row['source'],
            row['actual_mw'],
            row['estimated_mw'],
            row['absolute_diff_mw'],
            row['percent_diff'],
            row['variance_category'],
            row['variance_score'],
            row['match_distance_miles']
        ])

print(f"  Created table: {output_capacity_report}")
print(f"  Records: {len(df_capacity):,}")

# ============================================================================
# FINAL SUMMARY
# ============================================================================
print("\n" + "="*80)
print("ANALYSIS COMPLETE - ALL TASKS FINISHED")
print("="*80)

print("\n📊 TASK 1: Enhanced Spatial Match Data")
print(f"  ✓ Added distance_to_meta_dc_miles")
print(f"  ✓ Added meta_location_name")
print(f"  ✓ Added points (Building/Campus designation)")
print(f"  ✓ Added campus_name_meta")

print("\n📊 TASK 2: Unified Global_DC_All_Sources Table")
print(f"  ✓ Feature class: {output_unified}")
print(f"  ✓ Total records: {meta_count + external_count:,}")
print(f"    - Meta Actuals: {meta_count:,}")
print(f"    - External Sources: {external_count:,}")
print(f"  ✓ Standardized schema: {len(unified_fields)} fields")
print(f"  ✓ Spatial matches linked: {update_count:,} records")

print("\n📊 TASK 3: Capacity Validation Analysis")
print(f"  ✓ Report table: {output_capacity_report}")
print(f"  ✓ Comparisons analyzed: {len(comparisons):,}")
print(f"  ✓ Meta locations covered: {df_capacity['meta_location'].nunique():,}")
print(f"  ✓ Methodology: Accenture variance scoring replicated")

print("\n🎯 KEY FINDINGS:")
print("\nCapacity Accuracy by Source (% Within 15% of Meta Actual):")
for idx, row in df_summary.iterrows():
    print(f"  {row['source']}: {row['within_15pct_rate']:.1f}% " +
          f"({row['within_15pct']}/{row['total_comparisons']} sites)")

print("\n📁 OUTPUT FILES:")
print(f"  1. {accuracy_matches} (enhanced with new fields)")
print(f"  2. {output_unified} (unified feature class)")
print(f"  3. {output_capacity_report} (capacity validation table)")

print("\n💡 NEXT STEPS:")
print("  1. Review Global_DC_All_Sources in ArcGIS Pro")
print("  2. Validate capacity_validation_report matches Accenture's findings")
print("  3. Use for ESRI Experience Builder dashboard development")
print("  4. Share results with supervisor for review")

print("\n" + "="*80)
