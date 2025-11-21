import arcpy
import os
from datetime import datetime

# ============================================================================
# CAMPUS ROLLUP WORKFLOW - WITH COST/ACREAGE + YEAR MW FIELDS
# ============================================================================

gdb_path = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
arcpy.env.workspace = gdb_path
arcpy.env.overwriteOutput = True

gold_buildings = os.path.join(gdb_path, "gold_buildings")
gold_campus = os.path.join(gdb_path, "gold_campus")

print("="*80)
print("CAMPUS ROLLUP WORKFLOW - WITH COST/ACREAGE + YEAR MW FIELDS")
print("="*80)

# Verify gold_buildings exists and has records
if not arcpy.Exists(gold_buildings):
    print(f"ERROR: gold_buildings does not exist")
    exit()

building_count = int(arcpy.management.GetCount(gold_buildings)[0])
print(f"\nStarting with {building_count} building records")

# Step 1: Clear gold_campus
print("\nStep 1: Clearing gold_campus...")
arcpy.management.TruncateTable(gold_campus)
print("   - gold_campus truncated")

# Step 2: Pairwise Dissolve by campus_id
print("\nStep 2: Dissolving buildings by campus_id...")

# Use geodatabase workspace instead of in_memory
dissolved_fc = os.path.join(gdb_path, "temp_dissolved_campus")

# Delete if exists from previous run
if arcpy.Exists(dissolved_fc):
    arcpy.management.Delete(dissolved_fc)
    print("   - Deleted existing temp_dissolved_campus")

# Check if new fields exist in gold_buildings
building_fields = [f.name for f in arcpy.ListFields(gold_buildings)]
has_cost_fields = all(f in building_fields for f in ['total_cost_usd_million', 'land_cost_usd_million', 
                                                       'total_site_acres', 'data_center_acres'])

# Check for year MW fields
has_year_fields = all(f in building_fields for f in [f'mw_{year}' for year in range(2023, 2033)])

# Define statistics fields
stats_fields = [
    ['company_clean', 'FIRST'],
    ['campus_name', 'FIRST'],
    ['city', 'FIRST'],
    ['market', 'FIRST'],
    ['state', 'FIRST'],
    ['state_abbr', 'FIRST'],
    ['county', 'FIRST'],
    ['country', 'FIRST'],
    ['region', 'FIRST'],
    ['postal_code', 'FIRST'],
    ['address', 'FIRST'],
    ['planned_power_mw', 'SUM'],
    ['uc_power_mw', 'SUM'],
    ['commissioned_power_mw', 'SUM'],
    ['full_capacity_mw', 'SUM'],
    ['facility_sqft', 'SUM'],
    ['whitespace_sqft', 'SUM'],
    ['actual_live_date', 'MIN'],
    ['status_rank_tmp', 'MIN'],
    ['cancelled', 'MAX'],
    ['pue', 'MEAN'],
    ['unique_id', 'COUNT']
]

# Add cost/acreage fields if they exist
if has_cost_fields:
    stats_fields.extend([
        ['total_cost_usd_million', 'SUM'],
        ['land_cost_usd_million', 'SUM'],
        ['total_site_acres', 'SUM'],
        ['data_center_acres', 'SUM']
    ])
    print("   - Including cost/acreage fields in dissolve")
else:
    print("   - Cost/acreage fields not found - skipping in dissolve")

# Add year MW fields if they exist (CRITICAL FOR SEMIANALYSIS DATA)
if has_year_fields:
    for year in range(2023, 2033):
        stats_fields.append([f'mw_{year}', 'SUM'])
    print("   - Including mw_2023-2032 year fields in dissolve")
else:
    print("   - Year MW fields not found - skipping in dissolve")

try:
    arcpy.analysis.PairwiseDissolve(
        in_features=gold_buildings,
        out_feature_class=dissolved_fc,
        dissolve_field=['campus_id'],
        statistics_fields=stats_fields,
        multi_part="MULTI_PART"
    )
    
    # Verify it was created
    if not arcpy.Exists(dissolved_fc):
        print("   ERROR: Dissolve failed - output not created")
        exit()
    
    dissolved_count = int(arcpy.management.GetCount(dissolved_fc)[0])
    print(f"   - Dissolve complete - {dissolved_count} campus polygons created")
    
except Exception as e:
    print(f"   ERROR during dissolve: {str(e)}")
    exit()

# Step 3: Feature To Point (INSIDE)
print("\nStep 3: Creating representative points...")

point_fc = os.path.join(gdb_path, "temp_campus_points")

# Delete if exists from previous run
if arcpy.Exists(point_fc):
    arcpy.management.Delete(point_fc)

try:
    arcpy.management.FeatureToPoint(
        in_features=dissolved_fc,
        out_feature_class=point_fc,
        point_location="INSIDE"
    )
    
    # Verify it was created
    if not arcpy.Exists(point_fc):
        print("   ERROR: FeatureToPoint failed - output not created")
        exit()
    
    point_count = int(arcpy.management.GetCount(point_fc)[0])
    print(f"   - Points created - {point_count} campus points")
    
except Exception as e:
    print(f"   ERROR during FeatureToPoint: {str(e)}")
    exit()

# Step 4: Map fields and insert into gold_campus
print("\nStep 4: Mapping fields to gold_campus schema...")

# Get dissolved fields
dissolved_fields = [f.name for f in arcpy.ListFields(point_fc)]
print(f"   - Point feature class has {len(dissolved_fields)} fields")

# Check if gold_campus has cost/acreage fields
campus_fields = [f.name for f in arcpy.ListFields(gold_campus)]
campus_has_cost_fields = all(f in campus_fields for f in ['total_cost_usd_million', 'land_cost_usd_million',
                                                            'total_site_acres', 'data_center_acres'])

# Check if gold_campus has year MW fields
campus_has_year_fields = all(f in campus_fields for f in [f'mw_{year}' for year in range(2023, 2033)])

# Define insert fields
insert_fields = [
    'SHAPE@', 'campus_id', 'company_clean', 'campus_name', 'city', 'market',
    'state', 'state_abbr', 'county', 'country', 'region', 'postal_code',
    'address', 'planned_power_mw', 'uc_power_mw', 'commissioned_power_mw',
    'full_capacity_mw', 'planned_plus_uc_mw', 'facility_sqft_sum',
    'whitespace_sqft_sum', 'building_count', 'first_live_date',
    'facility_status_agg', 'cancelled', 'pue_avg', 'record_level',
    'ingest_date'
]

# Add cost/acreage fields to insert if they exist in gold_campus
if campus_has_cost_fields:
    insert_fields.extend(['total_cost_usd_million', 'land_cost_usd_million',
                          'total_site_acres', 'data_center_acres'])
    print("   - Including cost/acreage fields in insert")
else:
    print("   - Cost/acreage fields not in gold_campus - need to add them first")

# Add year MW fields to insert if they exist in gold_campus
if campus_has_year_fields:
    for year in range(2023, 2033):
        insert_fields.append(f'mw_{year}')
    print("   - Including mw_2023-2032 fields in insert")
else:
    print("   - Year MW fields not in gold_campus - need to add them first")

# Status rank to status mapping
status_map = {
    1: 'Active',
    2: 'Under Construction',
    3: 'Permitting',
    4: 'Announced',
    5: 'Land Acquisition',
    6: 'Rumor',
    7: 'Unknown'
}

current_date = datetime.now()
campus_count = 0

# Helper function to safely get field value
def get_field_value(row, field_name, fields_list):
    """Safely get field value from row"""
    try:
        idx = fields_list.index(field_name)
        return row[idx + 1]  # +1 because SHAPE@ is position 0
    except (ValueError, IndexError):
        return None

try:
    with arcpy.da.SearchCursor(point_fc, ['SHAPE@'] + dissolved_fields) as s_cursor:
        with arcpy.da.InsertCursor(gold_campus, insert_fields) as i_cursor:
            
            for row in s_cursor:
                geom = row[0]
                
                # Extract dissolved stats
                campus_id = get_field_value(row, 'campus_id', dissolved_fields)
                company = get_field_value(row, 'FIRST_company_clean', dissolved_fields)
                campus_name = get_field_value(row, 'FIRST_campus_name', dissolved_fields)
                city = get_field_value(row, 'FIRST_city', dissolved_fields)
                market = get_field_value(row, 'FIRST_market', dissolved_fields)
                state = get_field_value(row, 'FIRST_state', dissolved_fields)
                state_abbr = get_field_value(row, 'FIRST_state_abbr', dissolved_fields)
                county = get_field_value(row, 'FIRST_county', dissolved_fields)
                country = get_field_value(row, 'FIRST_country', dissolved_fields)
                region = get_field_value(row, 'FIRST_region', dissolved_fields)
                postal = get_field_value(row, 'FIRST_postal_code', dissolved_fields)
                address = get_field_value(row, 'FIRST_address', dissolved_fields)
                
                # Capacity sums
                planned_mw = get_field_value(row, 'SUM_planned_power_mw', dissolved_fields)
                uc_mw = get_field_value(row, 'SUM_uc_power_mw', dissolved_fields)
                commissioned_mw = get_field_value(row, 'SUM_commissioned_power_mw', dissolved_fields)
                full_cap_mw = get_field_value(row, 'SUM_full_capacity_mw', dissolved_fields)
                
                # Area sums
                sqft_sum = get_field_value(row, 'SUM_facility_sqft', dissolved_fields)
                whitespace_sum = get_field_value(row, 'SUM_whitespace_sqft', dissolved_fields)
                
                # Aggregations
                building_count = get_field_value(row, 'COUNT_unique_id', dissolved_fields)
                first_live = get_field_value(row, 'MIN_actual_live_date', dissolved_fields)
                min_status_rank = get_field_value(row, 'MIN_status_rank_tmp', dissolved_fields)
                cancelled = get_field_value(row, 'MAX_cancelled', dissolved_fields)
                pue_avg = get_field_value(row, 'MEAN_pue', dissolved_fields)
                
                # Cost and acreage sums (if available)
                if has_cost_fields and campus_has_cost_fields:
                    total_cost = get_field_value(row, 'SUM_total_cost_usd_million', dissolved_fields)
                    land_cost = get_field_value(row, 'SUM_land_cost_usd_million', dissolved_fields)
                    site_acres = get_field_value(row, 'SUM_total_site_acres', dissolved_fields)
                    dc_acres = get_field_value(row, 'SUM_data_center_acres', dissolved_fields)
                else:
                    total_cost = None
                    land_cost = None
                    site_acres = None
                    dc_acres = None
                
                # Year MW values (if available)
                year_mw_values = []
                if has_year_fields and campus_has_year_fields:
                    for year in range(2023, 2033):
                        year_mw = get_field_value(row, f'SUM_mw_{year}', dissolved_fields)
                        year_mw_values.append(year_mw)
                
                # Calculate derived fields
                planned_plus_uc = (planned_mw or 0) + (uc_mw or 0) if (planned_mw or uc_mw) else None
                facility_status = status_map.get(int(min_status_rank) if min_status_rank else 7, 'Unknown')
                
                # Build insert row
                insert_row = [
                    geom,                # SHAPE@
                    campus_id,           # campus_id
                    company,             # company_clean
                    campus_name,         # campus_name
                    city,                # city
                    market,              # market
                    state,               # state
                    state_abbr,          # state_abbr
                    county,              # county
                    country,             # country
                    region,              # region
                    postal,              # postal_code
                    address,             # address
                    planned_mw,          # planned_power_mw
                    uc_mw,               # uc_power_mw
                    commissioned_mw,     # commissioned_power_mw
                    full_cap_mw,         # full_capacity_mw
                    planned_plus_uc,     # planned_plus_uc_mw
                    sqft_sum,            # facility_sqft_sum
                    whitespace_sum,      # whitespace_sqft_sum
                    building_count,      # building_count
                    first_live,          # first_live_date
                    facility_status,     # facility_status_agg
                    cancelled,           # cancelled
                    pue_avg,             # pue_avg
                    'Campus',            # record_level
                    current_date         # ingest_date
                ]
                
                # Add cost/acreage if fields exist
                if campus_has_cost_fields:
                    insert_row.extend([total_cost, land_cost, site_acres, dc_acres])
                
                # Add year MW values if fields exist
                if campus_has_year_fields:
                    insert_row.extend(year_mw_values)
                
                i_cursor.insertRow(insert_row)
                campus_count += 1
    
    print(f"   - Inserted {campus_count} campus records")

except Exception as e:
    print(f"   ERROR during insert: {str(e)}")
    import traceback
    traceback.print_exc()
    exit()

# Step 5: Cleanup temp layers
print("\nStep 5: Cleaning up temporary layers...")
try:
    if arcpy.Exists(dissolved_fc):
        arcpy.management.Delete(dissolved_fc)
        print("   - Deleted temp_dissolved_campus")
    if arcpy.Exists(point_fc):
        arcpy.management.Delete(point_fc)
        print("   - Deleted temp_campus_points")
except Exception as e:
    print(f"   Warning during cleanup: {str(e)}")

print("\n" + "="*80)
print(f"CAMPUS ROLLUP COMPLETE")
print(f"   - gold_campus: {campus_count} records")
if campus_has_cost_fields:
    print(f"   - Cost/acreage fields aggregated via SUM")
if campus_has_year_fields:
    print(f"   - Year MW fields (2023-2032) aggregated via SUM")
print("="*80)
