import arcpy
import pandas as pd
from datetime import datetime

# ============================================================================
# IMPORT META CANONICAL DATA - ALL STATUSES
# ============================================================================

print("="*80)
print("IMPORTING META CANONICAL DATA (ALL STATUSES)")
print("="*80)

# File paths
excel_file = r"C:\Users\ptanderson\Downloads\SAMPLE-idc_schedule_udm_consumption_table (1).xlsx"
gdb = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
canonical_fc = f"{gdb}\\meta_canonical"

# Step 1: Read Excel file
print("\nStep 1: Reading Excel file...")
df = pd.read_excel(excel_file)
print(f"   - Total rows in Excel: {len(df)}")

# Step 2: Filter statuses (BOTH Active Build and Complete Build)
print("\nStep 2: Checking build statuses...")
status_counts = df['new_build_status'].value_counts()
for status, count in status_counts.items():
    print(f"   - {status}: {count} rows")

# Include both statuses for comprehensive comparison
df_filtered = df[df['new_build_status'].isin(['Active Build', 'Complete Build'])].copy()
print(f"   - Total after filtering: {len(df_filtered)} rows")

# Step 3: Remove rows with missing lat/lon
print("\nStep 3: Removing rows with missing coordinates...")
df_filtered = df_filtered.dropna(subset=['latitude', 'longitude'])
print(f"   - Rows with valid coordinates: {len(df_filtered)}")

# Step 4: Deduplicate by location_key (keep first occurrence)
print("\nStep 4: Deduplicating by location_key...")
print(f"   - Before deduplication: {len(df_filtered)} rows")
df_unique = df_filtered.drop_duplicates(subset=['location_key'], keep='first')
print(f"   - After deduplication: {len(df_unique)} unique locations")
print(f"   - Removed {len(df_filtered) - len(df_unique)} duplicate milestone entries")

# Step 5: Fix field naming (region = datacenter code, datacenter = building number)
print("\nStep 5: Renaming fields for clarity...")
df_unique = df_unique.rename(columns={
    'region': 'datacenter_code',      # CHY, DKL
    'datacenter': 'building_number',  # 1, 2, 3, 5
})
print("   - Renamed 'region' → 'datacenter_code' (CHY, DKL)")
print("   - Renamed 'datacenter' → 'building_number' (1, 2, 3, 5)")

# Step 6: Create feature class
print("\nStep 6: Creating feature class...")

# Delete if exists
if arcpy.Exists(canonical_fc):
    arcpy.management.Delete(canonical_fc)
    print("   - Deleted existing meta_canonical feature class")

# Create new feature class
arcpy.management.CreateFeatureclass(
    out_path=gdb,
    out_name="meta_canonical",
    geometry_type="POINT",
    spatial_reference=arcpy.SpatialReference(4326)  # WGS84
)

# Add fields
print("\nStep 7: Adding fields...")
fields_to_add = [
    ['location_key', 'TEXT', 50, 'Location Key (CHY5A, DKL1B, etc.)'],
    ['datacenter_code', 'TEXT', 20, 'Datacenter Code (CHY, DKL)'],
    ['building_number', 'TEXT', 10, 'Building Number (1, 2, 3, 5)'],
    ['suite', 'TEXT', 10, 'Suite ID (A, B, C, D)'],
    ['building_type', 'TEXT', 20, 'Building Type (own)'],
    ['new_build_status', 'TEXT', 50, 'Build Status (Active/Complete)'],
    ['address', 'TEXT', 200, 'Address'],
    ['it_load_mw', 'DOUBLE', None, 'IT Load (MW)'],
    ['dc_design_type', 'TEXT', 10, 'Design Type (F, T, etc.)'],
    ['dc_product_type', 'TEXT', 50, 'Product Type'],
    ['milestone_name', 'TEXT', 200, 'Sample Milestone'],
    ['milestone_date', 'DATE', None, 'Sample Milestone Date'],
    ['project_p6_id', 'TEXT', 50, 'P6 Project ID'],
    ['ingest_date', 'DATE', None, 'Import Date']
]

for field_info in fields_to_add:
    arcpy.management.AddField(
        in_table=canonical_fc,
        field_name=field_info[0],
        field_type=field_info[1],
        field_length=field_info[2] if field_info[1] == 'TEXT' else None,
        field_alias=field_info[3]
    )

# Step 8: Insert records
print("\nStep 8: Inserting records...")
insert_fields = ['SHAPE@XY', 'location_key', 'datacenter_code', 'building_number', 'suite',
                 'building_type', 'new_build_status', 'address', 'it_load_mw', 'dc_design_type',
                 'dc_product_type', 'milestone_name', 'milestone_date', 'project_p6_id', 'ingest_date']

current_date = datetime.now()
insert_count = 0
active_count = 0
complete_count = 0

with arcpy.da.InsertCursor(canonical_fc, insert_fields) as cursor:
    for idx, row in df_unique.iterrows():
        try:
            lat = float(row['latitude'])
            lon = float(row['longitude'])

            # Parse milestone_date if it exists
            milestone_date = None
            if pd.notna(row.get('milestone_date')):
                try:
                    milestone_date = pd.to_datetime(row['milestone_date'])
                except:
                    pass

            # Track status counts
            if row.get('new_build_status') == 'Active Build':
                active_count += 1
            elif row.get('new_build_status') == 'Complete Build':
                complete_count += 1

            cursor.insertRow([
                (lon, lat),  # SHAPE@XY
                row.get('location_key'),
                row.get('datacenter_code'),
                str(row.get('building_number')),  # Convert to string
                row.get('suite'),
                row.get('building_type'),
                row.get('new_build_status'),
                row.get('address'),
                row.get('it_load'),  # Already in MW!
                row.get('dc_design_type'),
                row.get('dc_product_type'),
                row.get('milestone_name'),
                milestone_date,
                row.get('project_p6_id'),
                current_date
            ])
            insert_count += 1
        except Exception as e:
            print(f"   - Warning: Skipped row {idx}: {str(e)}")

print(f"   - Inserted {insert_count} unique locations")
print(f"     • Active Build: {active_count}")
print(f"     • Complete Build: {complete_count}")

# Step 9: Summary statistics
print("\n" + "="*80)
print("IMPORT COMPLETE")
print("="*80)
print(f"Total unique Meta locations: {insert_count}")
print(f"  • Active Build (under construction): {active_count}")
print(f"  • Complete Build (operational): {complete_count}")
print(f"\nFeature class created: {canonical_fc}")

# Show breakdown by datacenter
print("\nLocations by Datacenter:")
dc_counts = df_unique['datacenter_code'].value_counts()
for dc, count in dc_counts.items():
    print(f"  {dc}: {count} locations")

# Show breakdown by building within each datacenter
print("\nLocations by Building:")
building_counts = df_unique.groupby(['datacenter_code', 'building_number']).size()
for (dc, bldg), count in building_counts.items():
    print(f"  {dc} Building {bldg}: {count} suites")

# Capacity stats
total_mw = df_unique['it_load'].sum()
avg_mw = df_unique['it_load'].mean()
print(f"\nCapacity Statistics:")
print(f"  Total IT Load: {total_mw:.1f} MW")
print(f"  Average per location: {avg_mw:.1f} MW")
print(f"  Min: {df_unique['it_load'].min():.1f} MW")
print(f"  Max: {df_unique['it_load'].max():.1f} MW")

print("="*80)
