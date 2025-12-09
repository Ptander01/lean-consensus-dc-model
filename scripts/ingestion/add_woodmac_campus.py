import arcpy
import os

gdb_path = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
gold_campus = os.path.join(gdb_path, "gold_campus")

print("="*80)
print("ADDING COST/ACREAGE FIELDS TO GOLD_CAMPUS")
print("="*80)

# Check current fields
existing_fields = [f.name for f in arcpy.ListFields(gold_campus)]
print(f"\nCurrent gold_campus field count: {len(existing_fields)}")

# Define fields to add (matching gold_buildings)
new_fields = [
    ('total_cost_usd_million', 'DOUBLE', None, None, 'Total Project Cost (USD Millions)'),
    ('land_cost_usd_million', 'DOUBLE', None, None, 'Land Acquisition Cost (USD Millions)'),
    ('total_site_acres', 'DOUBLE', None, None, 'Total Site Acreage'),
    ('data_center_acres', 'DOUBLE', None, None, 'Data Center Footprint Acreage')
]

print("\nAdding fields to gold_campus...")
for field_def in new_fields:
    field_name = field_def[0]
    
    if field_name not in existing_fields:
        arcpy.management.AddField(
            in_table=gold_campus,
            field_name=field_name,
            field_type=field_def[1],
            field_length=field_def[2],
            field_alias=field_def[4]
        )
        print(f"  ✓ Added: {field_name}")
    else:
        print(f"  ⚠ Already exists: {field_name}")

print("\n✅ gold_campus schema updated!")
print("="*80)