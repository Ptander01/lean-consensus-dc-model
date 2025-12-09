import arcpy
import os

# Path setup
gdb_path = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
gold_buildings = os.path.join(gdb_path, "gold_buildings")

# Add ONLY cost and acreage fields (quantitative, filterable)
new_fields = [
    ('total_cost_usd_million', 'DOUBLE', None, None, 'Total Project Cost (USD Millions)'),
    ('land_cost_usd_million', 'DOUBLE', None, None, 'Land Acquisition Cost (USD Millions)'),
    ('total_site_acres', 'DOUBLE', None, None, 'Total Site Acreage'),
    ('data_center_acres', 'DOUBLE', None, None, 'Data Center Footprint Acreage')
]

print("="*80)
print("ADDING MINIMAL SCHEMA EXTENSION (4 FIELDS)")
print("="*80)
print("\nAdding cost and acreage fields to gold_buildings schema...")
print("(All other WoodMac metadata will go into existing 'notes' field)")
print("-" * 80)

for field_def in new_fields:
    field_name = field_def[0]
    
    # Check if field already exists
    existing_fields = [f.name for f in arcpy.ListFields(gold_buildings)]
    if field_name not in existing_fields:
        arcpy.management.AddField(
            in_table=gold_buildings,
            field_name=field_name,
            field_type=field_def[1],
            field_length=field_def[2],
            field_alias=field_def[4]
        )
        print(f"  ✓ Added: {field_name}")
    else:
        print(f"  ⚠ Already exists: {field_name}")

print("\n✅ Schema extension complete!")
print("="*80)