import arcpy
import os
from datetime import datetime

gdb = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
meta_canonical = os.path.join(gdb, "meta_canonical_v2")
meta_buildings = os.path.join(gdb, "meta_canonical_buildings")

print("="*80)
print("CREATE BUILDING-LEVEL META CANONICAL (CORRECTED)")
print("="*80)
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# ============================================================================
# STEP 1: Add building_key field to meta_canonical_v2
# ============================================================================
print("STEP 1: Adding building_key field to meta_canonical_v2...")

fields = [f.name for f in arcpy.ListFields(meta_canonical)]

if "building_key" not in fields:
    print("   Adding building_key field...")
    arcpy.management.AddField(
        in_table=meta_canonical,
        field_name="building_key",
        field_type="TEXT",
        field_length=50,
        field_alias="Building Key (dc_code + datacenter)"
    )
    print("   ✅ Field added")
else:
    print("   ℹ️  building_key field already exists")

# ============================================================================
# STEP 2: Calculate building_key = dc_code + "-" + datacenter
# ============================================================================
print("\nSTEP 2: Calculating building_key values...")

count_updated = 0
count_null = 0

with arcpy.da.UpdateCursor(meta_canonical,
                          ["dc_code", "datacenter", "building_key"]) as cursor:
    for row in cursor:
        dc_code = row[0]
        datacenter = row[1]

        if dc_code and datacenter:
            # Create composite key
            row[2] = f"{dc_code}-{datacenter}"
            cursor.updateRow(row)
            count_updated += 1
        else:
            # Handle missing values
            count_null += 1

print(f"   ✅ Updated {count_updated:,} records with building_key")
if count_null > 0:
    print(f"   ⚠️  {count_null:,} records missing dc_code or datacenter (building_key = NULL)")

# ============================================================================
# STEP 3: Validate unique building count
# ============================================================================
print("\nSTEP 3: Validating unique buildings...")

unique_buildings = set()
valid_buildings = set()

with arcpy.da.SearchCursor(meta_canonical,
                          ["building_key", "has_coordinates"]) as cursor:
    for row in cursor:
        if row[0]:  # building_key exists
            unique_buildings.add(row[0])
            if row[1] == 1:  # has valid coordinates
                valid_buildings.add(row[0])

print(f"   Total unique buildings (all): {len(unique_buildings)}")
print(f"   Unique buildings (has_coordinates=1): {len(valid_buildings)}")
print(f"   Expected: 276 ✅" if len(valid_buildings) == 276 else f"   ⚠️ Expected 276, got {len(valid_buildings)}")

# ============================================================================
# STEP 4: Create filtered layer (has_coordinates = 1)
# ============================================================================
print("\nSTEP 4: Creating filtered layer for buildings with valid coordinates...")

meta_layer = arcpy.management.MakeFeatureLayer(
    in_features=meta_canonical,
    out_layer="meta_valid_coords",
    where_clause="has_coordinates = 1"
)

layer_count = int(arcpy.management.GetCount(meta_layer)[0])
print(f"   ✅ Filtered layer: {layer_count:,} suites with valid coordinates")

# ============================================================================
# STEP 5: Dissolve to building-level (grouped by building_key)
# ============================================================================
print("\nSTEP 5: Dissolving suites to building-level...")

print("   Dissolve parameters:")
print(f"      Input: {layer_count:,} suite records")
print(f"      Dissolve field: building_key")
print(f"      Output: {os.path.basename(meta_buildings)}")

arcpy.management.Dissolve(
    in_features=meta_layer,
    out_feature_class=meta_buildings,
    dissolve_field="building_key",
    statistics_fields=[
        ["location_key", "COUNT"],      # Count suites per building
        ["dc_code", "FIRST"],           # Preserve campus code
        ["datacenter", "FIRST"],        # Preserve building number
        ["region_derived", "FIRST"],    # Preserve region
        ["new_build_status", "FIRST"],  # Preserve build status
        ["it_load", "SUM"]              # Sum IT load across suites
    ],
    multi_part="SINGLE_PART"
)

building_count = int(arcpy.management.GetCount(meta_buildings)[0])
print(f"   ✅ Created {building_count} building records")

if building_count != 276:
    print(f"   ⚠️ WARNING: Expected 276 buildings, got {building_count}")

# ============================================================================
# STEP 6: Rename dissolved fields for clarity
# ============================================================================
print("\nSTEP 6: Renaming dissolved fields...")

field_renames = {
    "COUNT_location_key": "suite_count",
    "FIRST_dc_code": "dc_code",
    "FIRST_datacenter": "datacenter",
    "FIRST_region_derived": "region_derived",
    "FIRST_new_build_status": "new_build_status",
    "SUM_it_load": "it_load_total"
}

for old_name, new_name in field_renames.items():
    existing_fields = [f.name for f in arcpy.ListFields(meta_buildings)]

    if old_name in existing_fields:
        try:
            arcpy.management.AlterField(
                in_table=meta_buildings,
                field=old_name,
                new_field_name=new_name,
                new_field_alias=new_name.replace("_", " ").title()
            )
            print(f"   ✅ Renamed: {old_name} → {new_name}")
        except Exception as e:
            print(f"   ⚠️ Could not rename {old_name}: {e}")
    else:
        print(f"   ℹ️  Field {old_name} not found (may already be renamed)")

# ============================================================================
# STEP 7: Validation - Check regional distribution
# ============================================================================
print("\nSTEP 7: Validating regional distribution...")

regions = {}
statuses = {}

with arcpy.da.SearchCursor(meta_buildings,
                          ["region_derived", "new_build_status", "suite_count"]) as cursor:
    for row in cursor:
        region = row[0] if row[0] else "NULL"
        status = row[1] if row[1] else "NULL"

        regions[region] = regions.get(region, 0) + 1
        statuses[status] = statuses.get(status, 0) + 1

print(f"\n   Regional distribution:")
for region, count in sorted(regions.items()):
    print(f"      {region}: {count} buildings")

print(f"\n   Build status distribution:")
for status, count in sorted(statuses.items()):
    print(f"      {status}: {count} buildings")

# Expected from context
expected_regions = {
    "AMER": 251,
    "APAC": 5,
    "EMEA": 20
}

print(f"\n   Validation against expected counts:")
all_match = True
for region, expected in expected_regions.items():
    actual = regions.get(region, 0)
    match = "✅" if actual == expected else "⚠️"
    print(f"      {match} {region}: {actual} (expected {expected})")
    if actual != expected:
        all_match = False

# ============================================================================
# STEP 8: Sample building keys
# ============================================================================
print("\nSTEP 8: Sample building records...")

sample_buildings = []
with arcpy.da.SearchCursor(meta_buildings,
                          ["building_key", "dc_code", "datacenter",
                           "suite_count", "region_derived"],
                          sql_clause=(None, "ORDER BY building_key")) as cursor:
    for i, row in enumerate(cursor):
        if i < 10:
            sample_buildings.append(row)
        else:
            break

print(f"\n   First 10 buildings:")
print(f"   {'Building Key':<15} {'Campus':<10} {'Bldg#':<6} {'Suites':<7} {'Region':<10}")
print(f"   {'-'*60}")
for bkey, dc, datacenter, suites, region in sample_buildings:
    print(f"   {bkey:<15} {dc:<10} {datacenter:<6} {suites:<7} {region:<10}")

# ============================================================================
# COMPLETION
# ============================================================================
print("\n" + "="*80)
print("✅ BUILDING-LEVEL META CANONICAL CREATED!")
print("="*80)
print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

print(f"📊 Summary:")
print(f"   Input: {layer_count:,} suites (has_coordinates=1)")
print(f"   Output: {building_count} buildings")
print(f"   Feature class: {meta_buildings}")

if building_count == 276 and all_match:
    print(f"\n✅ All validation checks passed!")
    print(f"\n🚀 Next step: Run spatial join with meta_canonical_buildings")
    print(f"   Update target_features to: {os.path.basename(meta_buildings)}")
else:
    print(f"\n⚠️ Some validation checks failed - review output above")

print("="*80)
