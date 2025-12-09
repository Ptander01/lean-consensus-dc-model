# CAPACITY DATA DIAGNOSTIC - FIXED
import arcpy
import pandas as pd
from datetime import datetime

# Configuration
gdb_path = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
gold_campus_fc = f"{gdb_path}\\gold_campus"
gold_buildings_fc = f"{gdb_path}\\gold_buildings"

print("="*80)
print(f"CAPACITY DATA DIAGNOSTIC")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# ============================================================================
# Check gold_campus schema
# ============================================================================

print("\n" + "="*80)
print("GOLD_CAMPUS SCHEMA")
print("="*80)

print("\n📋 All Fields:")
campus_fields = arcpy.ListFields(gold_campus_fc)
field_names = [f.name for f in campus_fields]

for f in campus_fields:
    print(f"   {f.name} ({f.type}, length={f.length if f.type == 'String' else 'N/A'})")

# Check for capacity-related fields
capacity_fields = [f.name for f in campus_fields if 'capacity' in f.name.lower() or 'mw' in f.name.lower()]
print(f"\n🔋 Capacity-related fields found: {len(capacity_fields)}")
if len(capacity_fields) > 0:
    for field in capacity_fields:
        print(f"   • {field}")
else:
    print("   ❌ NO capacity fields found!")

# ============================================================================
# Sample data from gold_campus
# ============================================================================

print("\n" + "="*80)
print("GOLD_CAMPUS SAMPLE DATA (First 10 records)")
print("="*80)

# Build sample fields list from what actually exists
sample_fields = ['campus_id', 'campus_name', 'source', 'company_clean',
                 'building_count', 'city', 'state']

# Only use fields that exist
sample_fields = [f for f in sample_fields if f in field_names]

# Add MW forecast fields if they exist
for year in range(2023, 2033):
    field_name = f'mw_{year}'
    if field_name in field_names:
        sample_fields.append(field_name)

sample_data = []
with arcpy.da.SearchCursor(gold_campus_fc, sample_fields) as cursor:
    for i, row in enumerate(cursor):
        if i >= 10:  # First 10 records
            break
        sample_data.append(row)

df_sample = pd.DataFrame(sample_data, columns=sample_fields)

print(f"\n📊 Sample records ({len(df_sample)}):")
print("\n" + df_sample.to_string(index=False))

# ============================================================================
# Check capacity data completeness in gold_campus
# ============================================================================

print("\n" + "="*80)
print("GOLD_CAMPUS CAPACITY DATA COMPLETENESS")
print("="*80)

# Check if total_capacity_mw exists
if 'total_capacity_mw' in field_names:
    print("\n✅ Field 'total_capacity_mw' EXISTS")

    # Read all data
    all_fields = ['source', 'company_clean', 'total_capacity_mw']
    all_data = []
    with arcpy.da.SearchCursor(gold_campus_fc, all_fields) as cursor:
        for row in cursor:
            all_data.append(row)

    df_all = pd.DataFrame(all_data, columns=all_fields)

    # Filter to Meta
    if 'company_clean' in df_all.columns:
        df_meta = df_all[df_all['company_clean'] == 'Meta']
    else:
        df_meta = df_all

    print(f"\n📊 Total Meta campus records: {len(df_meta)}")

    total_with_capacity = df_meta['total_capacity_mw'].notna().sum()
    total_without_capacity = df_meta['total_capacity_mw'].isna().sum()

    print(f"\n🔋 Capacity Data:")
    print(f"   Records WITH capacity: {total_with_capacity} ({total_with_capacity/len(df_meta)*100:.1f}%)")
    print(f"   Records WITHOUT capacity: {total_without_capacity} ({total_without_capacity/len(df_meta)*100:.1f}%)")

    if total_with_capacity > 0:
        print(f"\n   Capacity Statistics (non-null only):")
        print(f"   Mean: {df_meta['total_capacity_mw'].mean():.1f} MW")
        print(f"   Median: {df_meta['total_capacity_mw'].median():.1f} MW")
        print(f"   Min: {df_meta['total_capacity_mw'].min():.1f} MW")
        print(f"   Max: {df_meta['total_capacity_mw'].max():.1f} MW")
        print(f"   Total: {df_meta['total_capacity_mw'].sum():.1f} MW")

        # By source
        if 'source' in df_meta.columns:
            print(f"\n📊 By Source:")
            for source in df_meta['source'].unique():
                if pd.isna(source):
                    continue
                source_data = df_meta[df_meta['source'] == source]
                with_cap = source_data['total_capacity_mw'].notna().sum()
                total = len(source_data)
                print(f"   {source}: {with_cap}/{total} ({with_cap/total*100:.1f}%) have capacity data")
    else:
        print("\n   ❌ ALL records have NULL capacity!")
else:
    print("\n❌ Field 'total_capacity_mw' DOES NOT EXIST in gold_campus")
    print("\n   Available fields with 'mw' or 'capacity':")
    if len(capacity_fields) > 0:
        for field in capacity_fields:
            print(f"      • {field}")
    else:
        print("      (none)")

# ============================================================================
# Check gold_buildings for comparison
# ============================================================================

print("\n" + "="*80)
print("GOLD_BUILDINGS CAPACITY DATA (for comparison)")
print("="*80)

buildings_fields = arcpy.ListFields(gold_buildings_fc)
buildings_field_names = [f.name for f in buildings_fields]
buildings_capacity_fields = [f.name for f in buildings_fields if 'capacity' in f.name.lower() or 'mw' in f.name.lower()]

print(f"\n🔋 Capacity fields in gold_buildings:")
for field in buildings_capacity_fields:
    print(f"   • {field}")

# Check data completeness
building_read_fields = ['source', 'company_clean']
if 'full_capacity_mw' in buildings_field_names:
    building_read_fields.append('full_capacity_mw')
elif 'capacity_mw' in buildings_field_names:
    building_read_fields.append('capacity_mw')
else:
    # Use first MW field found
    if len(buildings_capacity_fields) > 0:
        building_read_fields.append(buildings_capacity_fields[0])

building_data = []
with arcpy.da.SearchCursor(gold_buildings_fc, building_read_fields) as cursor:
    for row in cursor:
        building_data.append(row)

df_buildings = pd.DataFrame(building_data, columns=building_read_fields)

if 'company_clean' in df_buildings.columns:
    df_buildings_meta = df_buildings[df_buildings['company_clean'] == 'Meta']
else:
    df_buildings_meta = df_buildings

print(f"\n📊 Total Meta building records: {len(df_buildings_meta)}")

# Find capacity column
capacity_col = None
if 'full_capacity_mw' in df_buildings_meta.columns:
    capacity_col = 'full_capacity_mw'
elif 'capacity_mw' in df_buildings_meta.columns:
    capacity_col = 'capacity_mw'
elif len(buildings_capacity_fields) > 0 and buildings_capacity_fields[0] in df_buildings_meta.columns:
    capacity_col = buildings_capacity_fields[0]

if capacity_col:
    total_with_capacity = df_buildings_meta[capacity_col].notna().sum()
    total_without_capacity = df_buildings_meta[capacity_col].isna().sum()

    print(f"\n🔋 Building-Level Capacity Data (field: {capacity_col}):")
    print(f"   Records WITH capacity: {total_with_capacity} ({total_with_capacity/len(df_buildings_meta)*100:.1f}%)")
    print(f"   Records WITHOUT capacity: {total_without_capacity} ({total_without_capacity/len(df_buildings_meta)*100:.1f}%)")

    if total_with_capacity > 0:
        print(f"\n   Overall Statistics:")
        print(f"   Mean: {df_buildings_meta[capacity_col].mean():.1f} MW")
        print(f"   Median: {df_buildings_meta[capacity_col].median():.1f} MW")
        print(f"   Total: {df_buildings_meta[capacity_col].sum():.1f} MW")

        print(f"\n   By Source:")
        for source in df_buildings_meta['source'].unique():
            if pd.isna(source):
                continue
            source_data = df_buildings_meta[df_buildings_meta['source'] == source]
            with_cap = source_data[capacity_col].notna().sum()
            total = len(source_data)
            avg_cap = source_data[capacity_col].mean()
            if pd.notna(avg_cap):
                print(f"   {source}: {with_cap}/{total} ({with_cap/total*100:.1f}%) - Avg: {avg_cap:.1f} MW")
            else:
                print(f"   {source}: {with_cap}/{total} ({with_cap/total*100:.1f}%) - Avg: N/A")
else:
    print("\n❌ No capacity field found in gold_buildings!")

# ============================================================================
# Recommendation
# ============================================================================

print("\n" + "="*80)
print("DIAGNOSIS & RECOMMENDATION")
print("="*80)

if 'total_capacity_mw' not in field_names:
    print("\n❌ CRITICAL ISSUE: gold_campus.total_capacity_mw field DOES NOT EXIST")
    print("\n💡 SOLUTION OPTIONS:")
    print("\n   OPTION A: Re-run campus rollup script (Campus_Rollup_new.py)")
    print("      - Check if it creates total_capacity_mw field")
    print("      - If not, may need to modify script")

    print("\n   OPTION B: Manually add and populate field")
    print("      Step 1: Add field:")
    print("         arcpy.management.AddField(gold_campus_fc, 'total_capacity_mw', 'DOUBLE')")
    print("\n      Step 2: Aggregate from gold_buildings:")
    print("         - Group by campus_id")
    print("         - Sum full_capacity_mw")
    print("         - Update gold_campus records")

    print("\n   OPTION C: Use building-level data for validation")
    print("      - Aggregate gold_buildings to campus level on-the-fly")
    print("      - Compare to Meta canonical campus aggregation")
    print("      - This is what capacity_validation script will do automatically")

elif 'total_capacity_mw' in field_names:
    # Already checked above, but add final recommendation
    all_fields_check = ['company_clean', 'total_capacity_mw']
    check_data = []
    with arcpy.da.SearchCursor(gold_campus_fc, all_fields_check) as cursor:
        for row in cursor:
            check_data.append(row)
    df_check = pd.DataFrame(check_data, columns=all_fields_check)
    df_check_meta = df_check[df_check['company_clean'] == 'Meta']

    if df_check_meta['total_capacity_mw'].notna().sum() == 0:
        print("\n❌ ISSUE: gold_campus.total_capacity_mw exists but ALL values are NULL")
        print("\n💡 SOLUTION: Re-run campus rollup or manually populate")
    elif df_check_meta['total_capacity_mw'].notna().sum() < len(df_check_meta) * 0.5:
        print(f"\n⚠️ WARNING: Only {df_check_meta['total_capacity_mw'].notna().sum()/len(df_check_meta)*100:.1f}% have capacity")
        print("\n💡 SOLUTION: Investigate which sources missing data, supplement from buildings")
    else:
        print(f"\n✅ GOOD: {df_check_meta['total_capacity_mw'].notna().sum()/len(df_check_meta)*100:.1f}% have capacity")
        print("\n💡 Ready for capacity validation!")

print("\n" + "="*80)
print("NEXT STEPS")
print("="*80)

if capacity_col and 'source' in df_buildings_meta.columns:
    print("\n✅ CAN PROCEED WITH CAPACITY VALIDATION")
    print("   - Will use gold_buildings aggregated to campus level")
    print("   - Run: capacity_validation_campus_level.py")
    print("   - Script will handle aggregation automatically")
else:
    print("\n⚠️ CANNOT PROCEED WITH CAPACITY VALIDATION")
    print("   - No capacity data available in either gold_campus or gold_buildings")
    print("   - Need to investigate source data")

print("\n" + "="*80)
print(f"DIAGNOSTIC COMPLETE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)
# CAPACITY DATA DIAGNOSTIC - FIXED
import arcpy
import pandas as pd
from datetime import datetime

# Configuration
gdb_path = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
gold_campus_fc = f"{gdb_path}\\gold_campus"
gold_buildings_fc = f"{gdb_path}\\gold_buildings"

print("="*80)
print(f"CAPACITY DATA DIAGNOSTIC")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# ============================================================================
# Check gold_campus schema
# ============================================================================

print("\n" + "="*80)
print("GOLD_CAMPUS SCHEMA")
print("="*80)

print("\n📋 All Fields:")
campus_fields = arcpy.ListFields(gold_campus_fc)
field_names = [f.name for f in campus_fields]

for f in campus_fields:
    print(f"   {f.name} ({f.type}, length={f.length if f.type == 'String' else 'N/A'})")

# Check for capacity-related fields
capacity_fields = [f.name for f in campus_fields if 'capacity' in f.name.lower() or 'mw' in f.name.lower()]
print(f"\n🔋 Capacity-related fields found: {len(capacity_fields)}")
if len(capacity_fields) > 0:
    for field in capacity_fields:
        print(f"   • {field}")
else:
    print("   ❌ NO capacity fields found!")

# ============================================================================
# Sample data from gold_campus
# ============================================================================

print("\n" + "="*80)
print("GOLD_CAMPUS SAMPLE DATA (First 10 records)")
print("="*80)

# Build sample fields list from what actually exists
sample_fields = ['campus_id', 'campus_name', 'source', 'company_clean',
                 'building_count', 'city', 'state']

# Only use fields that exist
sample_fields = [f for f in sample_fields if f in field_names]

# Add MW forecast fields if they exist
for year in range(2023, 2033):
    field_name = f'mw_{year}'
    if field_name in field_names:
        sample_fields.append(field_name)

sample_data = []
with arcpy.da.SearchCursor(gold_campus_fc, sample_fields) as cursor:
    for i, row in enumerate(cursor):
        if i >= 10:  # First 10 records
            break
        sample_data.append(row)

df_sample = pd.DataFrame(sample_data, columns=sample_fields)

print(f"\n📊 Sample records ({len(df_sample)}):")
print("\n" + df_sample.to_string(index=False))

# ============================================================================
# Check capacity data completeness in gold_campus
# ============================================================================

print("\n" + "="*80)
print("GOLD_CAMPUS CAPACITY DATA COMPLETENESS")
print("="*80)

# Check if total_capacity_mw exists
if 'total_capacity_mw' in field_names:
    print("\n✅ Field 'total_capacity_mw' EXISTS")

    # Read all data
    all_fields = ['source', 'company_clean', 'total_capacity_mw']
    all_data = []
    with arcpy.da.SearchCursor(gold_campus_fc, all_fields) as cursor:
        for row in cursor:
            all_data.append(row)

    df_all = pd.DataFrame(all_data, columns=all_fields)

    # Filter to Meta
    if 'company_clean' in df_all.columns:
        df_meta = df_all[df_all['company_clean'] == 'Meta']
    else:
        df_meta = df_all

    print(f"\n📊 Total Meta campus records: {len(df_meta)}")

    total_with_capacity = df_meta['total_capacity_mw'].notna().sum()
    total_without_capacity = df_meta['total_capacity_mw'].isna().sum()

    print(f"\n🔋 Capacity Data:")
    print(f"   Records WITH capacity: {total_with_capacity} ({total_with_capacity/len(df_meta)*100:.1f}%)")
    print(f"   Records WITHOUT capacity: {total_without_capacity} ({total_without_capacity/len(df_meta)*100:.1f}%)")

    if total_with_capacity > 0:
        print(f"\n   Capacity Statistics (non-null only):")
        print(f"   Mean: {df_meta['total_capacity_mw'].mean():.1f} MW")
        print(f"   Median: {df_meta['total_capacity_mw'].median():.1f} MW")
        print(f"   Min: {df_meta['total_capacity_mw'].min():.1f} MW")
        print(f"   Max: {df_meta['total_capacity_mw'].max():.1f} MW")
        print(f"   Total: {df_meta['total_capacity_mw'].sum():.1f} MW")

        # By source
        if 'source' in df_meta.columns:
            print(f"\n📊 By Source:")
            for source in df_meta['source'].unique():
                if pd.isna(source):
                    continue
                source_data = df_meta[df_meta['source'] == source]
                with_cap = source_data['total_capacity_mw'].notna().sum()
                total = len(source_data)
                print(f"   {source}: {with_cap}/{total} ({with_cap/total*100:.1f}%) have capacity data")
    else:
        print("\n   ❌ ALL records have NULL capacity!")
else:
    print("\n❌ Field 'total_capacity_mw' DOES NOT EXIST in gold_campus")
    print("\n   Available fields with 'mw' or 'capacity':")
    if len(capacity_fields) > 0:
        for field in capacity_fields:
            print(f"      • {field}")
    else:
        print("      (none)")

# ============================================================================
# Check gold_buildings for comparison
# ============================================================================

print("\n" + "="*80)
print("GOLD_BUILDINGS CAPACITY DATA (for comparison)")
print("="*80)

buildings_fields = arcpy.ListFields(gold_buildings_fc)
buildings_field_names = [f.name for f in buildings_fields]
buildings_capacity_fields = [f.name for f in buildings_fields if 'capacity' in f.name.lower() or 'mw' in f.name.lower()]

print(f"\n🔋 Capacity fields in gold_buildings:")
for field in buildings_capacity_fields:
    print(f"   • {field}")

# Check data completeness
building_read_fields = ['source', 'company_clean']
if 'full_capacity_mw' in buildings_field_names:
    building_read_fields.append('full_capacity_mw')
elif 'capacity_mw' in buildings_field_names:
    building_read_fields.append('capacity_mw')
else:
    # Use first MW field found
    if len(buildings_capacity_fields) > 0:
        building_read_fields.append(buildings_capacity_fields[0])

building_data = []
with arcpy.da.SearchCursor(gold_buildings_fc, building_read_fields) as cursor:
    for row in cursor:
        building_data.append(row)

df_buildings = pd.DataFrame(building_data, columns=building_read_fields)

if 'company_clean' in df_buildings.columns:
    df_buildings_meta = df_buildings[df_buildings['company_clean'] == 'Meta']
else:
    df_buildings_meta = df_buildings

print(f"\n📊 Total Meta building records: {len(df_buildings_meta)}")

# Find capacity column
capacity_col = None
if 'full_capacity_mw' in df_buildings_meta.columns:
    capacity_col = 'full_capacity_mw'
elif 'capacity_mw' in df_buildings_meta.columns:
    capacity_col = 'capacity_mw'
elif len(buildings_capacity_fields) > 0 and buildings_capacity_fields[0] in df_buildings_meta.columns:
    capacity_col = buildings_capacity_fields[0]

if capacity_col:
    total_with_capacity = df_buildings_meta[capacity_col].notna().sum()
    total_without_capacity = df_buildings_meta[capacity_col].isna().sum()

    print(f"\n🔋 Building-Level Capacity Data (field: {capacity_col}):")
    print(f"   Records WITH capacity: {total_with_capacity} ({total_with_capacity/len(df_buildings_meta)*100:.1f}%)")
    print(f"   Records WITHOUT capacity: {total_without_capacity} ({total_without_capacity/len(df_buildings_meta)*100:.1f}%)")

    if total_with_capacity > 0:
        print(f"\n   Overall Statistics:")
        print(f"   Mean: {df_buildings_meta[capacity_col].mean():.1f} MW")
        print(f"   Median: {df_buildings_meta[capacity_col].median():.1f} MW")
        print(f"   Total: {df_buildings_meta[capacity_col].sum():.1f} MW")

        print(f"\n   By Source:")
        for source in df_buildings_meta['source'].unique():
            if pd.isna(source):
                continue
            source_data = df_buildings_meta[df_buildings_meta['source'] == source]
            with_cap = source_data[capacity_col].notna().sum()
            total = len(source_data)
            avg_cap = source_data[capacity_col].mean()
            if pd.notna(avg_cap):
                print(f"   {source}: {with_cap}/{total} ({with_cap/total*100:.1f}%) - Avg: {avg_cap:.1f} MW")
            else:
                print(f"   {source}: {with_cap}/{total} ({with_cap/total*100:.1f}%) - Avg: N/A")
else:
    print("\n❌ No capacity field found in gold_buildings!")

# ============================================================================
# Recommendation
# ============================================================================

print("\n" + "="*80)
print("DIAGNOSIS & RECOMMENDATION")
print("="*80)

if 'total_capacity_mw' not in field_names:
    print("\n❌ CRITICAL ISSUE: gold_campus.total_capacity_mw field DOES NOT EXIST")
    print("\n💡 SOLUTION OPTIONS:")
    print("\n   OPTION A: Re-run campus rollup script (Campus_Rollup_new.py)")
    print("      - Check if it creates total_capacity_mw field")
    print("      - If not, may need to modify script")

    print("\n   OPTION B: Manually add and populate field")
    print("      Step 1: Add field:")
    print("         arcpy.management.AddField(gold_campus_fc, 'total_capacity_mw', 'DOUBLE')")
    print("\n      Step 2: Aggregate from gold_buildings:")
    print("         - Group by campus_id")
    print("         - Sum full_capacity_mw")
    print("         - Update gold_campus records")

    print("\n   OPTION C: Use building-level data for validation")
    print("      - Aggregate gold_buildings to campus level on-the-fly")
    print("      - Compare to Meta canonical campus aggregation")
    print("      - This is what capacity_validation script will do automatically")

elif 'total_capacity_mw' in field_names:
    # Already checked above, but add final recommendation
    all_fields_check = ['company_clean', 'total_capacity_mw']
    check_data = []
    with arcpy.da.SearchCursor(gold_campus_fc, all_fields_check) as cursor:
        for row in cursor:
            check_data.append(row)
    df_check = pd.DataFrame(check_data, columns=all_fields_check)
    df_check_meta = df_check[df_check['company_clean'] == 'Meta']

    if df_check_meta['total_capacity_mw'].notna().sum() == 0:
        print("\n❌ ISSUE: gold_campus.total_capacity_mw exists but ALL values are NULL")
        print("\n💡 SOLUTION: Re-run campus rollup or manually populate")
    elif df_check_meta['total_capacity_mw'].notna().sum() < len(df_check_meta) * 0.5:
        print(f"\n⚠️ WARNING: Only {df_check_meta['total_capacity_mw'].notna().sum()/len(df_check_meta)*100:.1f}% have capacity")
        print("\n💡 SOLUTION: Investigate which sources missing data, supplement from buildings")
    else:
        print(f"\n✅ GOOD: {df_check_meta['total_capacity_mw'].notna().sum()/len(df_check_meta)*100:.1f}% have capacity")
        print("\n💡 Ready for capacity validation!")

print("\n" + "="*80)
print("NEXT STEPS")
print("="*80)

if capacity_col and 'source' in df_buildings_meta.columns:
    print("\n✅ CAN PROCEED WITH CAPACITY VALIDATION")
    print("   - Will use gold_buildings aggregated to campus level")
    print("   - Run: capacity_validation_campus_level.py")
    print("   - Script will handle aggregation automatically")
else:
    print("\n⚠️ CANNOT PROCEED WITH CAPACITY VALIDATION")
    print("   - No capacity data available in either gold_campus or gold_buildings")
    print("   - Need to investigate source data")

print("\n" + "="*80)
print(f"DIAGNOSTIC COMPLETE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)
