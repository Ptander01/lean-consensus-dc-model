import arcpy
import os

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------
gdb_path = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
arcpy.env.workspace = gdb_path
arcpy.env.overwriteOutput = True

# Feature classes to update
fcs_to_fix = ["gold_buildings", "gold_campus"]

print("=" * 80)
print("DATA QUALITY FIXES")
print("=" * 80)
print("\nFixing:")
print("  1. Standardize country names → 'United States'")
print("  2. Populate region field (AMER/EMEA/APAC/OTHER)")
print("  3. Change 'Planned' status → 'Announced'")
print("=" * 80)

# --------------------------------------------------
# 1. STANDARDIZE COUNTRY FIELD
# --------------------------------------------------
print("\n🔧 STEP 1: Standardizing country names...")

country_mapping = {
    'USA': 'United States',
    'US': 'United States',
    'U.S.': 'United States',
    'United States of America': 'United States',
    'UK': 'United Kingdom',
    'Great Britain': 'United Kingdom',
    'UAE': 'United Arab Emirates'
}

for fc_name in fcs_to_fix:
    fc_path = os.path.join(gdb_path, fc_name)
    
    if not arcpy.Exists(fc_path):
        print(f"  ⚠ {fc_name} not found, skipping...")
        continue
    
    print(f"\n  Processing {fc_name}...")
    
    update_count = 0
    updates_by_country = {}
    
    with arcpy.da.UpdateCursor(fc_path, ['country']) as cursor:
        for row in cursor:
            original_country = row[0]
            
            if original_country in country_mapping:
                new_country = country_mapping[original_country]
                row[0] = new_country
                cursor.updateRow(row)
                
                updates_by_country[original_country] = updates_by_country.get(original_country, 0) + 1
                update_count += 1
    
    if update_count > 0:
        print(f"    ✓ Updated {update_count} country values:")
        for old, count in updates_by_country.items():
            print(f"      • {old} → {country_mapping[old]} ({count} records)")
    else:
        print(f"    ✓ No country updates needed")

# --------------------------------------------------
# 2. DERIVE REGION FROM COUNTRY
# --------------------------------------------------
print("\n🔧 STEP 2: Deriving region from country...")

# Region mapping
region_mapping = {
    # AMER (Americas)
    'United States': 'AMER',
    'Canada': 'AMER',
    'Mexico': 'AMER',
    'Brazil': 'AMER',
    'Chile': 'AMER',
    'Argentina': 'AMER',
    'Colombia': 'AMER',
    'Peru': 'AMER',
    'Costa Rica': 'AMER',
    'Panama': 'AMER',
    
    # EMEA (Europe, Middle East, Africa)
    'United Kingdom': 'EMEA',
    'Ireland': 'EMEA',
    'Germany': 'EMEA',
    'France': 'EMEA',
    'Netherlands': 'EMEA',
    'Belgium': 'EMEA',
    'Switzerland': 'EMEA',
    'Spain': 'EMEA',
    'Italy': 'EMEA',
    'Poland': 'EMEA',
    'Sweden': 'EMEA',
    'Norway': 'EMEA',
    'Denmark': 'EMEA',
    'Finland': 'EMEA',
    'Austria': 'EMEA',
    'Czech Republic': 'EMEA',
    'Portugal': 'EMEA',
    'Greece': 'EMEA',
    'South Africa': 'EMEA',
    'Israel': 'EMEA',
    'United Arab Emirates': 'EMEA',
    'Saudi Arabia': 'EMEA',
    'Qatar': 'EMEA',
    'Turkey': 'EMEA',
    'Egypt': 'EMEA',
    'Kenya': 'EMEA',
    'Nigeria': 'EMEA',
    
    # APAC (Asia Pacific)
    'Singapore': 'APAC',
    'China': 'APAC',
    'Japan': 'APAC',
    'South Korea': 'APAC',
    'Australia': 'APAC',
    'New Zealand': 'APAC',
    'India': 'APAC',
    'Hong Kong': 'APAC',
    'Taiwan': 'APAC',
    'Malaysia': 'APAC',
    'Thailand': 'APAC',
    'Indonesia': 'APAC',
    'Philippines': 'APAC',
    'Vietnam': 'APAC',
    'Pakistan': 'APAC',
    'Bangladesh': 'APAC'
}

for fc_name in fcs_to_fix:
    fc_path = os.path.join(gdb_path, fc_name)
    
    if not arcpy.Exists(fc_path):
        continue
    
    print(f"\n  Processing {fc_name}...")
    
    update_count = 0
    null_count = 0
    unknown_countries = set()
    region_counts = {}
    
    with arcpy.da.UpdateCursor(fc_path, ['country', 'region']) as cursor:
        for row in cursor:
            country = row[0]
            
            if not country:
                null_count += 1
                continue
            
            if country in region_mapping:
                new_region = region_mapping[country]
                row[1] = new_region
                cursor.updateRow(row)
                region_counts[new_region] = region_counts.get(new_region, 0) + 1
                update_count += 1
            else:
                # Unknown country - default to OTHER
                row[1] = 'OTHER'
                cursor.updateRow(row)
                unknown_countries.add(country)
                region_counts['OTHER'] = region_counts.get('OTHER', 0) + 1
                update_count += 1
    
    print(f"    ✓ Updated {update_count} region values:")
    for region in ['AMER', 'EMEA', 'APAC', 'OTHER']:
        if region in region_counts:
            print(f"      • {region}: {region_counts[region]} records")
    
    if null_count > 0:
        print(f"    ⚠ Skipped {null_count} records with null country")
    
    if unknown_countries:
        print(f"    ⚠ Unknown countries mapped to 'OTHER': {', '.join(sorted(unknown_countries))}")

# --------------------------------------------------
# 3. FIX FACILITY_STATUS: Planned → Announced
# --------------------------------------------------
print("\n🔧 STEP 3: Changing 'Planned' status to 'Announced'...")

for fc_name in fcs_to_fix:
    fc_path = os.path.join(gdb_path, fc_name)
    
    if not arcpy.Exists(fc_path):
        continue
    
    # Check if facility_status field exists
    if not arcpy.ListFields(fc_path, 'facility_status'):
        print(f"  ⚠ {fc_name} does not have 'facility_status' field, skipping...")
        continue
    
    print(f"\n  Processing {fc_name}...")
    
    update_count = 0
    
    with arcpy.da.UpdateCursor(fc_path, ['facility_status']) as cursor:
        for row in cursor:
            if row[0] == 'Planned':
                row[0] = 'Announced'
                cursor.updateRow(row)
                update_count += 1
    
    if update_count > 0:
        print(f"    ✓ Updated {update_count} records: 'Planned' → 'Announced'")
    else:
        print(f"    ✓ No 'Planned' status records found")

# --------------------------------------------------
# 4. VERIFY RESULTS
# --------------------------------------------------
print("\n" + "=" * 80)
print("VERIFICATION REPORT")
print("=" * 80)

for fc_name in fcs_to_fix:
    fc_path = os.path.join(gdb_path, fc_name)
    
    if not arcpy.Exists(fc_path):
        continue
    
    count = int(arcpy.management.GetCount(fc_path)[0])
    print(f"\n{fc_name.upper()} ({count:,} total records)")
    print("-" * 80)
    
    # Region distribution
    print("\n  🌍 Regions:")
    region_counts = {}
    with arcpy.da.SearchCursor(fc_path, ['region']) as cursor:
        for row in cursor:
            region = row[0] if row[0] else '(null)'
            region_counts[region] = region_counts.get(region, 0) + 1
    
    for region in ['AMER', 'EMEA', 'APAC', 'OTHER', '(null)']:
        if region in region_counts:
            pct = (region_counts[region] / count * 100) if count > 0 else 0
            print(f"      • {region:<10} {region_counts[region]:>6,} ({pct:>5.1f}%)")
    
    # Country distribution (top 10)
    print("\n  🌍 Countries (top 10):")
    country_counts = {}
    with arcpy.da.SearchCursor(fc_path, ['country']) as cursor:
        for row in cursor:
            country = row[0] if row[0] else '(null)'
            country_counts[country] = country_counts.get(country, 0) + 1
    
    sorted_countries = sorted(country_counts.items(), key=lambda x: x[1], reverse=True)
    for country, cnt in sorted_countries[:10]:
        pct = (cnt / count * 100) if count > 0 else 0
        print(f"      • {country:<25} {cnt:>6,} ({pct:>5.1f}%)")
    
    # Facility status distribution
    if arcpy.ListFields(fc_path, 'facility_status'):
        print("\n  📊 Facility Status:")
        status_counts = {}
        with arcpy.da.SearchCursor(fc_path, ['facility_status']) as cursor:
            for row in cursor:
                status = row[0] if row[0] else '(null)'
                status_counts[status] = status_counts.get(status, 0) + 1
        
        status_order = ['Active', 'Under Construction', 'Permitting', 'Announced', 
                       'Land Acquisition', 'Rumor', 'Unknown', 'Planned']
        
        for status in status_order:
            if status in status_counts:
                pct = (status_counts[status] / count * 100) if count > 0 else 0
                marker = "⚠" if status == "Planned" else "✓"
                print(f"      {marker} {status:<25} {status_counts[status]:>6,} ({pct:>5.1f}%)")
        
        # Show any unexpected statuses
        for status in sorted(status_counts.keys()):
            if status not in status_order:
                pct = (status_counts[status] / count * 100) if count > 0 else 0
                print(f"      ⚠ {status:<25} {status_counts[status]:>6,} ({pct:>5.1f}%)")

print("\n" + "=" * 80)
print("✓ DATA QUALITY FIXES COMPLETE")
print("=" * 80)
print("\nSummary:")
print("  ✅ Country names standardized to 'United States'")
print("  ✅ Region field populated (AMER/EMEA/APAC/OTHER)")
print("  ✅ 'Planned' status changed to 'Announced'")
print("\nReady to ingest new data sources!")
print("=" * 80)