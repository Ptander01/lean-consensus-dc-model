import pandas as pd
import os

# ============================================================================
# DIAGNOSTIC SCRIPT: New Meta Canonical Data (Deduplicated Query)
# ============================================================================
# Purpose: Analyze new CSV from internal database query to determine if it's
#          worth importing as canonical baseline for spatial accuracy analysis
#
# Expected fields from SQL query:
#   - location_key, datacenter, region, address
#   - latitude, longitude, building_type, new_build_status
#   - it_load, latest_milestone_date, latest_phase_gate, latest_activity_status
# ============================================================================

csv_file = r"C:\Users\ptanderson\Downloads\daiquery-896763959683293-1594552168562015-2025-11-25 10_08am.csv"

print("="*80)
print("META CANONICAL DATA DIAGNOSTIC - Deduplicated Query Version")
print("="*80)
print(f"\nCSV File: {csv_file}")
print(f"File exists: {os.path.exists(csv_file)}")
print(f"File size: {os.path.getsize(csv_file) / 1024:.1f} KB")
print("\n" + "="*80)

# Load CSV with UTF-8 encoding to handle international characters
try:
    df = pd.read_csv(csv_file, encoding='utf-8')
    print("✅ CSV loaded successfully with UTF-8 encoding")
except UnicodeDecodeError:
    print("⚠️  UTF-8 failed, trying utf-8-sig...")
    try:
        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        print("✅ CSV loaded successfully with UTF-8-SIG encoding")
    except:
        print("⚠️  UTF-8-SIG failed, trying latin-1...")
        df = pd.read_csv(csv_file, encoding='latin-1')
        print("✅ CSV loaded successfully with Latin-1 encoding")

# ============================================================================
# 1. BASIC STRUCTURE
# ============================================================================
print("\n📊 1. BASIC STRUCTURE")
print("-"*80)
print(f"Total rows: {len(df):,}")
print(f"Total columns: {len(df.columns)}")
print(f"\nColumn names:")
for i, col in enumerate(df.columns, 1):
    print(f"  {i}. {col}")

# ============================================================================
# 2. UNIQUE LOCATIONS
# ============================================================================
print("\n" + "="*80)
print("🗺️  2. UNIQUE LOCATIONS")
print("-"*80)

if 'location_key' in df.columns:
    unique_locations = df['location_key'].nunique()
    total_rows = len(df)
    print(f"Unique location_keys: {unique_locations:,}")
    print(f"Total rows: {total_rows:,}")
    print(f"Deduplication ratio: {total_rows / unique_locations:.2f}x")

    if unique_locations == total_rows:
        print("✅ Perfect deduplication! One row per unique location.")
    else:
        print(f"⚠️  Found duplicates. Analyzing...")
        dupes = df[df.duplicated(subset=['location_key'], keep=False)]
        if len(dupes) > 0:
            print(f"\nDuplicate location_keys ({len(dupes)} rows):")
            print(dupes[['location_key', 'datacenter', 'new_build_status']].head(10))
else:
    print("❌ No 'location_key' field found!")

# ============================================================================
# 3. GEOGRAPHIC COVERAGE
# ============================================================================
print("\n" + "="*80)
print("🌍 3. GEOGRAPHIC COVERAGE")
print("-"*80)

# Region breakdown
if 'region' in df.columns:
    print("\nBy Region:")
    region_counts = df['region'].value_counts()
    for region, count in region_counts.items():
        pct = (count / len(df)) * 100
        print(f"  {region}: {count} sites ({pct:.1f}%)")

    print(f"\nUnique regions: {df['region'].nunique()}")
    if df['region'].nunique() > 2:
        print("✅ Global coverage detected!")
    else:
        print("⚠️  Limited regional coverage (US-only?)")
else:
    print("❌ No 'region' field found!")

# Datacenter breakdown
if 'datacenter' in df.columns:
    print(f"\nDatacenter Codes ({df['datacenter'].nunique()} unique):")
    dc_counts = df['datacenter'].value_counts().head(15)
    for dc, count in dc_counts.items():
        print(f"  {dc}: {count} sites")

    if df['datacenter'].nunique() > 15:
        print(f"  ... and {df['datacenter'].nunique() - 15} more datacenters")

# ============================================================================
# 4. COORDINATE QUALITY
# ============================================================================
print("\n" + "="*80)
print("📍 4. COORDINATE QUALITY")
print("-"*80)

if 'latitude' in df.columns and 'longitude' in df.columns:
    total_coords = len(df)
    valid_coords = df[df['latitude'].notna() & df['longitude'].notna()]
    missing_coords = total_coords - len(valid_coords)

    print(f"Total records: {total_coords}")
    print(f"Valid coordinates: {len(valid_coords)} ({len(valid_coords)/total_coords*100:.1f}%)")
    print(f"Missing coordinates: {missing_coords} ({missing_coords/total_coords*100:.1f}%)")

    if len(valid_coords) > 0:
        print(f"\nCoordinate ranges:")
        print(f"  Latitude:  {valid_coords['latitude'].min():.6f} to {valid_coords['latitude'].max():.6f}")
        print(f"  Longitude: {valid_coords['longitude'].min():.6f} to {valid_coords['longitude'].max():.6f}")

        # Check for suspect coordinates (0,0 or invalid ranges)
        suspect = valid_coords[
            (valid_coords['latitude'] == 0) & (valid_coords['longitude'] == 0) |
            (valid_coords['latitude'].abs() > 90) |
            (valid_coords['longitude'].abs() > 180)
        ]
        if len(suspect) > 0:
            print(f"⚠️  Suspect coordinates found: {len(suspect)} records")
        else:
            print("✅ All coordinates within valid ranges")
else:
    print("❌ Missing latitude/longitude fields!")

# ============================================================================
# 5. CAPACITY DATA
# ============================================================================
print("\n" + "="*80)
print("⚡ 5. CAPACITY DATA (IT Load)")
print("-"*80)

if 'it_load' in df.columns:
    total_records = len(df)
    has_capacity = df[df['it_load'].notna()]
    missing_capacity = total_records - len(has_capacity)

    print(f"Records with IT load: {len(has_capacity)} ({len(has_capacity)/total_records*100:.1f}%)")
    print(f"Records missing IT load: {missing_capacity} ({missing_capacity/total_records*100:.1f}%)")

    if len(has_capacity) > 0:
        total_mw = has_capacity['it_load'].sum()
        mean_mw = has_capacity['it_load'].mean()
        median_mw = has_capacity['it_load'].median()

        print(f"\nCapacity statistics:")
        print(f"  Total IT load: {total_mw:,.1f} MW")
        print(f"  Mean per site: {mean_mw:.1f} MW")
        print(f"  Median per site: {median_mw:.1f} MW")
        print(f"  Min: {has_capacity['it_load'].min():.1f} MW")
        print(f"  Max: {has_capacity['it_load'].max():.1f} MW")
else:
    print("❌ No 'it_load' field found!")

# ============================================================================
# 6. BUILD STATUS
# ============================================================================
print("\n" + "="*80)
print("🏗️  6. BUILD STATUS")
print("-"*80)

if 'new_build_status' in df.columns:
    print("\nBuild status breakdown:")
    status_counts = df['new_build_status'].value_counts()
    for status, count in status_counts.items():
        pct = (count / len(df)) * 100
        print(f"  {status}: {count} sites ({pct:.1f}%)")
else:
    print("❌ No 'new_build_status' field found!")

if 'latest_activity_status' in df.columns:
    print(f"\nLatest activity status ({df['latest_activity_status'].nunique()} unique values):")
    activity_counts = df['latest_activity_status'].value_counts().head(10)
    for status, count in activity_counts.items():
        print(f"  {status}: {count}")

# ============================================================================
# 7. BUILDING TYPE
# ============================================================================
print("\n" + "="*80)
print("🏢 7. BUILDING TYPE")
print("-"*80)

if 'building_type' in df.columns:
    print("\nBuilding type breakdown:")
    type_counts = df['building_type'].value_counts()
    for btype, count in type_counts.items():
        pct = (count / len(df)) * 100
        print(f"  {btype}: {count} sites ({pct:.1f}%)")
else:
    print("❌ No 'building_type' field found!")

# ============================================================================
# 8. TEMPORAL DATA
# ============================================================================
print("\n" + "="*80)
print("📅 8. TEMPORAL DATA")
print("-"*80)

if 'latest_milestone_date' in df.columns:
    df['latest_milestone_date'] = pd.to_datetime(df['latest_milestone_date'], errors='coerce')
    has_dates = df[df['latest_milestone_date'].notna()]

    print(f"Records with milestone dates: {len(has_dates)} ({len(has_dates)/len(df)*100:.1f}%)")

    if len(has_dates) > 0:
        print(f"\nMilestone date range:")
        print(f"  Earliest: {has_dates['latest_milestone_date'].min()}")
        print(f"  Latest: {has_dates['latest_milestone_date'].max()}")
else:
    print("❌ No 'latest_milestone_date' field found!")

# ============================================================================
# 9. COMPARISON TO EXISTING CANONICAL DATA
# ============================================================================
print("\n" + "="*80)
print("🔄 9. COMPARISON TO EXISTING CANONICAL DATA")
print("-"*80)

print("\nOld canonical dataset (from milestone data):")
print("  Total records: 30")
print("  Unique locations: 30")
print("  Datacenters: 2 (CHY, DKL)")
print("  Buildings: 9 (CHY Bldg 1-5, DKL Bldg 1-6)")
print("  Total IT load: 366 MW")
print("  Coverage: US-only (Wyoming, Illinois)")

print(f"\nNew canonical dataset:")
if 'location_key' in df.columns:
    print(f"  Total records: {len(df)}")
    print(f"  Unique locations: {df['location_key'].nunique()}")
if 'datacenter' in df.columns:
    print(f"  Datacenters: {df['datacenter'].nunique()}")
if 'region' in df.columns:
    print(f"  Regions: {df['region'].nunique()} ({', '.join(df['region'].unique())})")
if 'it_load' in df.columns:
    total_load = df['it_load'].sum()
    print(f"  Total IT load: {total_load:,.1f} MW")

# Calculate improvement
if 'location_key' in df.columns:
    new_unique = df['location_key'].nunique()
    old_unique = 30
    improvement = ((new_unique - old_unique) / old_unique) * 100
    print(f"\n📈 Location coverage increase: +{new_unique - old_unique} sites ({improvement:.1f}%)")

# ============================================================================
# 10. SAMPLE RECORDS
# ============================================================================
print("\n" + "="*80)
print("📋 10. SAMPLE RECORDS")
print("-"*80)

print("\nFirst 5 records:")
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
print(df.head(5).to_string())

# ============================================================================
# 11. FINAL RECOMMENDATION
# ============================================================================
print("\n" + "="*80)
print("✅ FINAL RECOMMENDATION")
print("="*80)

# Decision logic
has_coords = 'latitude' in df.columns and 'longitude' in df.columns
coord_completeness = len(df[df['latitude'].notna() & df['longitude'].notna()]) / len(df) if has_coords else 0
unique_sites = df['location_key'].nunique() if 'location_key' in df.columns else len(df)
is_global = df['region'].nunique() > 2 if 'region' in df.columns else False

print(f"\nDecision criteria:")
print(f"  ✓ Unique locations: {unique_sites} (threshold: >50)")
print(f"  ✓ Coordinate completeness: {coord_completeness*100:.1f}% (threshold: >80%)")
print(f"  ✓ Global coverage: {'Yes' if is_global else 'No/Unknown'}")

if unique_sites >= 50 and coord_completeness >= 0.8:
    print("\n🎉 RECOMMENDATION: **IMPORT THIS DATASET**")
    print("\nThis data is significantly better than the existing 30-location baseline.")
    print("Proceed with creating import script: import_meta_canonical_v2.py")
elif unique_sites >= 20 and coord_completeness >= 0.5:
    print("\n⚠️  RECOMMENDATION: **IMPORT WITH CAUTION**")
    print("\nThis data has some issues but still valuable. Review sample records first.")
else:
    print("\n❌ RECOMMENDATION: **SKIP - NOT WORTH IMPORTING**")
    print("\nThis data is not significantly better than existing baseline.")

print("\n" + "="*80)
print("DIAGNOSTIC COMPLETE")
print("="*80)
