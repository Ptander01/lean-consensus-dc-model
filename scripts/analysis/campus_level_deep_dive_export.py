# CAMPUS-LEVEL DEEP DIVE COMPARISON - FIXED
import arcpy
import pandas as pd
import numpy as np
from datetime import datetime
import os

# Configuration
gdb_path = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
gold_campus_fc = f"{gdb_path}\\gold_campus"
meta_fc = f"{gdb_path}\\meta_canonical_v2"

# Output directory
output_dir = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\analysis_outputs"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Output files
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
txt_output = os.path.join(output_dir, f"campus_comparison_{timestamp}.txt")
csv_summary = os.path.join(output_dir, f"campus_comparison_summary_{timestamp}.csv")
csv_detailed = os.path.join(output_dir, f"campus_comparison_detailed_{timestamp}.csv")

# Conversion factor
METERS_TO_MILES = 0.000621371

print("="*80)
print(f"CAMPUS-LEVEL DEEP DIVE COMPARISON")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# Open text file for writing
txt_file = open(txt_output, 'w', encoding='utf-8')

def write_both(text):
    """Write to both console and file"""
    txt_file.write(text + '\n')
    print(text)

write_both("="*80)
write_both(f"CAMPUS-LEVEL COMPARISON: META CANONICAL VS EXTERNAL SOURCES")
write_both(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
write_both("="*80)
write_both("")

# Check if feature classes exist
if not arcpy.Exists(gold_campus_fc):
    error_msg = f"❌ ERROR: Gold campus feature class not found: {gold_campus_fc}"
    write_both(error_msg)
    txt_file.close()
    raise Exception(f"Feature class does not exist: {gold_campus_fc}")

if not arcpy.Exists(meta_fc):
    error_msg = f"❌ ERROR: Meta canonical feature class not found: {meta_fc}"
    write_both(error_msg)
    txt_file.close()
    raise Exception(f"Feature class does not exist: {meta_fc}")

print("   Checking data sources...")
write_both(f"✅ Found gold_campus: {arcpy.management.GetCount(gold_campus_fc)[0]} campus records")
write_both(f"✅ Found meta_canonical_v2: {arcpy.management.GetCount(meta_fc)[0]} building records")

# STEP 1: Aggregate Meta Data to Campus Level
write_both("\n" + "="*80)
write_both("STEP 1: AGGREGATING META DATA TO CAMPUS LEVEL")
write_both("="*80)

print("   Reading Meta canonical data...")

meta_fields = ['location_key', 'datacenter', 'suite', 'dc_code', 'region_derived',
               'it_load', 'new_build_status', 'building_type', 'address',
               'SHAPE@X', 'SHAPE@Y']

meta_data = []
with arcpy.da.SearchCursor(meta_fc, meta_fields) as cursor:
    for row in cursor:
        meta_data.append(row)

df_meta = pd.DataFrame(meta_data, columns=meta_fields)

print("   Aggregating to campus level...")

# Helper function for safe aggregation
def safe_mode(x):
    """Get mode value, handling empty series"""
    try:
        vc = x.value_counts()
        if len(vc) > 0:
            return vc.index[0]
        else:
            return 'Unknown'
    except:
        return 'Unknown'

def safe_building_type(x):
    """Get building type, handling mixed types"""
    try:
        unique_vals = x.dropna().unique()
        if len(unique_vals) > 1:
            return 'Mixed'
        elif len(unique_vals) == 1:
            return unique_vals[0]
        else:
            return 'Unknown'
    except:
        return 'Unknown'

meta_campus_agg = df_meta.groupby('dc_code').agg({
    'it_load': 'sum',
    'location_key': 'count',
    'region_derived': 'first',
    'address': 'first',
    'SHAPE@X': 'first',
    'SHAPE@Y': 'first',
    'building_type': safe_building_type,
    'new_build_status': safe_mode
}).reset_index()

meta_campus_agg.columns = ['dc_code', 'total_mw', 'building_count', 'region',
                           'address', 'lon', 'lat', 'building_type', 'primary_status']

# Filter to campuses with coordinates and MW data
meta_campus_agg = meta_campus_agg[
    (meta_campus_agg['total_mw'].notna()) &
    (meta_campus_agg['total_mw'] > 0) &
    (meta_campus_agg['lon'].notna()) &
    (meta_campus_agg['lat'].notna()) &
    (meta_campus_agg['lon'] != 0) &
    (meta_campus_agg['lat'] != 0)
].copy()

write_both(f"\n📊 Meta Campus Summary:")
write_both(f"   Total unique campuses: {len(meta_campus_agg)}")
write_both(f"   Total IT Load: {meta_campus_agg['total_mw'].sum():.1f} MW")
write_both(f"   Avg campus size: {meta_campus_agg['total_mw'].mean():.1f} MW")
write_both(f"   Avg buildings per campus: {meta_campus_agg['building_count'].mean():.1f}")

write_both(f"\n🏆 Top 15 Meta Campuses:")
top_15 = meta_campus_agg.sort_values('total_mw', ascending=False).head(15)
write_both("\n" + top_15[['dc_code', 'total_mw', 'building_count', 'region', 'primary_status']].to_string(index=False))

# STEP 2: Read Gold Campus Data
write_both("\n" + "="*80)
write_both("STEP 2: READING EXTERNAL CAMPUS DATA")
write_both("="*80)

print("   Reading gold_campus data...")

gold_fields = [f.name for f in arcpy.ListFields(gold_campus_fc)]

extract_fields = ['SHAPE@X', 'SHAPE@Y', 'campus_id', 'campus_name', 'source',
                  'company_clean', 'city', 'state', 'country', 'region',
                  'total_capacity_mw', 'total_cost', 'total_acreage',
                  'building_count', 'facility_status']

for year in range(2023, 2033):
    field_name = f'mw_{year}'
    if field_name in gold_fields:
        extract_fields.append(field_name)

extract_fields = [f for f in extract_fields if f in gold_fields or f.startswith('SHAPE@')]

gold_data = []
with arcpy.da.SearchCursor(gold_campus_fc, extract_fields) as cursor:
    for row in cursor:
        gold_data.append(row)

df_gold = pd.DataFrame(gold_data, columns=extract_fields)

if 'company_clean' in df_gold.columns:
    df_gold_meta = df_gold[df_gold['company_clean'] == 'Meta'].copy()
else:
    df_gold_meta = df_gold.copy()

write_both(f"\n📊 External Campus Data:")
write_both(f"   Total campus records: {len(df_gold)}")
write_both(f"   Meta campus records: {len(df_gold_meta)}")

if 'source' in df_gold_meta.columns:
    write_both(f"\n   By Source:")
    source_counts = df_gold_meta['source'].value_counts()
    for source, count in source_counts.items():
        write_both(f"      {source}: {count}")

# STEP 3: Spatial Join - Campus to Campus
write_both("\n" + "="*80)
write_both("STEP 3: PERFORMING CAMPUS-TO-CAMPUS SPATIAL MATCHING")
write_both("="*80)

print("   Calculating campus-to-campus distances...")

from math import radians, sin, cos, sqrt, atan2

comparison_data = []

for _, meta_campus in meta_campus_agg.iterrows():
    meta_dc = meta_campus['dc_code']
    meta_lon = meta_campus['lon']
    meta_lat = meta_campus['lat']
    meta_mw = meta_campus['total_mw']

    for _, ext_campus in df_gold_meta.iterrows():
        ext_lon = ext_campus['SHAPE@X'] if 'SHAPE@X' in ext_campus else None
        ext_lat = ext_campus['SHAPE@Y'] if 'SHAPE@Y' in ext_campus else None

        if ext_lon is None or ext_lat is None or pd.isna(ext_lon) or pd.isna(ext_lat):
            continue

        R = 6371000  # Earth radius in meters

        lat1 = radians(meta_lat)
        lon1 = radians(meta_lon)
        lat2 = radians(ext_lat)
        lon2 = radians(ext_lon)

        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        distance_m = R * c
        distance_miles = distance_m * METERS_TO_MILES

        if distance_miles <= 3.1:
            comparison_data.append({
                'meta_dc_code': meta_dc,
                'meta_mw': meta_mw,
                'meta_buildings': meta_campus['building_count'],
                'meta_region': meta_campus['region'],
                'meta_status': meta_campus['primary_status'],
                'meta_building_type': meta_campus['building_type'],
                'meta_address': meta_campus['address'],
                'meta_lat': meta_lat,
                'meta_lon': meta_lon,
                'ext_campus_id': ext_campus['campus_id'] if 'campus_id' in ext_campus else None,
                'ext_campus_name': ext_campus['campus_name'] if 'campus_name' in ext_campus else None,
                'ext_source': ext_campus['source'] if 'source' in ext_campus else None,
                'ext_capacity_mw': ext_campus['total_capacity_mw'] if 'total_capacity_mw' in ext_campus else None,
                'ext_buildings': ext_campus['building_count'] if 'building_count' in ext_campus else None,
                'ext_status': ext_campus['facility_status'] if 'facility_status' in ext_campus else None,
                'ext_city': ext_campus['city'] if 'city' in ext_campus else None,
                'ext_state': ext_campus['state'] if 'state' in ext_campus else None,
                'ext_cost': ext_campus['total_cost'] if 'total_cost' in ext_campus else None,
                'ext_acreage': ext_campus['total_acreage'] if 'total_acreage' in ext_campus else None,
                'distance_miles': distance_miles
            })

df_matches = pd.DataFrame(comparison_data)

write_both(f"\n✅ Found {len(df_matches)} campus-to-campus matches (within 3.1 miles)")

if len(df_matches) == 0:
    write_both("\n❌ No matches found - check coordinate systems")
    txt_file.close()
    raise Exception("No spatial matches found")

# STEP 4: Analyze Coverage by Campus
write_both("\n" + "="*80)
write_both("STEP 4: COVERAGE ANALYSIS")
write_both("="*80)

detected_campuses = df_matches['meta_dc_code'].unique()
all_campuses = meta_campus_agg['dc_code'].unique()
undetected_campuses = set(all_campuses) - set(detected_campuses)

write_both(f"\n📊 Detection Statistics:")
write_both(f"   Total Meta campuses: {len(all_campuses)}")
write_both(f"   Detected by external sources: {len(detected_campuses)} ({len(detected_campuses)/len(all_campuses)*100:.1f}%)")
write_both(f"   Undetected: {len(undetected_campuses)} ({len(undetected_campuses)/len(all_campuses)*100:.1f}%)")

if len(undetected_campuses) > 0:
    write_both(f"\n⚠️ Undetected Campuses:")
    for dc_code in sorted(undetected_campuses):
        campus_info = meta_campus_agg[meta_campus_agg['dc_code'] == dc_code].iloc[0]
        write_both(f"   {dc_code}: {campus_info['total_mw']:.1f} MW, {int(campus_info['building_count'])} buildings, {campus_info['primary_status']}")

write_both(f"\n📊 Coverage by Source:")
for source in df_matches['ext_source'].unique():
    if pd.isna(source):
        continue
    source_matches = df_matches[df_matches['ext_source'] == source]
    unique_campuses = source_matches['meta_dc_code'].nunique()
    write_both(f"   {source}: {unique_campuses} campuses ({unique_campuses/len(all_campuses)*100:.1f}%)")

# STEP 5: Campus-by-Campus Detailed Comparison (Top 15)
write_both("\n" + "="*80)
write_both("STEP 5: DETAILED CAMPUS COMPARISONS (Top 15)")
write_both("="*80)

top_15_codes = top_15['dc_code'].tolist()

for dc_code in top_15_codes:
    print(f"   Processing campus: {dc_code}")

    write_both("\n" + "="*80)
    write_both(f"🏢 CAMPUS: {dc_code}")
    write_both("="*80)

    meta_info = meta_campus_agg[meta_campus_agg['dc_code'] == dc_code].iloc[0]

    write_both(f"\n📊 META CANONICAL DATA:")
    write_both(f"   DC Code: {dc_code}")
    write_both(f"   Total IT Load: {meta_info['total_mw']:.1f} MW")
    write_both(f"   Number of Buildings: {int(meta_info['building_count'])}")
    write_both(f"   Region: {meta_info['region']}")
    write_both(f"   Build Status: {meta_info['primary_status']}")
    write_both(f"   Building Type: {meta_info['building_type']}")
    write_both(f"   Address: {meta_info['address']}")
    write_both(f"   Coordinates: {meta_info['lat']:.4f}°N, {meta_info['lon']:.4f}°E")

    campus_matches = df_matches[df_matches['meta_dc_code'] == dc_code]

    if len(campus_matches) == 0:
        write_both(f"\n   ❌ NO EXTERNAL SOURCE MATCHES FOUND")
        continue

    write_both(f"\n📡 EXTERNAL SOURCE COVERAGE:")
    write_both(f"   Total matches: {len(campus_matches)}")
    write_both(f"   Sources detected: {campus_matches['ext_source'].nunique()}")

    for source in sorted(campus_matches['ext_source'].unique()):
        if pd.notna(source):
            write_both(f"      ✓ {source}")

    write_both("\n" + "-"*80)
    write_both("DETAILED SOURCE-BY-SOURCE COMPARISON")
    write_both("-"*80)

    for source in sorted(campus_matches['ext_source'].unique()):
        if pd.isna(source):
            continue

        source_data = campus_matches[campus_matches['ext_source'] == source]

        write_both(f"\n📡 {source.upper()}")
        write_both(f"   Campus records: {len(source_data)}")

        unique_names = source_data['ext_campus_name'].dropna().unique()
        if len(unique_names) > 0:
            write_both(f"   Campus name(s):")
            for name in unique_names:
                write_both(f"      • {name}")

        mean_dist = source_data['distance_miles'].mean()
        median_dist = source_data['distance_miles'].median()
        min_dist = source_data['distance_miles'].min()
        max_dist = source_data['distance_miles'].max()

        write_both(f"\n   Geospatial Accuracy:")
        write_both(f"      Mean distance: {mean_dist:.2f} miles")
        write_both(f"      Median distance: {median_dist:.2f} miles")
        write_both(f"      Range: {min_dist:.2f} - {max_dist:.2f} miles")

        capacities = source_data['ext_capacity_mw'].dropna()
        if len(capacities) > 0:
            write_both(f"\n   Capacity Data:")
            write_both(f"      Meta actual: {meta_info['total_mw']:.1f} MW")
            for cap in capacities.unique():
                if cap > 0:
                    error_pct = abs(cap - meta_info['total_mw']) / meta_info['total_mw'] * 100
                    write_both(f"      {source} reported: {cap:.1f} MW (error: {error_pct:.1f}%)")
        else:
            write_both(f"\n   Capacity Data: Not reported")

        building_counts = source_data['ext_buildings'].dropna()
        if len(building_counts) > 0:
            write_both(f"\n   Building Count:")
            write_both(f"      Meta actual: {int(meta_info['building_count'])} buildings")
            for count in building_counts.unique():
                if count > 0:
                    write_both(f"      {source} reported: {int(count)} buildings")

        statuses = source_data['ext_status'].dropna().unique()
        if len(statuses) > 0:
            write_both(f"\n   Facility Status:")
            for status in statuses:
                write_both(f"      {status}")

        costs = source_data['ext_cost'].dropna()
        if len(costs) > 0 and costs.iloc[0] > 0:
            write_both(f"\n   Cost Estimate: ${costs.iloc[0]:,.0f}")

        acreage = source_data['ext_acreage'].dropna()
        if len(acreage) > 0 and acreage.iloc[0] > 0:
            write_both(f"   Site Acreage: {acreage.iloc[0]:,.1f} acres")

        cities = source_data['ext_city'].dropna().unique()
        states = source_data['ext_state'].dropna().unique()
        if len(cities) > 0:
            write_both(f"\n   Location:")
            for i, city in enumerate(cities):
                state = states[i] if i < len(states) else 'Unknown'
                write_both(f"      {city}, {state}")

# STEP 6: Summary Statistics
write_both("\n" + "="*80)
write_both("SUMMARY STATISTICS")
write_both("="*80)

df_matches['capacity_error_pct'] = np.where(
    (df_matches['ext_capacity_mw'].notna()) & (df_matches['ext_capacity_mw'] > 0),
    abs(df_matches['ext_capacity_mw'] - df_matches['meta_mw']) / df_matches['meta_mw'] * 100,
    None
)

write_both(f"\n📊 Overall Statistics:")
write_both(f"   Total comparisons: {len(df_matches)}")
write_both(f"   Mean distance: {df_matches['distance_miles'].mean():.2f} miles")
write_both(f"   Median distance: {df_matches['distance_miles'].median():.2f} miles")

capacity_comparisons = df_matches[df_matches['capacity_error_pct'].notna()]
if len(capacity_comparisons) > 0:
    write_both(f"\n   Capacity comparisons available: {len(capacity_comparisons)}")
    write_both(f"   Mean capacity error: {capacity_comparisons['capacity_error_pct'].mean():.1f}%")
    write_both(f"   Median capacity error: {capacity_comparisons['capacity_error_pct'].median():.1f}%")
    write_both(f"   Within 15% accuracy: {(capacity_comparisons['capacity_error_pct'] <= 15).sum()} ({(capacity_comparisons['capacity_error_pct'] <= 15).sum() / len(capacity_comparisons) * 100:.1f}%)")

write_both(f"\n📊 By Source:")
for source in sorted(df_matches['ext_source'].unique()):
    if pd.isna(source):
        continue

    source_data = df_matches[df_matches['ext_source'] == source]
    write_both(f"\n   {source}:")
    write_both(f"      Campuses detected: {source_data['meta_dc_code'].nunique()}")
    write_both(f"      Mean distance: {source_data['distance_miles'].mean():.2f} miles")

    source_capacity = source_data[source_data['capacity_error_pct'].notna()]
    if len(source_capacity) > 0:
        write_both(f"      Capacity comparisons: {len(source_capacity)}")
        write_both(f"      Mean capacity error: {source_capacity['capacity_error_pct'].mean():.1f}%")
        write_both(f"      Within 15%: {(source_capacity['capacity_error_pct'] <= 15).sum()} ({(source_capacity['capacity_error_pct'] <= 15).sum() / len(source_capacity) * 100:.1f}%)")

# STEP 7: Export to CSV
print("\n   Saving CSV files...")

summary_data = []
for dc_code in meta_campus_agg['dc_code']:
    meta_info = meta_campus_agg[meta_campus_agg['dc_code'] == dc_code].iloc[0]
    campus_matches = df_matches[df_matches['meta_dc_code'] == dc_code]

    summary_data.append({
        'dc_code': dc_code,
        'meta_mw': meta_info['total_mw'],
        'meta_buildings': meta_info['building_count'],
        'meta_region': meta_info['region'],
        'meta_status': meta_info['primary_status'],
        'sources_detected': campus_matches['ext_source'].nunique() if len(campus_matches) > 0 else 0,
        'total_matches': len(campus_matches),
        'avg_distance_miles': campus_matches['distance_miles'].mean() if len(campus_matches) > 0 else None,
        'min_distance_miles': campus_matches['distance_miles'].min() if len(campus_matches) > 0 else None
    })

df_summary = pd.DataFrame(summary_data)
df_summary = df_summary.sort_values('meta_mw', ascending=False)
df_summary.to_csv(csv_summary, index=False)

df_matches.to_csv(csv_detailed, index=False)

write_both("\n" + "="*80)
write_both(f"CAMPUS-LEVEL COMPARISON COMPLETE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
write_both("="*80)

txt_file.close()

print("\n" + "="*80)
print("📁 OUTPUT FILES CREATED")
print("="*80)
print(f"\n1. Detailed Analysis:")
print(f"   {txt_output}")
print(f"\n2. Summary by Campus:")
print(f"   {csv_summary}")
print(f"\n3. Detailed Matches:")
print(f"   {csv_detailed}")
print("\n" + "="*80)
print("✅ Campus-level comparison complete!")
print("="*80)
# CAMPUS-LEVEL DEEP DIVE COMPARISON - FIXED
import arcpy
import pandas as pd
import numpy as np
from datetime import datetime
import os

# Configuration
gdb_path = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
gold_campus_fc = f"{gdb_path}\\gold_campus"
meta_fc = f"{gdb_path}\\meta_canonical_v2"

# Output directory
output_dir = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\analysis_outputs"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Output files
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
txt_output = os.path.join(output_dir, f"campus_comparison_{timestamp}.txt")
csv_summary = os.path.join(output_dir, f"campus_comparison_summary_{timestamp}.csv")
csv_detailed = os.path.join(output_dir, f"campus_comparison_detailed_{timestamp}.csv")

# Conversion factor
METERS_TO_MILES = 0.000621371

print("="*80)
print(f"CAMPUS-LEVEL DEEP DIVE COMPARISON")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# Open text file for writing
txt_file = open(txt_output, 'w', encoding='utf-8')

def write_both(text):
    """Write to both console and file"""
    txt_file.write(text + '\n')
    print(text)

write_both("="*80)
write_both(f"CAMPUS-LEVEL COMPARISON: META CANONICAL VS EXTERNAL SOURCES")
write_both(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
write_both("="*80)
write_both("")

# Check if feature classes exist
if not arcpy.Exists(gold_campus_fc):
    error_msg = f"❌ ERROR: Gold campus feature class not found: {gold_campus_fc}"
    write_both(error_msg)
    txt_file.close()
    raise Exception(f"Feature class does not exist: {gold_campus_fc}")

if not arcpy.Exists(meta_fc):
    error_msg = f"❌ ERROR: Meta canonical feature class not found: {meta_fc}"
    write_both(error_msg)
    txt_file.close()
    raise Exception(f"Feature class does not exist: {meta_fc}")

print("   Checking data sources...")
write_both(f"✅ Found gold_campus: {arcpy.management.GetCount(gold_campus_fc)[0]} campus records")
write_both(f"✅ Found meta_canonical_v2: {arcpy.management.GetCount(meta_fc)[0]} building records")

# STEP 1: Aggregate Meta Data to Campus Level
write_both("\n" + "="*80)
write_both("STEP 1: AGGREGATING META DATA TO CAMPUS LEVEL")
write_both("="*80)

print("   Reading Meta canonical data...")

meta_fields = ['location_key', 'datacenter', 'suite', 'dc_code', 'region_derived',
               'it_load', 'new_build_status', 'building_type', 'address',
               'SHAPE@X', 'SHAPE@Y']

meta_data = []
with arcpy.da.SearchCursor(meta_fc, meta_fields) as cursor:
    for row in cursor:
        meta_data.append(row)

df_meta = pd.DataFrame(meta_data, columns=meta_fields)

print("   Aggregating to campus level...")

# Helper function for safe aggregation
def safe_mode(x):
    """Get mode value, handling empty series"""
    try:
        vc = x.value_counts()
        if len(vc) > 0:
            return vc.index[0]
        else:
            return 'Unknown'
    except:
        return 'Unknown'

def safe_building_type(x):
    """Get building type, handling mixed types"""
    try:
        unique_vals = x.dropna().unique()
        if len(unique_vals) > 1:
            return 'Mixed'
        elif len(unique_vals) == 1:
            return unique_vals[0]
        else:
            return 'Unknown'
    except:
        return 'Unknown'

meta_campus_agg = df_meta.groupby('dc_code').agg({
    'it_load': 'sum',
    'location_key': 'count',
    'region_derived': 'first',
    'address': 'first',
    'SHAPE@X': 'first',
    'SHAPE@Y': 'first',
    'building_type': safe_building_type,
    'new_build_status': safe_mode
}).reset_index()

meta_campus_agg.columns = ['dc_code', 'total_mw', 'building_count', 'region',
                           'address', 'lon', 'lat', 'building_type', 'primary_status']

# Filter to campuses with coordinates and MW data
meta_campus_agg = meta_campus_agg[
    (meta_campus_agg['total_mw'].notna()) &
    (meta_campus_agg['total_mw'] > 0) &
    (meta_campus_agg['lon'].notna()) &
    (meta_campus_agg['lat'].notna()) &
    (meta_campus_agg['lon'] != 0) &
    (meta_campus_agg['lat'] != 0)
].copy()

write_both(f"\n📊 Meta Campus Summary:")
write_both(f"   Total unique campuses: {len(meta_campus_agg)}")
write_both(f"   Total IT Load: {meta_campus_agg['total_mw'].sum():.1f} MW")
write_both(f"   Avg campus size: {meta_campus_agg['total_mw'].mean():.1f} MW")
write_both(f"   Avg buildings per campus: {meta_campus_agg['building_count'].mean():.1f}")

write_both(f"\n🏆 Top 15 Meta Campuses:")
top_15 = meta_campus_agg.sort_values('total_mw', ascending=False).head(15)
write_both("\n" + top_15[['dc_code', 'total_mw', 'building_count', 'region', 'primary_status']].to_string(index=False))

# STEP 2: Read Gold Campus Data
write_both("\n" + "="*80)
write_both("STEP 2: READING EXTERNAL CAMPUS DATA")
write_both("="*80)

print("   Reading gold_campus data...")

gold_fields = [f.name for f in arcpy.ListFields(gold_campus_fc)]

extract_fields = ['SHAPE@X', 'SHAPE@Y', 'campus_id', 'campus_name', 'source',
                  'company_clean', 'city', 'state', 'country', 'region',
                  'total_capacity_mw', 'total_cost', 'total_acreage',
                  'building_count', 'facility_status']

for year in range(2023, 2033):
    field_name = f'mw_{year}'
    if field_name in gold_fields:
        extract_fields.append(field_name)

extract_fields = [f for f in extract_fields if f in gold_fields or f.startswith('SHAPE@')]

gold_data = []
with arcpy.da.SearchCursor(gold_campus_fc, extract_fields) as cursor:
    for row in cursor:
        gold_data.append(row)

df_gold = pd.DataFrame(gold_data, columns=extract_fields)

if 'company_clean' in df_gold.columns:
    df_gold_meta = df_gold[df_gold['company_clean'] == 'Meta'].copy()
else:
    df_gold_meta = df_gold.copy()

write_both(f"\n📊 External Campus Data:")
write_both(f"   Total campus records: {len(df_gold)}")
write_both(f"   Meta campus records: {len(df_gold_meta)}")

if 'source' in df_gold_meta.columns:
    write_both(f"\n   By Source:")
    source_counts = df_gold_meta['source'].value_counts()
    for source, count in source_counts.items():
        write_both(f"      {source}: {count}")

# STEP 3: Spatial Join - Campus to Campus
write_both("\n" + "="*80)
write_both("STEP 3: PERFORMING CAMPUS-TO-CAMPUS SPATIAL MATCHING")
write_both("="*80)

print("   Calculating campus-to-campus distances...")

from math import radians, sin, cos, sqrt, atan2

comparison_data = []

for _, meta_campus in meta_campus_agg.iterrows():
    meta_dc = meta_campus['dc_code']
    meta_lon = meta_campus['lon']
    meta_lat = meta_campus['lat']
    meta_mw = meta_campus['total_mw']

    for _, ext_campus in df_gold_meta.iterrows():
        ext_lon = ext_campus['SHAPE@X'] if 'SHAPE@X' in ext_campus else None
        ext_lat = ext_campus['SHAPE@Y'] if 'SHAPE@Y' in ext_campus else None

        if ext_lon is None or ext_lat is None or pd.isna(ext_lon) or pd.isna(ext_lat):
            continue

        R = 6371000  # Earth radius in meters

        lat1 = radians(meta_lat)
        lon1 = radians(meta_lon)
        lat2 = radians(ext_lat)
        lon2 = radians(ext_lon)

        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        distance_m = R * c
        distance_miles = distance_m * METERS_TO_MILES

        if distance_miles <= 3.1:
            comparison_data.append({
                'meta_dc_code': meta_dc,
                'meta_mw': meta_mw,
                'meta_buildings': meta_campus['building_count'],
                'meta_region': meta_campus['region'],
                'meta_status': meta_campus['primary_status'],
                'meta_building_type': meta_campus['building_type'],
                'meta_address': meta_campus['address'],
                'meta_lat': meta_lat,
                'meta_lon': meta_lon,
                'ext_campus_id': ext_campus['campus_id'] if 'campus_id' in ext_campus else None,
                'ext_campus_name': ext_campus['campus_name'] if 'campus_name' in ext_campus else None,
                'ext_source': ext_campus['source'] if 'source' in ext_campus else None,
                'ext_capacity_mw': ext_campus['total_capacity_mw'] if 'total_capacity_mw' in ext_campus else None,
                'ext_buildings': ext_campus['building_count'] if 'building_count' in ext_campus else None,
                'ext_status': ext_campus['facility_status'] if 'facility_status' in ext_campus else None,
                'ext_city': ext_campus['city'] if 'city' in ext_campus else None,
                'ext_state': ext_campus['state'] if 'state' in ext_campus else None,
                'ext_cost': ext_campus['total_cost'] if 'total_cost' in ext_campus else None,
                'ext_acreage': ext_campus['total_acreage'] if 'total_acreage' in ext_campus else None,
                'distance_miles': distance_miles
            })

df_matches = pd.DataFrame(comparison_data)

write_both(f"\n✅ Found {len(df_matches)} campus-to-campus matches (within 3.1 miles)")

if len(df_matches) == 0:
    write_both("\n❌ No matches found - check coordinate systems")
    txt_file.close()
    raise Exception("No spatial matches found")

# STEP 4: Analyze Coverage by Campus
write_both("\n" + "="*80)
write_both("STEP 4: COVERAGE ANALYSIS")
write_both("="*80)

detected_campuses = df_matches['meta_dc_code'].unique()
all_campuses = meta_campus_agg['dc_code'].unique()
undetected_campuses = set(all_campuses) - set(detected_campuses)

write_both(f"\n📊 Detection Statistics:")
write_both(f"   Total Meta campuses: {len(all_campuses)}")
write_both(f"   Detected by external sources: {len(detected_campuses)} ({len(detected_campuses)/len(all_campuses)*100:.1f}%)")
write_both(f"   Undetected: {len(undetected_campuses)} ({len(undetected_campuses)/len(all_campuses)*100:.1f}%)")

if len(undetected_campuses) > 0:
    write_both(f"\n⚠️ Undetected Campuses:")
    for dc_code in sorted(undetected_campuses):
        campus_info = meta_campus_agg[meta_campus_agg['dc_code'] == dc_code].iloc[0]
        write_both(f"   {dc_code}: {campus_info['total_mw']:.1f} MW, {int(campus_info['building_count'])} buildings, {campus_info['primary_status']}")

write_both(f"\n📊 Coverage by Source:")
for source in df_matches['ext_source'].unique():
    if pd.isna(source):
        continue
    source_matches = df_matches[df_matches['ext_source'] == source]
    unique_campuses = source_matches['meta_dc_code'].nunique()
    write_both(f"   {source}: {unique_campuses} campuses ({unique_campuses/len(all_campuses)*100:.1f}%)")

# STEP 5: Campus-by-Campus Detailed Comparison (Top 15)
write_both("\n" + "="*80)
write_both("STEP 5: DETAILED CAMPUS COMPARISONS (Top 15)")
write_both("="*80)

top_15_codes = top_15['dc_code'].tolist()

for dc_code in top_15_codes:
    print(f"   Processing campus: {dc_code}")

    write_both("\n" + "="*80)
    write_both(f"🏢 CAMPUS: {dc_code}")
    write_both("="*80)

    meta_info = meta_campus_agg[meta_campus_agg['dc_code'] == dc_code].iloc[0]

    write_both(f"\n📊 META CANONICAL DATA:")
    write_both(f"   DC Code: {dc_code}")
    write_both(f"   Total IT Load: {meta_info['total_mw']:.1f} MW")
    write_both(f"   Number of Buildings: {int(meta_info['building_count'])}")
    write_both(f"   Region: {meta_info['region']}")
    write_both(f"   Build Status: {meta_info['primary_status']}")
    write_both(f"   Building Type: {meta_info['building_type']}")
    write_both(f"   Address: {meta_info['address']}")
    write_both(f"   Coordinates: {meta_info['lat']:.4f}°N, {meta_info['lon']:.4f}°E")

    campus_matches = df_matches[df_matches['meta_dc_code'] == dc_code]

    if len(campus_matches) == 0:
        write_both(f"\n   ❌ NO EXTERNAL SOURCE MATCHES FOUND")
        continue

    write_both(f"\n📡 EXTERNAL SOURCE COVERAGE:")
    write_both(f"   Total matches: {len(campus_matches)}")
    write_both(f"   Sources detected: {campus_matches['ext_source'].nunique()}")

    for source in sorted(campus_matches['ext_source'].unique()):
        if pd.notna(source):
            write_both(f"      ✓ {source}")

    write_both("\n" + "-"*80)
    write_both("DETAILED SOURCE-BY-SOURCE COMPARISON")
    write_both("-"*80)

    for source in sorted(campus_matches['ext_source'].unique()):
        if pd.isna(source):
            continue

        source_data = campus_matches[campus_matches['ext_source'] == source]

        write_both(f"\n📡 {source.upper()}")
        write_both(f"   Campus records: {len(source_data)}")

        unique_names = source_data['ext_campus_name'].dropna().unique()
        if len(unique_names) > 0:
            write_both(f"   Campus name(s):")
            for name in unique_names:
                write_both(f"      • {name}")

        mean_dist = source_data['distance_miles'].mean()
        median_dist = source_data['distance_miles'].median()
        min_dist = source_data['distance_miles'].min()
        max_dist = source_data['distance_miles'].max()

        write_both(f"\n   Geospatial Accuracy:")
        write_both(f"      Mean distance: {mean_dist:.2f} miles")
        write_both(f"      Median distance: {median_dist:.2f} miles")
        write_both(f"      Range: {min_dist:.2f} - {max_dist:.2f} miles")

        capacities = source_data['ext_capacity_mw'].dropna()
        if len(capacities) > 0:
            write_both(f"\n   Capacity Data:")
            write_both(f"      Meta actual: {meta_info['total_mw']:.1f} MW")
            for cap in capacities.unique():
                if cap > 0:
                    error_pct = abs(cap - meta_info['total_mw']) / meta_info['total_mw'] * 100
                    write_both(f"      {source} reported: {cap:.1f} MW (error: {error_pct:.1f}%)")
        else:
            write_both(f"\n   Capacity Data: Not reported")

        building_counts = source_data['ext_buildings'].dropna()
        if len(building_counts) > 0:
            write_both(f"\n   Building Count:")
            write_both(f"      Meta actual: {int(meta_info['building_count'])} buildings")
            for count in building_counts.unique():
                if count > 0:
                    write_both(f"      {source} reported: {int(count)} buildings")

        statuses = source_data['ext_status'].dropna().unique()
        if len(statuses) > 0:
            write_both(f"\n   Facility Status:")
            for status in statuses:
                write_both(f"      {status}")

        costs = source_data['ext_cost'].dropna()
        if len(costs) > 0 and costs.iloc[0] > 0:
            write_both(f"\n   Cost Estimate: ${costs.iloc[0]:,.0f}")

        acreage = source_data['ext_acreage'].dropna()
        if len(acreage) > 0 and acreage.iloc[0] > 0:
            write_both(f"   Site Acreage: {acreage.iloc[0]:,.1f} acres")

        cities = source_data['ext_city'].dropna().unique()
        states = source_data['ext_state'].dropna().unique()
        if len(cities) > 0:
            write_both(f"\n   Location:")
            for i, city in enumerate(cities):
                state = states[i] if i < len(states) else 'Unknown'
                write_both(f"      {city}, {state}")

# STEP 6: Summary Statistics
write_both("\n" + "="*80)
write_both("SUMMARY STATISTICS")
write_both("="*80)

df_matches['capacity_error_pct'] = np.where(
    (df_matches['ext_capacity_mw'].notna()) & (df_matches['ext_capacity_mw'] > 0),
    abs(df_matches['ext_capacity_mw'] - df_matches['meta_mw']) / df_matches['meta_mw'] * 100,
    None
)

write_both(f"\n📊 Overall Statistics:")
write_both(f"   Total comparisons: {len(df_matches)}")
write_both(f"   Mean distance: {df_matches['distance_miles'].mean():.2f} miles")
write_both(f"   Median distance: {df_matches['distance_miles'].median():.2f} miles")

capacity_comparisons = df_matches[df_matches['capacity_error_pct'].notna()]
if len(capacity_comparisons) > 0:
    write_both(f"\n   Capacity comparisons available: {len(capacity_comparisons)}")
    write_both(f"   Mean capacity error: {capacity_comparisons['capacity_error_pct'].mean():.1f}%")
    write_both(f"   Median capacity error: {capacity_comparisons['capacity_error_pct'].median():.1f}%")
    write_both(f"   Within 15% accuracy: {(capacity_comparisons['capacity_error_pct'] <= 15).sum()} ({(capacity_comparisons['capacity_error_pct'] <= 15).sum() / len(capacity_comparisons) * 100:.1f}%)")

write_both(f"\n📊 By Source:")
for source in sorted(df_matches['ext_source'].unique()):
    if pd.isna(source):
        continue

    source_data = df_matches[df_matches['ext_source'] == source]
    write_both(f"\n   {source}:")
    write_both(f"      Campuses detected: {source_data['meta_dc_code'].nunique()}")
    write_both(f"      Mean distance: {source_data['distance_miles'].mean():.2f} miles")

    source_capacity = source_data[source_data['capacity_error_pct'].notna()]
    if len(source_capacity) > 0:
        write_both(f"      Capacity comparisons: {len(source_capacity)}")
        write_both(f"      Mean capacity error: {source_capacity['capacity_error_pct'].mean():.1f}%")
        write_both(f"      Within 15%: {(source_capacity['capacity_error_pct'] <= 15).sum()} ({(source_capacity['capacity_error_pct'] <= 15).sum() / len(source_capacity) * 100:.1f}%)")

# STEP 7: Export to CSV
print("\n   Saving CSV files...")

summary_data = []
for dc_code in meta_campus_agg['dc_code']:
    meta_info = meta_campus_agg[meta_campus_agg['dc_code'] == dc_code].iloc[0]
    campus_matches = df_matches[df_matches['meta_dc_code'] == dc_code]

    summary_data.append({
        'dc_code': dc_code,
        'meta_mw': meta_info['total_mw'],
        'meta_buildings': meta_info['building_count'],
        'meta_region': meta_info['region'],
        'meta_status': meta_info['primary_status'],
        'sources_detected': campus_matches['ext_source'].nunique() if len(campus_matches) > 0 else 0,
        'total_matches': len(campus_matches),
        'avg_distance_miles': campus_matches['distance_miles'].mean() if len(campus_matches) > 0 else None,
        'min_distance_miles': campus_matches['distance_miles'].min() if len(campus_matches) > 0 else None
    })

df_summary = pd.DataFrame(summary_data)
df_summary = df_summary.sort_values('meta_mw', ascending=False)
df_summary.to_csv(csv_summary, index=False)

df_matches.to_csv(csv_detailed, index=False)

write_both("\n" + "="*80)
write_both(f"CAMPUS-LEVEL COMPARISON COMPLETE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
write_both("="*80)

txt_file.close()

print("\n" + "="*80)
print("📁 OUTPUT FILES CREATED")
print("="*80)
print(f"\n1. Detailed Analysis:")
print(f"   {txt_output}")
print(f"\n2. Summary by Campus:")
print(f"   {csv_summary}")
print(f"\n3. Detailed Matches:")
print(f"   {csv_detailed}")
print("\n" + "="*80)
print("✅ Campus-level comparison complete!")
print("="*80)
