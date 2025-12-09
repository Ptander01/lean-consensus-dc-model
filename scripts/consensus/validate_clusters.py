"""
CLUSTER VALIDATION SCRIPT
Validates external source clusters against Meta canonical locations
Identifies: matches, false positives, and coverage gaps
Author: AI Assistant for Data Center Consensus Project
"""

import arcpy
import pandas as pd
from datetime import datetime
import math

# Configuration
gdb_path = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
gold_campus_fc = f"{gdb_path}\\gold_campus"
meta_canonical_fc = f"{gdb_path}\\meta_canonical_v2"
output_csv = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\analysis_outputs\cluster_validation_{}.csv"
output_txt = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\analysis_outputs\cluster_validation_report_{}.txt"

MATCH_DISTANCE_KM = 5.0

print("=" * 80)
print("🔍 CLUSTER VALIDATION ANALYSIS")
print("=" * 80)
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

def haversine_distance(lon1, lat1, lon2, lat2):
    """Calculate distance in km between two lat/lon points"""
    R = 6371  # Earth radius in km
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return R * c

# Step 1: Load cluster data from gold_campus
print("📊 Step 1: Loading cluster data from gold_campus...")
cluster_data = {}

fields = ['cluster_id', 'cluster_campus_name', 'cluster_source_count',
          'source', 'SHAPE@X', 'SHAPE@Y', 'commissioned_power_mw',
          'full_capacity_mw', 'mw_2032']

with arcpy.da.SearchCursor(gold_campus_fc, fields,
                           where_clause="cluster_id IS NOT NULL AND company_clean LIKE '%Meta%'") as cursor:
    for row in cursor:
        cluster_id, campus_name, source_count, source, x, y, comm_mw, full_mw, mw_2032 = row

        if cluster_id not in cluster_data:
            cluster_data[cluster_id] = {
                'cluster_id': cluster_id,
                'facility_name': campus_name,
                'source_count': source_count,
                'sources': [],
                'coords': [],
                'commissioned_mw': [],
                'full_capacity_mw': [],
                'mw_2032': []
            }

        cluster_data[cluster_id]['sources'].append(source)
        cluster_data[cluster_id]['coords'].append((x, y))
        if comm_mw and comm_mw > 0:
            cluster_data[cluster_id]['commissioned_mw'].append(comm_mw)
        if full_mw and full_mw > 0:
            cluster_data[cluster_id]['full_capacity_mw'].append(full_mw)
        if mw_2032 and mw_2032 > 0:
            cluster_data[cluster_id]['mw_2032'].append(mw_2032)

# Calculate cluster centroids
for cid, data in cluster_data.items():
    coords = data['coords']
    centroid_x = sum(c[0] for c in coords) / len(coords)
    centroid_y = sum(c[1] for c in coords) / len(coords)
    data['centroid'] = (centroid_x, centroid_y)
    data['sources_list'] = ', '.join(sorted(set(data['sources'])))

print(f"   ✅ Loaded {len(cluster_data)} clusters")

# Step 2: Load Meta canonical campus data
print("\n📊 Step 2: Loading Meta canonical campus data...")
meta_campuses = {}

# Aggregate meta_canonical_v2 to campus level
fields = ['location_key', 'SHAPE@X', 'SHAPE@Y', 'it_load', 'build_status']
with arcpy.da.SearchCursor(meta_canonical_fc, fields,
                           where_clause="SHAPE@X IS NOT NULL AND SHAPE@Y IS NOT NULL AND SHAPE@X <> 0") as cursor:
    for row in cursor:
        location_key, x, y, it_load, build_status = row

        # Extract campus code (first 3 chars)
        campus_code = location_key[:3] if location_key and len(location_key) >= 3 else location_key

        if campus_code not in meta_campuses:
            meta_campuses[campus_code] = {
                'campus_code': campus_code,
                'building_count': 0,
                'total_it_load': 0,
                'coords': [],
                'build_status': set()
            }

        meta_campuses[campus_code]['building_count'] += 1
        meta_campuses[campus_code]['total_it_load'] += (it_load or 0)
        meta_campuses[campus_code]['coords'].append((x, y))
        if build_status:
            meta_campuses[campus_code]['build_status'].add(build_status)

# Calculate Meta campus centroids
for campus_code, data in meta_campuses.items():
    coords = data['coords']
    centroid_x = sum(c[0] for c in coords) / len(coords)
    centroid_y = sum(c[1] for c in coords) / len(coords)
    data['centroid'] = (centroid_x, centroid_y)
    data['build_status_list'] = ', '.join(sorted(data['build_status']))

print(f"   ✅ Loaded {len(meta_campuses)} Meta campuses")

# Step 3: Spatial matching
print(f"\n🔗 Step 3: Matching clusters to Meta campuses (within {MATCH_DISTANCE_KM} km)...")

matches = []
unmatched_clusters = []
matched_meta_campuses = set()

for cid, cluster in cluster_data.items():
    cluster_x, cluster_y = cluster['centroid']
    best_match = None
    best_distance = float('inf')

    # Find closest Meta campus
    for campus_code, meta in meta_campuses.items():
        meta_x, meta_y = meta['centroid']
        distance = haversine_distance(cluster_x, cluster_y, meta_x, meta_y)

        if distance < best_distance and distance <= MATCH_DISTANCE_KM:
            best_distance = distance
            best_match = campus_code

    if best_match:
        matches.append({
            'cluster_id': cid,
            'cluster_name': cluster['facility_name'],
            'source_count': cluster['source_count'],
            'sources': cluster['sources_list'],
            'meta_campus': best_match,
            'meta_buildings': meta_campuses[best_match]['building_count'],
            'meta_it_load': meta_campuses[best_match]['total_it_load'],
            'meta_build_status': meta_campuses[best_match]['build_status_list'],
            'distance_km': round(best_distance, 2),
            'match_quality': 'Excellent' if best_distance < 1 else 'Good' if best_distance < 3 else 'Fair'
        })
        matched_meta_campuses.add(best_match)
    else:
        unmatched_clusters.append({
            'cluster_id': cid,
            'cluster_name': cluster['facility_name'],
            'source_count': cluster['source_count'],
            'sources': cluster['sources_list'],
            'reason': 'No Meta campus within 5 km'
        })

# Find unmatched Meta campuses
unmatched_meta = [
    {
        'campus_code': code,
        'building_count': data['building_count'],
        'it_load_mw': data['total_it_load'],
        'build_status': data['build_status_list']
    }
    for code, data in meta_campuses.items()
    if code not in matched_meta_campuses
]

print(f"   ✅ Matched {len(matches)} clusters to Meta campuses")
print(f"   ⚠️  {len(unmatched_clusters)} clusters with NO Meta match")
print(f"   🚨 {len(unmatched_meta)} Meta campuses NOT detected by external sources")

# Step 4: Analyze single-source clusters
print("\n📊 Step 4: Analyzing single-source clusters...")
single_source_clusters = [m for m in matches if m['source_count'] == 1]
single_source_unmatched = [u for u in unmatched_clusters if u['source_count'] == 1]

print(f"   Single-source clusters that MATCH Meta: {len(single_source_clusters)}")
print(f"   Single-source clusters with NO match: {len(single_source_unmatched)} (likely false positives)")

# Step 5: Generate reports
print("\n📝 Step 5: Generating validation reports...")

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

# CSV Export
df_matches = pd.DataFrame(matches)
df_unmatched_clusters = pd.DataFrame(unmatched_clusters)
df_unmatched_meta = pd.DataFrame(unmatched_meta)

csv_path = output_csv.format(timestamp)
with pd.ExcelWriter(csv_path.replace('.csv', '.xlsx'), engine='openpyxl') as writer:
    df_matches.to_excel(writer, sheet_name='Matched_Clusters', index=False)
    df_unmatched_clusters.to_excel(writer, sheet_name='Unmatched_Clusters', index=False)
    df_unmatched_meta.to_excel(writer, sheet_name='Unmatched_Meta_Campuses', index=False)

# Also save as CSV for easier access
df_matches.to_csv(csv_path, index=False)

# Text Report
txt_path = output_txt.format(timestamp)
with open(txt_path, 'w', encoding='utf-8') as f:
    f.write("=" * 80 + "\n")
    f.write("CLUSTER VALIDATION REPORT\n")
    f.write("=" * 80 + "\n")
    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

    f.write("EXECUTIVE SUMMARY\n")
    f.write("-" * 80 + "\n")
    f.write(f"Total external clusters: {len(cluster_data)}\n")
    f.write(f"Total Meta campuses (canonical): {len(meta_campuses)}\n")
    f.write(f"Clusters matched to Meta: {len(matches)} ({len(matches)/len(cluster_data)*100:.1f}%)\n")
    f.write(f"Meta campuses detected: {len(matched_meta_campuses)} of {len(meta_campuses)} ({len(matched_meta_campuses)/len(meta_campuses)*100:.1f}%)\n\n")

    f.write("MATCH QUALITY BREAKDOWN\n")
    f.write("-" * 80 + "\n")
    excellent = sum(1 for m in matches if m['match_quality'] == 'Excellent')
    good = sum(1 for m in matches if m['match_quality'] == 'Good')
    fair = sum(1 for m in matches if m['match_quality'] == 'Fair')
    f.write(f"Excellent matches (<1 km): {excellent}\n")
    f.write(f"Good matches (1-3 km): {good}\n")
    f.write(f"Fair matches (3-5 km): {fair}\n\n")

    f.write("UNMATCHED CLUSTERS (Potential False Positives)\n")
    f.write("-" * 80 + "\n")
    f.write(f"Total unmatched: {len(unmatched_clusters)}\n")
    f.write(f"Single-source unmatched: {len(single_source_unmatched)}\n\n")

    if single_source_unmatched:
        f.write("Single-source clusters with NO Meta match:\n")
        for u in single_source_unmatched:
            f.write(f"  • {u['cluster_name']} (Source: {u['sources']})\n")
    f.write("\n")

    f.write("UNDETECTED META CAMPUSES (Coverage Gaps)\n")
    f.write("-" * 80 + "\n")
    f.write(f"Total undetected: {len(unmatched_meta)}\n\n")

    if unmatched_meta:
        for u in sorted(unmatched_meta, key=lambda x: x['it_load_mw'], reverse=True):
            f.write(f"  • {u['campus_code']}: {u['it_load_mw']:.1f} MW, {u['building_count']} buildings ({u['build_status']})\n")
    f.write("\n")

    f.write("TOP MATCHES (Multi-Source High Confidence)\n")
    f.write("-" * 80 + "\n")
    top_matches = sorted([m for m in matches if m['source_count'] >= 3],
                         key=lambda x: x['source_count'], reverse=True)[:15]
    for m in top_matches:
        f.write(f"  • {m['cluster_name']} → {m['meta_campus']}\n")
        f.write(f"    Sources: {m['source_count']} ({m['sources']})\n")
        f.write(f"    Distance: {m['distance_km']} km, Meta IT Load: {m['meta_it_load']:.1f} MW\n")

print(f"   ✅ Excel saved: {csv_path.replace('.csv', '.xlsx')}")
print(f"   ✅ CSV saved: {csv_path}")
print(f"   ✅ Report saved: {txt_path}")

# Print summary to console
print("\n" + "=" * 80)
print("📊 VALIDATION SUMMARY")
print("=" * 80)
print(f"Total clusters: {len(cluster_data)}")
print(f"Matched to Meta: {len(matches)} ({len(matches)/len(cluster_data)*100:.1f}%)")
print(f"Unmatched clusters: {len(unmatched_clusters)} ({len(unmatched_clusters)/len(cluster_data)*100:.1f}%)")
print(f"\nMeta campuses detected: {len(matched_meta_campuses)} of {len(meta_campuses)} ({len(matched_meta_campuses)/len(meta_campuses)*100:.1f}%)")
print(f"Undetected Meta campuses: {len(unmatched_meta)}")

print("\n🚨 SINGLE-SOURCE UNMATCHED (Likely False Positives):")
print(f"   Count: {len(single_source_unmatched)}")
if single_source_unmatched:
    for u in single_source_unmatched[:10]:
        print(f"   • {u['cluster_name']} ({u['sources']})")
    if len(single_source_unmatched) > 10:
        print(f"   ... and {len(single_source_unmatched) - 10} more")

print("\n🔍 UNDETECTED META CAMPUSES:")
if unmatched_meta:
    for u in sorted(unmatched_meta, key=lambda x: x['it_load_mw'], reverse=True)[:10]:
        print(f"   • {u['campus_code']}: {u['it_load_mw']:.1f} MW, {u['building_count']} buildings")

print("\n" + "=" * 80)
print("✅ CLUSTER VALIDATION COMPLETE")
print("=" * 80)
print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
