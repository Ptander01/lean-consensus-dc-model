"""
SPATIAL CLUSTERING SCRIPT
Clusters gold_campus Meta records within 1.5 km to identify unique facilities
Author: AI Assistant for Data Center Consensus Project
"""

import arcpy
import pandas as pd
from datetime import datetime
import math

# Configuration
gdb_path = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
gold_campus_fc = f"{gdb_path}\\gold_campus"
output_csv = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\analysis_outputs\cluster_results_{}.csv"

# Source priority for cluster naming (best spatial accuracy first)
SOURCE_PRIORITY = {
    'DataCenterMap': 1,
    'Semianalysis': 2,
    'DataCenterHawk': 3,
    'NewProjectMedia': 4,
    'WoodMac': 5,
    'Synergy': 6
}

CLUSTER_DISTANCE_KM = 2.5

print("=" * 80)
print("🔍 SPATIAL CLUSTERING - META FACILITIES")
print("=" * 80)
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# Step 1: Add cluster fields if they don't exist
print("📝 Step 1: Adding cluster fields to gold_campus...")
fields_to_add = [
    ('cluster_id', 'LONG', 'Unique cluster identifier'),
    ('cluster_source_count', 'SHORT', 'Number of sources in cluster'),
    ('cluster_campus_name', 'TEXT', 'Primary facility name', 255)
]

existing_fields = [f.name for f in arcpy.ListFields(gold_campus_fc)]

for field_name, field_type, alias, *length in fields_to_add:
    if field_name not in existing_fields:
        if length:
            arcpy.AddField_management(gold_campus_fc, field_name, field_type, field_alias=alias, field_length=length[0])
        else:
            arcpy.AddField_management(gold_campus_fc, field_name, field_type, field_alias=alias)
        print(f"   ✅ Added: {field_name}")
    else:
        print(f"   ⚠️  Already exists: {field_name}")

# Step 2: Extract Meta records with coordinates
print("\n📊 Step 2: Extracting Meta records...")
meta_records = []

fields = ['OBJECTID', 'campus_name', 'source', 'company_clean', 'SHAPE@X', 'SHAPE@Y']
with arcpy.da.SearchCursor(gold_campus_fc, fields) as cursor:
    for row in cursor:
        oid, campus_name, source, company_clean, x, y = row
        if company_clean and 'meta' in company_clean.lower() and x and y:
            meta_records.append({
                'OBJECTID': oid,
                'campus_name': campus_name,
                'source': source,
                'x': x,
                'y': y,
                'cluster_id': None,
                'priority': SOURCE_PRIORITY.get(source, 99)
            })

print(f"   ✅ Found {len(meta_records)} Meta records with coordinates")

# Step 3: Calculate distances and cluster
print(f"\n🔗 Step 3: Clustering records within {CLUSTER_DISTANCE_KM} km...")

def haversine_distance(lon1, lat1, lon2, lat2):
    """Calculate distance in km between two lat/lon points"""
    R = 6371  # Earth radius in km
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return R * c

# Sort by source priority (best sources seed clusters)
meta_records.sort(key=lambda x: x['priority'])

cluster_id = 1
clustered_oids = set()

for i, record in enumerate(meta_records):
    if record['OBJECTID'] in clustered_oids:
        continue

    # Start new cluster
    cluster_members = [record]
    clustered_oids.add(record['OBJECTID'])

    # Find all records within distance threshold
    for j, other_record in enumerate(meta_records):
        if other_record['OBJECTID'] in clustered_oids:
            continue

        distance = haversine_distance(
            record['x'], record['y'],
            other_record['x'], other_record['y']
        )

        if distance <= CLUSTER_DISTANCE_KM:
            cluster_members.append(other_record)
            clustered_oids.add(other_record['OBJECTID'])

    # Assign cluster ID to all members
    for member in cluster_members:
        member['cluster_id'] = cluster_id

    cluster_id += 1

total_clusters = cluster_id - 1
print(f"   ✅ Created {total_clusters} facility clusters from {len(meta_records)} records")

# Step 4: Calculate cluster statistics
print("\n📈 Step 4: Calculating cluster statistics...")
cluster_stats = {}

for record in meta_records:
    cid = record['cluster_id']
    if cid not in cluster_stats:
        cluster_stats[cid] = {
            'members': [],
            'sources': set(),
            'names': []
        }

    cluster_stats[cid]['members'].append(record)
    cluster_stats[cid]['sources'].add(record['source'])
    cluster_stats[cid]['names'].append(record['campus_name'])

# Determine primary name for each cluster (most common, tiebreak by priority)
for cid, stats in cluster_stats.items():
    # Count name occurrences
    name_counts = {}
    for name in stats['names']:
        if name:
            name_counts[name] = name_counts.get(name, 0) + 1

    # Get most common name, tiebreak by first occurrence (which is sorted by priority)
    if name_counts:
        primary_name = max(name_counts.items(), key=lambda x: (x[1], -stats['names'].index(x[0])))[0]
    else:
        primary_name = f"Cluster_{cid}"

    stats['primary_name'] = primary_name
    stats['source_count'] = len(stats['sources'])

# Step 5: Update gold_campus with cluster assignments
print("\n💾 Step 5: Updating gold_campus with cluster assignments...")
update_count = 0

with arcpy.da.UpdateCursor(gold_campus_fc,
                           ['OBJECTID', 'cluster_id', 'cluster_source_count', 'cluster_campus_name']) as cursor:
    for row in cursor:
        oid = row[0]

        # Find this record's cluster
        record_cluster = next((r for r in meta_records if r['OBJECTID'] == oid), None)

        if record_cluster and record_cluster['cluster_id']:
            cid = record_cluster['cluster_id']
            stats = cluster_stats[cid]

            row[1] = cid  # cluster_id
            row[2] = stats['source_count']  # cluster_source_count
            row[3] = stats['primary_name']  # cluster_campus_name

            cursor.updateRow(row)
            update_count += 1

print(f"   ✅ Updated {update_count} records")

# Step 6: Generate summary report
print("\n📊 Step 6: Generating cluster summary report...")

# Create DataFrame for analysis
cluster_data = []
for cid, stats in sorted(cluster_stats.items()):
    cluster_data.append({
        'cluster_id': cid,
        'facility_name': stats['primary_name'],
        'source_count': stats['source_count'],
        'sources': ', '.join(sorted(stats['sources'])),
        'record_count': len(stats['members'])
    })

df = pd.DataFrame(cluster_data)

# Save to CSV
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
csv_path = output_csv.format(timestamp)
df.to_csv(csv_path, index=False)

# Print summary statistics
print("\n" + "=" * 80)
print("📊 CLUSTERING SUMMARY")
print("=" * 80)
print(f"Total Meta records processed: {len(meta_records)}")
print(f"Total facility clusters: {total_clusters}")
print(f"Average sources per cluster: {df['source_count'].mean():.1f}")
print(f"Clusters with 1 source: {(df['source_count'] == 1).sum()}")
print(f"Clusters with 2+ sources: {(df['source_count'] >= 2).sum()}")
print(f"Clusters with 3+ sources: {(df['source_count'] >= 3).sum()}")

print("\n🏆 Top Multi-Source Clusters:")
top_clusters = df.nlargest(10, 'source_count')[['facility_name', 'source_count', 'sources']]
for idx, row in top_clusters.iterrows():
    print(f"   • {row['facility_name']}: {row['source_count']} sources ({row['sources']})")

print("\n📁 Output saved to:")
print(f"   {csv_path}")

print("\n" + "=" * 80)
print("✅ SPATIAL CLUSTERING COMPLETE")
print("=" * 80)
print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
