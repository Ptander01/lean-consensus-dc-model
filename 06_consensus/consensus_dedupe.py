"""
Consensus Dedupe Script
Creates deduplicated consensus layers for gold_campus and gold_buildings.

Purpose:
- Identifies overlapping records across vendors within spatial threshold
- Selects "best" record based on source priority and data completeness
- Creates clean consensus_buildings and consensus_campus layers for dashboarding

Logic:
1. Group records by campus_id (or spatial proximity if no campus_id match)
2. For each group, score records based on:
   - Source priority (DataCenterHawk > Semianalysis > others)
   - Spatial accuracy weight
   - Field completeness (capacity, sqft, timeline data)
3. Select highest-scoring record as "consensus" record
4. Merge complementary fields from other sources

Author: Meta Data Center GIS Team
Date: December 10, 2024
"""

import arcpy
import pandas as pd
import numpy as np
from datetime import datetime
from collections import defaultdict
import math
import os

# ============================================================================
# CONFIGURATION
# ============================================================================

GDB = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
GOLD_BUILDINGS = os.path.join(GDB, "gold_buildings")
GOLD_CAMPUS = os.path.join(GDB, "gold_campus")

# Output feature classes
CONSENSUS_BUILDINGS = os.path.join(GDB, "consensus_buildings")
CONSENSUS_CAMPUS = os.path.join(GDB, "consensus_campus")

# Spatial clustering threshold (meters)
CLUSTER_THRESHOLD_M = 1000  # Records within 1km are considered same location

# Source priority weights (based on Dec 10 spatial accuracy analysis)
SOURCE_WEIGHTS = {
    'DataCenterHawk': 1.0,    # Best accuracy (233m median)
    'Semianalysis': 0.95,     # Second best (307m median)
    'DataCenterMap': 0.7,     # Moderate (677m median)
    'NewProjectMedia': 0.5,   # Limited, US-only
    'WoodMac': 0.3,           # Limited coverage
    'Synergy': 0.0            # Excluded from spatial consensus
}

# Fields to use for scoring completeness
COMPLETENESS_FIELDS = {
    'capacity': ['commissioned_power_mw', 'full_capacity_mw', 'mw_2032'],
    'facility': ['facility_sqft'],
    'timeline': ['actual_live_date', 'construction_started', 'cod'],
    'cost': ['total_cost_usd_million'],
    'land': ['total_site_acres']
}

# Fields to merge from secondary sources
MERGE_FIELDS = [
    'mw_2023', 'mw_2024', 'mw_2025', 'mw_2026', 'mw_2027',
    'mw_2028', 'mw_2029', 'mw_2030', 'mw_2031', 'mw_2032',
    'total_cost_usd_million', 'land_cost_usd_million',
    'total_site_acres', 'data_center_acres',
    'announced', 'construction_started', 'cod'
]

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate geodesic distance in meters using Haversine formula."""
    if any(v is None for v in [lat1, lon1, lat2, lon2]):
        return float('inf')

    R = 6371000  # Earth's radius in meters
    lat1_rad, lat2_rad = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def calculate_completeness_score(record, field_categories):
    """Calculate completeness score (0-1) based on non-null fields."""
    total_fields = 0
    filled_fields = 0

    for category, fields in field_categories.items():
        for field in fields:
            if field in record:
                total_fields += 1
                if record[field] is not None and record[field] != '':
                    filled_fields += 1

    return filled_fields / total_fields if total_fields > 0 else 0


def calculate_consensus_score(record, source_weights, completeness_fields):
    """
    Calculate overall consensus score for a record.

    Score = (source_weight * 0.5) + (completeness_score * 0.3) + (has_capacity * 0.2)
    """
    source = record.get('source', '')
    source_weight = source_weights.get(source, 0.1)

    completeness = calculate_completeness_score(record, completeness_fields)

    # Bonus for having capacity data
    has_capacity = 0
    if record.get('commissioned_power_mw') or record.get('full_capacity_mw'):
        has_capacity = 1

    score = (source_weight * 0.5) + (completeness * 0.3) + (has_capacity * 0.2)
    return score


def cluster_by_proximity(records, threshold_m):
    """
    Cluster records by spatial proximity.
    Returns list of clusters, each cluster is a list of record indices.
    """
    n = len(records)
    if n == 0:
        return []

    # Initialize: each record in its own cluster
    cluster_ids = list(range(n))

    # Merge clusters for records within threshold
    for i in range(n):
        for j in range(i + 1, n):
            if cluster_ids[i] != cluster_ids[j]:
                lat1, lon1 = records[i].get('latitude'), records[i].get('longitude')
                lat2, lon2 = records[j].get('latitude'), records[j].get('longitude')

                dist = haversine_distance(lat1, lon1, lat2, lon2)

                if dist <= threshold_m:
                    # Merge clusters
                    old_cluster = cluster_ids[j]
                    new_cluster = cluster_ids[i]
                    for k in range(n):
                        if cluster_ids[k] == old_cluster:
                            cluster_ids[k] = new_cluster

    # Group by cluster ID
    clusters = defaultdict(list)
    for idx, cid in enumerate(cluster_ids):
        clusters[cid].append(idx)

    return list(clusters.values())


def merge_complementary_fields(primary_record, secondary_records, merge_fields):
    """
    Merge non-null fields from secondary records into primary.
    Only fills in fields that are null in primary.
    """
    merged = primary_record.copy()

    for field in merge_fields:
        if field not in merged or merged[field] is None or merged[field] == '':
            # Look for value in secondary records (in priority order)
            for secondary in secondary_records:
                if field in secondary and secondary[field] is not None and secondary[field] != '':
                    merged[field] = secondary[field]
                    if 'merged_from' not in merged:
                        merged['merged_from'] = []
                    merged['merged_from'].append(f"{field}:{secondary.get('source', 'unknown')}")
                    break

    return merged


def get_all_fields(fc):
    """Get list of all field names in feature class."""
    return [f.name for f in arcpy.ListFields(fc)]


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def create_consensus_buildings():
    """
    Create deduplicated consensus_buildings layer.
    """

    print("=" * 80)
    print("CREATING CONSENSUS BUILDINGS LAYER")
    print("=" * 80)
    print(f"Started: {datetime.now()}")
    print(f"Source: {GOLD_BUILDINGS}")
    print(f"Output: {CONSENSUS_BUILDINGS}")
    print(f"Cluster threshold: {CLUSTER_THRESHOLD_M}m")
    print()

    # ========================================================================
    # Step 1: Read all records from gold_buildings
    # ========================================================================

    print("[1/5] Reading gold_buildings...")

    all_fields = get_all_fields(GOLD_BUILDINGS)

    # Exclude system fields
    exclude_fields = ['OBJECTID', 'Shape', 'Shape_Length', 'Shape_Area']
    read_fields = [f for f in all_fields if f not in exclude_fields]
    read_fields.append('SHAPE@XY')

    records = []
    with arcpy.da.SearchCursor(GOLD_BUILDINGS, read_fields) as cursor:
        field_names = read_fields[:-1]  # Exclude SHAPE@XY from names
        for row in cursor:
            record = {}
            for i, field in enumerate(field_names):
                record[field] = row[i]
            # Add coordinates from shape
            shape_xy = row[-1]
            if shape_xy:
                record['shape_x'] = shape_xy[0]
                record['shape_y'] = shape_xy[1]
            records.append(record)

    print(f"   Loaded {len(records)} records")

    # ========================================================================
    # Step 2: Group by campus_id
    # ========================================================================

    print("[2/5] Grouping by campus_id...")

    campus_groups = defaultdict(list)
    no_campus_id = []

    for idx, record in enumerate(records):
        campus_id = record.get('campus_id')
        if campus_id:
            campus_groups[campus_id].append(idx)
        else:
            no_campus_id.append(idx)

    print(f"   Found {len(campus_groups)} unique campus_ids")
    print(f"   Records without campus_id: {len(no_campus_id)}")

    # For records without campus_id, cluster by proximity
    if no_campus_id:
        print("   Clustering records without campus_id by proximity...")
        no_campus_records = [records[i] for i in no_campus_id]
        proximity_clusters = cluster_by_proximity(no_campus_records, CLUSTER_THRESHOLD_M)

        for cluster_indices in proximity_clusters:
            # Create synthetic campus_id
            cluster_records = [no_campus_id[i] for i in cluster_indices]
            synthetic_id = f"SYNTHETIC_{min(cluster_records)}"
            campus_groups[synthetic_id].extend(cluster_records)

    total_groups = len(campus_groups)
    print(f"   Total groups to process: {total_groups}")

    # ========================================================================
    # Step 3: Score and select best record per group
    # ========================================================================

    print("[3/5] Scoring and selecting consensus records...")

    consensus_records = []
    multi_source_groups = 0
    merged_count = 0

    for campus_id, record_indices in campus_groups.items():
        group_records = [records[i] for i in record_indices]

        # Calculate scores for each record
        scored = []
        for rec in group_records:
            score = calculate_consensus_score(rec, SOURCE_WEIGHTS, COMPLETENESS_FIELDS)
            scored.append((score, rec))

        # Sort by score descending
        scored.sort(key=lambda x: -x[0])

        # Select primary (highest scoring)
        primary = scored[0][1]

        # Track multi-source groups
        sources_in_group = set(r.get('source') for _, r in scored)
        if len(sources_in_group) > 1:
            multi_source_groups += 1

        # Merge complementary fields from secondary records
        if len(scored) > 1:
            secondary = [r for _, r in scored[1:]]
            # Sort secondary by source weight
            secondary.sort(key=lambda x: -SOURCE_WEIGHTS.get(x.get('source', ''), 0))
            primary = merge_complementary_fields(primary, secondary, MERGE_FIELDS)
            merged_count += 1

        # Add consensus metadata
        primary['consensus_score'] = scored[0][0]
        primary['source_count'] = len(sources_in_group)
        primary['sources_available'] = ', '.join(sorted(sources_in_group))

        consensus_records.append(primary)

    print(f"   Selected {len(consensus_records)} consensus records")
    print(f"   Multi-source groups: {multi_source_groups}")
    print(f"   Records with merged data: {merged_count}")

    # ========================================================================
    # Step 4: Create output feature class
    # ========================================================================

    print("[4/5] Creating output feature class...")

    # Delete if exists
    if arcpy.Exists(CONSENSUS_BUILDINGS):
        arcpy.Delete_management(CONSENSUS_BUILDINGS)

    # Create feature class
    arcpy.CreateFeatureclass_management(
        out_path=os.path.dirname(CONSENSUS_BUILDINGS),
        out_name=os.path.basename(CONSENSUS_BUILDINGS),
        geometry_type="POINT",
        spatial_reference=arcpy.SpatialReference(4326)  # WGS84
    )

    # Add fields (copy from gold_buildings)
    source_fields = [f for f in arcpy.ListFields(GOLD_BUILDINGS)
                     if f.name not in ['OBJECTID', 'Shape', 'Shape_Length', 'Shape_Area']]

    for field in source_fields:
        try:
            arcpy.AddField_management(
                CONSENSUS_BUILDINGS,
                field.name,
                field.type,
                field_length=field.length if field.type == 'String' else None
            )
        except:
            pass  # Field may already exist

    # Add consensus metadata fields
    arcpy.AddField_management(CONSENSUS_BUILDINGS, "consensus_score", "DOUBLE")
    arcpy.AddField_management(CONSENSUS_BUILDINGS, "source_count", "SHORT")
    arcpy.AddField_management(CONSENSUS_BUILDINGS, "sources_available", "TEXT", field_length=200)

    # Insert records
    insert_fields = [f.name for f in arcpy.ListFields(CONSENSUS_BUILDINGS)
                     if f.name not in ['OBJECTID', 'Shape']]
    insert_fields.append('SHAPE@XY')

    insert_count = 0
    with arcpy.da.InsertCursor(CONSENSUS_BUILDINGS, insert_fields) as cursor:
        for record in consensus_records:
            try:
                row = []
                for field in insert_fields[:-1]:  # Exclude SHAPE@XY
                    row.append(record.get(field))

                # Add shape
                lon = record.get('shape_x') or record.get('longitude')
                lat = record.get('shape_y') or record.get('latitude')
                row.append((lon, lat) if lon and lat else None)

                cursor.insertRow(row)
                insert_count += 1
            except Exception as e:
                print(f"   Error inserting record: {e}")

    print(f"   Inserted {insert_count} records")

    # ========================================================================
    # Step 5: Summary statistics
    # ========================================================================

    print("[5/5] Generating summary...")

    # Count by source
    source_counts = defaultdict(int)
    for rec in consensus_records:
        source_counts[rec.get('source', 'Unknown')] += 1

    print()
    print("=" * 60)
    print("CONSENSUS BUILDINGS SUMMARY")
    print("=" * 60)
    print(f"Total consensus records: {len(consensus_records)}")
    print(f"Original gold_buildings: {len(records)}")
    print(f"Reduction: {len(records) - len(consensus_records)} records ({(1 - len(consensus_records)/len(records))*100:.1f}%)")
    print()
    print("Records by winning source:")
    for source in sorted(source_counts.keys(), key=lambda x: -source_counts[x]):
        print(f"   {source:20s}: {source_counts[source]:4d}")
    print()

    return consensus_records


def create_consensus_campus():
    """
    Create deduplicated consensus_campus layer by rolling up consensus_buildings.
    """

    print("=" * 80)
    print("CREATING CONSENSUS CAMPUS LAYER")
    print("=" * 80)
    print(f"Started: {datetime.now()}")

    # Check if consensus_buildings exists
    if not arcpy.Exists(CONSENSUS_BUILDINGS):
        print("ERROR: consensus_buildings not found. Run create_consensus_buildings() first.")
        return None

    # Delete if exists
    if arcpy.Exists(CONSENSUS_CAMPUS):
        arcpy.Delete_management(CONSENSUS_CAMPUS)

    # Read consensus_buildings
    print("[1/3] Reading consensus_buildings...")

    records = []
    fields = [f.name for f in arcpy.ListFields(CONSENSUS_BUILDINGS)
              if f.name not in ['OBJECTID', 'Shape']]
    fields.append('SHAPE@XY')

    with arcpy.da.SearchCursor(CONSENSUS_BUILDINGS, fields) as cursor:
        for row in cursor:
            record = {}
            for i, field in enumerate(fields[:-1]):
                record[field] = row[i]
            if row[-1]:
                record['shape_x'], record['shape_y'] = row[-1]
            records.append(record)

    print(f"   Loaded {len(records)} building records")

    # Group by campus_id
    print("[2/3] Aggregating to campus level...")

    campus_data = defaultdict(list)
    for rec in records:
        campus_id = rec.get('campus_id')
        if campus_id:
            campus_data[campus_id].append(rec)

    print(f"   Found {len(campus_data)} unique campuses")

    # Aggregate campus-level data
    campus_records = []
    for campus_id, buildings in campus_data.items():
        # Use first building for base attributes
        base = buildings[0].copy()

        # Aggregate capacity (sum)
        total_commissioned = sum(b.get('commissioned_power_mw') or 0 for b in buildings)
        total_full_capacity = sum(b.get('full_capacity_mw') or 0 for b in buildings)
        total_sqft = sum(b.get('facility_sqft') or 0 for b in buildings)

        # Aggregate forecast capacity
        for year in range(2023, 2033):
            field = f'mw_{year}'
            base[field] = sum(b.get(field) or 0 for b in buildings)

        base['commissioned_power_mw'] = total_commissioned if total_commissioned > 0 else None
        base['full_capacity_mw'] = total_full_capacity if total_full_capacity > 0 else None
        base['facility_sqft'] = total_sqft if total_sqft > 0 else None

        # Building count
        base['building_count'] = len(buildings)

        # Sources in campus
        sources = set(b.get('source') for b in buildings if b.get('source'))
        base['sources_available'] = ', '.join(sorted(sources))
        base['source_count'] = len(sources)

        # Use centroid for location
        lats = [b.get('latitude') or b.get('shape_y') for b in buildings if b.get('latitude') or b.get('shape_y')]
        lons = [b.get('longitude') or b.get('shape_x') for b in buildings if b.get('longitude') or b.get('shape_x')]
        if lats and lons:
            base['latitude'] = sum(lats) / len(lats)
            base['longitude'] = sum(lons) / len(lons)
            base['shape_x'] = base['longitude']
            base['shape_y'] = base['latitude']

        campus_records.append(base)

    # Create output
    print("[3/3] Creating output feature class...")

    arcpy.CreateFeatureclass_management(
        out_path=os.path.dirname(CONSENSUS_CAMPUS),
        out_name=os.path.basename(CONSENSUS_CAMPUS),
        geometry_type="POINT",
        spatial_reference=arcpy.SpatialReference(4326)
    )

    # Add fields
    source_fields = [f for f in arcpy.ListFields(CONSENSUS_BUILDINGS)
                     if f.name not in ['OBJECTID', 'Shape', 'Shape_Length', 'Shape_Area']]

    for field in source_fields:
        try:
            arcpy.AddField_management(
                CONSENSUS_CAMPUS,
                field.name,
                field.type,
                field_length=field.length if field.type == 'String' else None
            )
        except:
            pass

    # Add building_count field
    try:
        arcpy.AddField_management(CONSENSUS_CAMPUS, "building_count", "SHORT")
    except:
        pass

    # Insert records
    insert_fields = [f.name for f in arcpy.ListFields(CONSENSUS_CAMPUS)
                     if f.name not in ['OBJECTID', 'Shape']]
    insert_fields.append('SHAPE@XY')

    insert_count = 0
    with arcpy.da.InsertCursor(CONSENSUS_CAMPUS, insert_fields) as cursor:
        for record in campus_records:
            try:
                row = []
                for field in insert_fields[:-1]:
                    row.append(record.get(field))

                lon = record.get('shape_x') or record.get('longitude')
                lat = record.get('shape_y') or record.get('latitude')
                row.append((lon, lat) if lon and lat else None)

                cursor.insertRow(row)
                insert_count += 1
            except Exception as e:
                print(f"   Error: {e}")

    print(f"   Inserted {insert_count} campus records")

    print()
    print("=" * 60)
    print("CONSENSUS CAMPUS SUMMARY")
    print("=" * 60)
    print(f"Total campuses: {len(campus_records)}")
    print(f"Campuses with multiple buildings: {sum(1 for c in campus_records if c.get('building_count', 1) > 1)}")
    print()

    return campus_records


def main():
    """Main execution."""

    print("=" * 80)
    print("CONSENSUS DEDUPE SCRIPT")
    print("Creating deduplicated consensus layers for dashboarding")
    print("=" * 80)
    print(f"Started: {datetime.now()}")
    print()

    # Create consensus buildings
    buildings = create_consensus_buildings()

    if buildings:
        print()
        # Create consensus campus
        campus = create_consensus_campus()

    print()
    print("=" * 80)
    print("✅ CONSENSUS DEDUPE COMPLETE")
    print("=" * 80)
    print()
    print("📁 Output feature classes:")
    print(f"   • {CONSENSUS_BUILDINGS}")
    print(f"   • {CONSENSUS_CAMPUS}")
    print()
    print("🎯 Next steps:")
    print("   1. Add to ArcGIS Pro map")
    print("   2. Configure pop-ups for dashboarding")
    print("   3. Publish to ArcGIS Online/Portal")
    print()
    print(f"Completed: {datetime.now()}")


# ============================================================================
# EXECUTE
# ============================================================================

if __name__ == "__main__":
    main()
