"""
QA Validation Script
Quality assurance queries for gold_buildings and gold_campus.

Run after each ingestion/rollup to verify data quality.

Author: Meta Data Center GIS Team
Last Updated: 2025-01-14
"""

import arcpy
from collections import Counter

# ====== CONFIGURATION ======
GDB = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
BUILDINGS_FC = GDB + r"\gold_buildings"
CAMPUS_FC = GDB + r"\gold_campus"

# ====== QA QUERIES ======

def qa_record_counts():
    """Check record counts by source."""
    print("\n" + "=" * 70)
    print("QA: RECORD COUNTS BY SOURCE")
    print("=" * 70)
    
    with arcpy.da.SearchCursor(BUILDINGS_FC, ['source']) as cursor:
        sources = Counter([row[0] for row in cursor])
    
    print("\nBuildings by Source:")
    total_buildings = 0
    for source, count in sorted(sources.items()):
        print(f"  {source}: {count:,}")
        total_buildings += count
    
    print(f"\nTotal Buildings: {total_buildings:,}")
    
    campus_count = int(arcpy.management.GetCount(CAMPUS_FC)[0])
    print(f"Total Campuses: {campus_count:,}")
    print(f"Avg Buildings/Campus: {total_buildings/campus_count:.1f}")

def qa_status_distribution():
    """Check status distribution."""
    print("\n" + "=" * 70)
    print("QA: STATUS DISTRIBUTION")
    print("=" * 70)
    
    # Buildings
    with arcpy.da.SearchCursor(BUILDINGS_FC, ['facility_status']) as cursor:
        bldg_statuses = Counter([row[0] for row in cursor])
    
    print("\nBuildings:")
    for status, count in sorted(bldg_statuses.items(), key=lambda x: -x[1]):
        print(f"  {status}: {count:,}")
    
    # Campuses
    with arcpy.da.SearchCursor(CAMPUS_FC, ['facility_status_agg']) as cursor:
        campus_statuses = Counter([row[0] for row in cursor])
    
    print("\nCampuses:")
    for status, count in sorted(campus_statuses.items(), key=lambda x: -x[1]):
        print(f"  {status}: {count:,}")

def qa_capacity_stats():
    """Check capacity statistics."""
    print("\n" + "=" * 70)
    print("QA: CAPACITY STATISTICS")
    print("=" * 70)
    
    import statistics
    
    # Campus capacity
    with arcpy.da.SearchCursor(CAMPUS_FC, 
        ['full_capacity_mw', 'commissioned_power_mw', 'planned_power_mw', 'uc_power_mw'],
        where_clause="full_capacity_mw > 0") as cursor:
        
        full_cap = []
        comm_cap = []
        plan_cap = []
        uc_cap = []
        
        for row in cursor:
            full_cap.append(row[0])
            comm_cap.append(row[1] if row[1] else 0)
            plan_cap.append(row[2] if row[2] else 0)
            uc_cap.append(row[3] if row[3] else 0)
    
    print("\nCampus Full Capacity (MW):")
    print(f"  Min: {min(full_cap):,.1f}")
    print(f"  Max: {max(full_cap):,.1f}")
    print(f"  Avg: {statistics.mean(full_cap):,.1f}")
    print(f"  Median: {statistics.median(full_cap):,.1f}")
    print(f"  Total: {sum(full_cap):,.1f}")
    
    print("\nBreakdown:")
    print(f"  Commissioned: {sum(comm_cap):,.1f} MW")
    print(f"  Under Construction: {sum(uc_cap):,.1f} MW")
    print(f"  Planned: {sum(plan_cap):,.1f} MW")

def qa_top_campuses(n=10):
    """Show top N campuses by capacity."""
    print("\n" + "=" * 70)
    print(f"QA: TOP {n} CAMPUSES BY CAPACITY")
    print("=" * 70)
    
    with arcpy.da.SearchCursor(CAMPUS_FC,
        ['company_clean', 'city', 'building_count', 'full_capacity_mw', 'facility_status_agg'],
        sql_clause=(None, 'ORDER BY full_capacity_mw DESC')) as cursor:
        
        print(f"\n{'Rank':<6} {'Company':<15} {'City':<20} {'Bldgs':<7} {'MW':<10} {'Status'}")
        print("-" * 70)
        
        for i, row in enumerate(cursor):
            if i >= n:
                break
            company, city, bldg_count, mw, status = row
            print(f"{i+1:<6} {company:<15} {city:<20} {bldg_count:<7} {mw:<10.1f} {status}")

def qa_campus_detail(campus_id):
    """Show detailed breakdown for specific campus."""
    print("\n" + "=" * 70)
    print(f"QA: CAMPUS DETAIL - {campus_id}")
    print("=" * 70)
    
    # Campus summary
    with arcpy.da.SearchCursor(CAMPUS_FC,
        ['campus_name', 'company_clean', 'city', 'country', 'building_count',
         'commissioned_power_mw', 'uc_power_mw', 'planned_power_mw', 'full_capacity_mw',
         'facility_status_agg'],
        where_clause=f"campus_id = '{campus_id}'") as cursor:
        
        for row in cursor:
            print(f"\nCampus: {row[0]}")
            print(f"Company: {row[1]}")
            print(f"Location: {row[2]}, {row[3]}")
            print(f"Buildings: {row[4]}")
            print(f"\nCapacity:")
            print(f"  Commissioned: {row[5]:.1f} MW")
            print(f"  Under Construction: {row[6]:.1f} MW")
            print(f"  Planned: {row[7]:.1f} MW")
            print(f"  TOTAL: {row[8]:.1f} MW")
            print(f"\nStatus: {row[9]}")
    
    # Building breakdown
    print(f"\n{'Building':<20} {'Status':<20} {'Commissioned MW':<15} {'Full MW'}")
    print("-" * 70)
    
    with arcpy.da.SearchCursor(BUILDINGS_FC,
        ['building_designation', 'facility_status', 'commissioned_power_mw', 'full_capacity_mw'],
        where_clause=f"campus_id = '{campus_id}'",
        sql_clause=(None, 'ORDER BY building_designation')) as cursor:
        
        for row in cursor:
            bldg = row[0] if row[0] else 'N/A'
            status = row[1]
            comm = row[2] if row[2] else 0
            full = row[3] if row[3] else 0
            print(f"{bldg:<20} {status:<20} {comm:<15.1f} {full:.1f}")

def qa_geography_completeness():
    """Check for missing geography fields."""
    print("\n" + "=" * 70)
    print("QA: GEOGRAPHY COMPLETENESS")
    print("=" * 70)
    
    # Check buildings
    with arcpy.da.SearchCursor(BUILDINGS_FC,
        ['unique_id', 'city', 'state', 'country', 'market']) as cursor:
        
        missing_city = 0
        missing_state = 0
        missing_country = 0
        missing_market = 0
        
        for row in cursor:
            if not row[1]: missing_city += 1
            if not row[2]: missing_state += 1
            if not row[3]: missing_country += 1
            if not row[4]: missing_market += 1
    
    total = int(arcpy.management.GetCount(BUILDINGS_FC)[0])
    
    print(f"\nBuildings (Total: {total}):")
    print(f"  Missing city: {missing_city}")
    print(f"  Missing state: {missing_state} (OK for international)")
    print(f"  Missing country: {missing_country}")
    print(f"  Missing market: {missing_market}")

# ====== MAIN MENU ======
def main():
    """Run all QA queries."""
    print("\n" + "=" * 70)
    print("RUNNING FULL QA VALIDATION")
    print("=" * 70)
    
    qa_record_counts()
    qa_status_distribution()
    qa_capacity_stats()
    qa_top_campuses(10)
    qa_geography_completeness()
    
    # Example: Detail for Meta Portan
    print("\n" + "=" * 70)
    print("EXAMPLE: META PORTAN CAMPUS DETAIL")
    qa_campus_detail('meta|portan|metaportan')
    
    print("\n" + "=" * 70)
    print("QA VALIDATION COMPLETE")
    print("=" * 70)

# ====== EXECUTE ======
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()