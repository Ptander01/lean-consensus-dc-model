import arcpy
from datetime import datetime
from collections import defaultdict
import re

# ============================================================================
# CONFIGURATION
# ============================================================================
gdb_path = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
source_fc = f"{gdb_path}\\SemiAnalysis_Building_MetaOracle_ExportFeatures"
target_fc = f"{gdb_path}\\gold_buildings"

SOURCE_NAME = "Semianalysis"

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def slugify(text):
    """Convert text to URL-friendly slug"""
    if not text or text in [None, '', ' ']:
        return 'unknown'
    text = str(text).lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    return text.strip('-')

def generate_campus_id(company, city, campus_name):
    """Generate standardized campus_id"""
    company_slug = slugify(company) if company else 'unknown'
    city_slug = slugify(city) if city else 'unknown'
    campus_slug = slugify(campus_name) if campus_name else 'unknown'
    return f"{company_slug}|{city_slug}|{campus_slug}"

def parse_quarter_date(quarter_str):
    """Parse quarter date string to Python date"""
    if not quarter_str:
        return None
    try:
        # Handle format like "2024 Q1" or "Q1 2024"
        quarter_str = str(quarter_str).strip()
        
        # Try YYYY-MM-DD format first
        if '-' in quarter_str:
            return datetime.strptime(quarter_str, '%Y-%m-%d').date()
        
        # Try quarter format
        parts = quarter_str.replace('Q', '').split()
        if len(parts) == 2:
            year = int(parts[0]) if len(parts[0]) == 4 else int(parts[1])
            quarter = int(parts[1]) if len(parts[0]) == 4 else int(parts[0])
            month = (quarter - 1) * 3 + 3  # End of quarter month
            return datetime(year, month, 1).date()
    except:
        pass
    return None

def get_state_abbr(state_name):
    """Convert state name to abbreviation"""
    state_map = {
        'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
        'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
        'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
        'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
        'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
        'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
        'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
        'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
        'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
        'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
        'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
        'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
        'Wisconsin': 'WI', 'Wyoming': 'WY'
    }
    if not state_name:
        return None
    # If already abbreviation, return as-is
    if len(str(state_name)) == 2:
        return str(state_name).upper()
    return state_map.get(str(state_name).strip(), None)

def safe_float(value):
    """Safely convert to float"""
    if value in [None, '', ' ', 'None']:
        return None
    try:
        return float(value)
    except:
        return None

# ============================================================================
# MAIN INGESTION LOGIC
# ============================================================================

def ingest_semianalysis():
    """
    Ingest Semianalysis time-series data into gold_buildings
    Pivots year/capacity columns to mw_YYYY fields
    """
    
    print("=" * 80)
    print("SEMIANALYSIS INGESTION - TIME-SERIES PIVOT")
    print("=" * 80)
    
    # Step 1: Read all records and group by cluster (building)
    print("\n[1/4] Reading source data and grouping by building...")
    
    fields_to_read = [
        'SHAPE@XY', 'unique_id', 'lat', 'long', 'us_state', 'city', 'zip_code',
        'cluster', 'country', 'region', 'company', 'market', 'type', 'unit',
        'power_grid', 'total_under_construction_mw', 'start_of_operations',
        'start_date__quarter_end', 'full_capacity', 'actual_live_assumption',
        'live_date__quarter_end', 'total_planned_mw', 'planned__uc',
        'quarters_to_complete', 'pace_per_quarter', 'installed_capacity_mw',
        'facility_square_footage', 'year', 'capacity', 'ds'
    ]
    
    # Dictionary to store pivoted data: {cluster: {data}}
    buildings = defaultdict(lambda: {
        'year_capacity': {},  # {year: capacity}
        'geometry': None,
        'attributes': {}
    })
    
    with arcpy.da.SearchCursor(source_fc, fields_to_read) as cursor:
        for row in cursor:
            row_dict = dict(zip(fields_to_read, row))
            cluster = row_dict['cluster']
            year = row_dict['year']
            capacity = row_dict['capacity']
            
            # Store year/capacity pair
            if year and capacity:
                buildings[cluster]['year_capacity'][int(year)] = safe_float(capacity)
            
            # Store geometry (same for all years)
            if not buildings[cluster]['geometry']:
                buildings[cluster]['geometry'] = row_dict['SHAPE@XY']
            
            # Store other attributes (take first occurrence)
            if not buildings[cluster]['attributes']:
                buildings[cluster]['attributes'] = row_dict
    
    unique_buildings = len(buildings)
    print(f"   ✓ Found {unique_buildings} unique buildings from {int(arcpy.GetCount_management(source_fc)[0])} time-series records")
    
    # Step 2: Transform and prepare records for insertion
    print("\n[2/4] Transforming pivoted data to gold schema...")
    
    records_to_insert = []
    ingest_date = datetime.now().date()
    
    for cluster, data in buildings.items():
        attrs = data['attributes']
        year_capacity = data['year_capacity']
        
        # Generate IDs
        company = attrs.get('company', 'Unknown')
        city = attrs.get('city', 'Unknown')
        campus_id = generate_campus_id(company, city, cluster)
        unique_id = f"{SOURCE_NAME}_{slugify(cluster)}"
        
        # Parse dates
        start_date = parse_quarter_date(attrs.get('start_date__quarter_end'))
        live_date = parse_quarter_date(attrs.get('live_date__quarter_end'))
        
        # Build record
        record = {
            # Geometry
            'SHAPE@XY': data['geometry'],
            
            # Core Identity
            'unique_id': unique_id,
            'source': SOURCE_NAME,
            'source_unique_id': cluster,
            'campus_id': campus_id,
            'record_level': 'Building',
            'ingest_date': ingest_date,
            'date_reported': ingest_date,
            
            # Company & Naming
            'company_source': company,
            'company_clean': company,
            'campus_name': cluster,
            'building_designation': None,
            
            # Geography
            'address': None,
            'postal_code': attrs.get('zip_code'),
            'city': city,
            'market': attrs.get('market'),
            'state': attrs.get('us_state'),
            'state_abbr': get_state_abbr(attrs.get('us_state')),
            'county': None,
            'country': attrs.get('country'),
            'region': attrs.get('region'),
            'latitude': safe_float(attrs.get('lat')),
            'longitude': safe_float(attrs.get('long')),
            
            # Capacity - Current totals
            'planned_power_mw': safe_float(attrs.get('total_planned_mw')),
            'uc_power_mw': safe_float(attrs.get('total_under_construction_mw')),
            'commissioned_power_mw': safe_float(attrs.get('installed_capacity_mw')),
            'full_capacity_mw': safe_float(attrs.get('total_planned_mw')),
            'planned_plus_uc_mw': safe_float(attrs.get('planned__uc')),
            
            # Year-by-year capacity (PIVOTED DATA)
            'mw_2023': year_capacity.get(2023),
            'mw_2024': year_capacity.get(2024),
            'mw_2025': year_capacity.get(2025),
            'mw_2026': year_capacity.get(2026),
            'mw_2027': year_capacity.get(2027),
            'mw_2028': year_capacity.get(2028),
            'mw_2029': year_capacity.get(2029),
            'mw_2030': year_capacity.get(2030),
            'mw_2031': year_capacity.get(2031),
            'mw_2032': year_capacity.get(2032),
            
            # Facility Details
            'facility_sqft': safe_float(attrs.get('facility_square_footage')),
            'whitespace_sqft': None,
            'pue': None,
            'available_power_kw': None,
            
            # Status & Dates
            'facility_status': 'Unknown',  # Semianalysis doesn't provide status
            'status_rank_tmp': 7,
            'actual_live_date': live_date,
            'construction_started': start_date,
            'announced': None,
            'land_acquisition': None,
            'cod': live_date,
            
            # Cost & Land
            'total_cost_usd_million': None,
            'land_cost_usd_million': None,
            'total_site_acres': None,
            'data_center_acres': None,
            
            # Metadata
            'additional_references': None,
            'notes': f"Type: {attrs.get('type')} | Power Grid: {attrs.get('power_grid')} | Quarters to Complete: {attrs.get('quarters_to_complete')}"
        }
        
        records_to_insert.append(record)
    
    print(f"   ✓ Prepared {len(records_to_insert)} building records for insertion")
    
    # Step 3: Insert into gold_buildings
    print(f"\n[3/4] Inserting {len(records_to_insert)} records into gold_buildings...")
    
    insert_fields = list(records_to_insert[0].keys())
    
    inserted_count = 0
    with arcpy.da.InsertCursor(target_fc, insert_fields) as cursor:
        for record in records_to_insert:
            cursor.insertRow([record[field] for field in insert_fields])
            inserted_count += 1
    
    print(f"   ✓ Successfully inserted {inserted_count} records")
    
    # Step 4: Verification
    print("\n[4/4] Verification...")
    
    # Count Semianalysis records
    where_clause = f"source = '{SOURCE_NAME}'"
    semianalysis_count = int(arcpy.GetCount_management(target_fc, where_clause)[0])
    
    # Get total count
    total_count = int(arcpy.GetCount_management(target_fc)[0])
    
    print(f"   ✓ Semianalysis records in gold_buildings: {semianalysis_count}")
    print(f"   ✓ Total records in gold_buildings: {total_count}")
    
    # Year-by-year capacity summary
    print("\n   Year-by-year capacity summary (Semianalysis only):")
    for year in range(2023, 2033):
        field = f'mw_{year}'
        stats = arcpy.Statistics_analysis(
            target_fc,
            arcpy.management.CreateScratchName("stats", workspace="in_memory"),
            [[field, "SUM"]],
            where_clause
        )
        with arcpy.da.SearchCursor(stats, f"SUM_{field}") as cursor:
            total = next(cursor)[0]
            if total:
                print(f"      {year}: {total:,.0f} MW")
        arcpy.Delete_management(stats)
    
    print("\n" + "=" * 80)
    print("✅ SEMIANALYSIS INGESTION COMPLETE")
    print("=" * 80)
    print(f"\n📊 SUMMARY:")
    print(f"   • Source records (time-series): 4,064")
    print(f"   • Unique buildings identified: {unique_buildings}")
    print(f"   • Records inserted: {inserted_count}")
    print(f"   • Total gold_buildings: {total_count}")
    print(f"\n⚠️  NEXT STEP: Run campus_rollup.py to update gold_campus!")
    
    return inserted_count

# ============================================================================
# EXECUTE
# ============================================================================

if __name__ == "__main__":
    try:
        inserted = ingest_semianalysis()
        print(f"\n✅ SUCCESS: {inserted} Semianalysis records ingested")
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
