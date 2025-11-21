"""
Data Center Hawk Ingestion Script
Ingests DCH hyperscale data into gold_buildings feature class.

Author: Meta Data Center GIS Team
Last Updated: 2025-01-14
"""

import arcpy
from datetime import datetime
import re

# ====== CONFIGURATION ======
# Update these paths for your environment
GDB = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
SOURCE_FC = GDB + r"\DCH_Hyper_MetaOracle_ConsensusXY"
TARGET_FC = GDB + r"\gold_buildings"
SOURCE_NAME = "DataCenterHawk"

# Conversion factors
KW_TO_MW = 0.001

# Status vocabulary mapping (DCH → Gold schema)
STATUS_MAP = {
    'Owned': 'Active',
    'Under Construction': 'Under Construction',
    'Planned': 'Announced',
    None: 'Unknown',
    '': 'Unknown'
}

# ====== HELPER FUNCTIONS ======
def slug(text):
    """Generate URL-safe slug from text."""
    if not text:
        return ''
    return re.sub(r'[^a-z0-9]+', '', str(text).lower())

def generate_campus_id(company, city, campus_name, lat, lon):
    """
    Generate unique campus_id using convention:
    company|city|campus_name (slugified)
    
    Fallback to coordinates if campus_name missing.
    """
    name_slug = slug(campus_name) if campus_name else \
                f"{round(lat,3)}{round(lon,3)}".replace('.', '').replace('-', 'n')
    return f"{slug(company)}|{slug(city)}|{name_slug}"

def year_to_date(year_val):
    """Convert commissioned_year (Double) to datetime (Dec 31)."""
    if year_val and year_val > 1900:
        return datetime(int(year_val), 12, 31)
    return None

def derive_record_level(facility_type, name):
    """
    Determine if record represents Building or Campus level.
    Currently all hyperscale = Campus level.
    """
    if facility_type and 'building' in str(facility_type).lower():
        return 'Building'
    if name and ' - building' in str(name).lower():
        return 'Building'
    return 'Campus'

# ====== MAIN INGESTION ======
def main():
    print("=" * 70)
    print(f"DCH INGESTION STARTED: {datetime.now()}")
    print("=" * 70)
    
    # Verify source exists
    if not arcpy.Exists(SOURCE_FC):
        raise Exception(f"Source feature class not found: {SOURCE_FC}")
    
    # Get count
    total_records = int(arcpy.management.GetCount(SOURCE_FC)[0])
    print(f"\nTotal DCH records to process: {total_records}")
    
    # Field mappings
    insert_fields = [
        'SHAPE@XY', 'unique_id', 'source', 'source_unique_id', 'date_reported',
        'record_level', 'campus_id', 'campus_name', 'company_source', 'company_clean',
        'building_designation', 'address', 'postal_code', 'city', 'market',
        'state', 'state_abbr', 'county', 'country', 'region', 'latitude', 'longitude',
        'planned_power_mw', 'uc_power_mw', 'commissioned_power_mw', 'full_capacity_mw',
        'planned_plus_uc_mw', 'mw_2023', 'mw_2024', 'mw_2025', 'mw_2026', 'mw_2027',
        'mw_2028', 'mw_2029', 'mw_2030', 'mw_2031', 'mw_2032', 'pue',
        'actual_live_date', 'construction_started', 'announced', 'land_acquisition',
        'permitting', 'cod', 'facility_status', 'cancelled', 'facility_sqft',
        'whitespace_sqft', 'available_power_kw', 'substation_count', 'onsite_substation',
        'power_provider', 'power_grid', 'tier_design', 'type_category', 'owned_leased',
        'purpose', 'feed_config', 'ecosystem_ixps', 'ecosystem_cloud', 'ecosystem_children',
        'ecosystem_networkproviders', 'ecosystem_networkpresence', 'ecosystem_serviceproviders',
        'additional_references', 'notes', 'ingest_date', 'status_rank_tmp'
    ]
    
    read_fields = [
        'facility_id', 'company_name', 'company_code', 'address', 'city', 'state',
        'State_Abbr', 'postal_code', 'country', 'market_name', 'Region', 'County',
        'Metro_Admin', 'latitude', 'longitude', 'facility_type', 'status',
        'capacity_commissioned_power', 'capacity_planned_power', 'capacity_under_construction_power',
        'capacity_building_sf', 'commissioned_year', 'date_updated', 'extraction_date'
    ]
    
    insert_count = 0
    skip_count = 0
    
    print("\nProcessing records...")
    
    with arcpy.da.SearchCursor(SOURCE_FC, read_fields) as search_cursor, \
         arcpy.da.InsertCursor(TARGET_FC, insert_fields) as insert_cursor:
        
        for row in search_cursor:
            (facility_id, company_name, company_code, address, city, state, state_abbr,
             postal_code, country, market_name, region, county, metro_admin,
             latitude, longitude, facility_type, status,
             cap_comm, cap_plan, cap_uc, cap_sf, commissioned_year,
             date_updated, extraction_date) = row
            
            # Skip if missing critical fields
            if not latitude or not longitude or not company_name:
                skip_count += 1
                continue
            
            # ===== DERIVE FIELDS =====
            
            # Unique ID
            unique_id = f"DCH_{facility_id}"
            
            # Name & Campus Logic
            # If company_code exists (CLN2, ODN1): name includes code, campus_name excludes it
            # If no code: name = campus_name = "Company City"
            if company_code and str(company_code).strip() and str(company_code) != 'None':
                name = f"{company_name} {company_code}".strip()
                campus_name = f"{company_name} {city}".strip()
            else:
                name = f"{company_name} {city}".strip()
                campus_name = name
            
            # Clean campus_name
            campus_name = campus_name.replace(' Data Center', '').replace(' Campus', '').strip()
            
            # Generate campus_id
            campus_id = generate_campus_id(company_name, city, campus_name, latitude, longitude)
            
            # Status mapping
            facility_status = STATUS_MAP.get(status, 'Unknown')
            
            # Capacity conversion (kW → MW)
            commissioned_mw = (cap_comm * KW_TO_MW) if cap_comm else 0
            planned_mw = (cap_plan * KW_TO_MW) if cap_plan else 0
            uc_mw = (cap_uc * KW_TO_MW) if cap_uc else 0
            full_capacity_mw = commissioned_mw + planned_mw + uc_mw
            planned_plus_uc_mw = planned_mw + uc_mw
            
            # Dates
            actual_live_date = year_to_date(commissioned_year)
            ingest_date = datetime.now()
            
            # Record level
            record_level = derive_record_level(facility_type, name)
            
            # Geometry
            point = (longitude, latitude)
            
            # Insert row
            insert_cursor.insertRow([
                point, unique_id, SOURCE_NAME, facility_id, date_updated,
                record_level, campus_id, campus_name, company_name, company_name,
                company_code, address, postal_code, city, market_name,
                state, state_abbr, county, country, region, latitude, longitude,
                planned_mw, uc_mw, commissioned_mw, full_capacity_mw, planned_plus_uc_mw,
                None, None, None, None, None, None, None, None, None, None, None,
                actual_live_date, None, None, None, None, None,
                facility_status, 0, cap_sf, None, None, None, None, None, None, None,
                facility_type, None, None, None, None, None, None, None, None, None,
                None, None, ingest_date, None
            ])
            
            insert_count += 1
            
            # Progress indicator
            if insert_count % 50 == 0:
                print(f"  Processed {insert_count} / {total_records} records...")
    
    print("\n" + "=" * 70)
    print(f"✅ INGESTION COMPLETE")
    print("=" * 70)
    print(f"Inserted: {insert_count} records")
    print(f"Skipped: {skip_count} records (missing lat/lon/company)")
    print(f"Completed: {datetime.now()}")
    print("=" * 70)

# ====== EXECUTE ======
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
