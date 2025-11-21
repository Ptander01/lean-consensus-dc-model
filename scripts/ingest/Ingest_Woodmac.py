import arcpy
import os
from datetime import datetime
import re

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def slugify(text):
    """Convert text to slug format for campus_id generation"""
    if not text:
        return ''
    text = str(text).lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    return text.strip('-')

def generate_campus_id(company, city, campus_name='', source=''):
    """Generate standardized campus_id"""
    company_slug = slugify(company)
    city_slug = slugify(city)
    
    if campus_name:
        campus_slug = slugify(campus_name)
        return f"{company_slug}|{city_slug}|{campus_slug}"
    elif source:
        return f"{company_slug}|{city_slug}|{source.lower()}"
    else:
        return f"{company_slug}|{city_slug}"

def parse_mw_string(mw_str):
    """Convert MW string to float, handling various formats"""
    if not mw_str or str(mw_str).strip() in ['', 'None', 'NULL']:
        return None
    
    try:
        # Remove any non-numeric characters except decimal point
        clean_str = re.sub(r'[^\d.]', '', str(mw_str))
        if clean_str:
            return float(clean_str)
    except:
        pass
    
    return None

def determine_facility_status(announced, land_acq, construction, cod):
    """
    Determine facility status based on milestone dates
    Priority: cod > construction > land_acq > announced
    """
    now = datetime.now()
    
    if cod and cod <= now:
        return 'Active'
    elif construction:
        if construction <= now:
            return 'Under Construction'
        else:
            return 'Permitting'
    elif land_acq:
        return 'Land Acquisition'
    elif announced:
        return 'Announced'
    else:
        return 'Unknown'

def extract_building_designation(project_name):
    """
    Extract building/phase designation from project name
    Examples: "Altoona campus (expansion 1)" -> "Expansion Phase 1"
    """
    if not project_name:
        return None
    
    name_lower = str(project_name).lower()
    
    # Look for expansion phase patterns
    expansion_match = re.search(r'expansion\s*(\d+|[ivx]+)', name_lower)
    if expansion_match:
        phase_num = expansion_match.group(1)
        return f"Expansion Phase {phase_num.upper()}"
    
    # Look for building number patterns
    building_match = re.search(r'building\s*(\d+|[a-z])', name_lower)
    if building_match:
        bldg_num = building_match.group(1)
        return f"Building {bldg_num.upper()}"
    
    return None

def build_woodmac_notes(partner, workloads, energy, cooling, prior_use, buildings, source_notes):
    """
    Combine WoodMac metadata into structured notes field
    Format: "Partner: X | Buildings planned: Y | Workloads: Z | ..."
    """
    notes_parts = []
    
    if partner:
        notes_parts.append(f"Partner: {partner}")
    if buildings:
        notes_parts.append(f"Buildings planned: {buildings}")
    if workloads:
        notes_parts.append(f"Workloads: {workloads}")
    if energy:
        notes_parts.append(f"Energy: {energy}")
    if cooling:
        notes_parts.append(f"Cooling: {cooling}")
    if prior_use:
        notes_parts.append(f"Prior use: {prior_use}")
    if source_notes:
        notes_parts.append(f"Notes: {source_notes}")
    
    return " | ".join(notes_parts) if notes_parts else None

def combine_references(initial_ref, addl_refs):
    """
    Combine initial_announcement and additional_references
    into single additional_references field
    """
    refs = []
    
    if initial_ref:
        refs.append(f"Initial: {initial_ref}")
    if addl_refs:
        refs.append(f"Additional: {addl_refs}")
    
    return "; ".join(refs) if refs else None

# ============================================================================
# MAIN INGESTION SCRIPT
# ============================================================================

# Environment setup
gdb_path = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
arcpy.env.workspace = gdb_path
arcpy.env.overwriteOutput = True

# Feature class paths
woodmac_fc = os.path.join(gdb_path, "WoodMac_MetaOracle_Consensus")
gold_buildings = os.path.join(gdb_path, "gold_buildings")

print("="*80)
print("WOODMAC INGESTION SCRIPT - OPTION A (MINIMAL SCHEMA)")
print("="*80)

# Verify source exists
if not arcpy.Exists(woodmac_fc):
    print(f"❌ ERROR: Source feature class not found: {woodmac_fc}")
    exit()

# Get source record count
source_count = int(arcpy.management.GetCount(woodmac_fc)[0])
print(f"\n📊 Source: WoodMac_MetaOracle_Consensus")
print(f"   Records: {source_count}")

# Define source fields to read
source_fields = [
    'SHAPE@',           # Geometry
    'project_id',       # Unique identifier
    'project_name',     # Project/campus name
    'developer',        # Company
    'partner',          # Partner company
    'city',             # City
    'state',            # State
    'market',           # Market
    'announced',        # Date: announced
    'land_acquisition', # Date: land acquisition
    'construction',     # Date: construction start
    'cod',              # Date: commercial operation
    'existing_mw',      # String: existing capacity
    'new_mw',           # String: new capacity
    'overall_cost_usd_million',   # Total project cost
    'land_cost_usd_million',      # Land cost
    'total_site_acres',           # Site size
    'data_center_acres',          # DC footprint
    'buildings',                  # Building count
    'workloads',                  # Workload types
    'energy',                     # Energy infrastructure
    'cooling',                    # Cooling systems
    'connectivity',               # Connectivity details
    'prior_use',                  # Previous site use
    'initial_announcement',       # Reference URL
    'additional_references',      # Additional URLs
    'notes',                      # Notes
    'longitude',                  # Coordinate
    'latitude'                    # Coordinate
]

# Define gold_buildings insert fields
insert_fields = [
    'SHAPE@',
    'unique_id',
    'source',
    'source_unique_id',
    'date_reported',
    'record_level',
    'campus_id',
    'campus_name',
    'building_designation',
    'company_source',
    'company_clean',
    'city',
    'state',
    'state_abbr',
    'market',
    'country',
    'region',
    'latitude',
    'longitude',
    'facility_status',
    'announced',
    'land_acquisition',
    'construction_started',
    'cod',
    'actual_live_date',
    'commissioned_power_mw',
    'planned_power_mw',
    'full_capacity_mw',
    'planned_plus_uc_mw',
    'total_cost_usd_million',      # New field
    'land_cost_usd_million',       # New field
    'total_site_acres',            # New field
    'data_center_acres',           # New field
    'additional_references',       # Existing field
    'notes',                       # Existing field - structured metadata
    'ingest_date'
]

# Process records
current_date = datetime.now()
inserted_count = 0
error_count = 0

print(f"\n🔄 Processing WoodMac records...")
print("-" * 80)

with arcpy.da.SearchCursor(woodmac_fc, source_fields) as s_cursor:
    with arcpy.da.InsertCursor(gold_buildings, insert_fields) as i_cursor:
        
        for row in s_cursor:
            try:
                # Unpack source row
                (geom, project_id, project_name, developer, partner, city, state, 
                 market, announced, land_acq, construction, cod_date,
                 existing_mw_str, new_mw_str, total_cost, land_cost,
                 site_acres, dc_acres, bldg_count, workloads, energy, 
                 cooling, connectivity, prior_use, initial_ref, addl_refs,
                 source_notes, lon, lat) = row
                
                # ============================================================
                # FIELD TRANSFORMATIONS
                # ============================================================
                
                # Generate unique_id
                unique_id = f"woodmac_{project_id}"
                
                # Company name (use developer)
                company = str(developer).strip() if developer else 'Unknown'
                
                # Generate campus_id
                campus_id = generate_campus_id(
                    company=company,
                    city=city if city else 'Unknown',
                    campus_name=project_name if project_name else ''
                )
                
                # Extract building designation
                building_designation = extract_building_designation(project_name)
                
                # State abbreviation (US only - simple lookup for common states)
                state_abbr_map = {
                    'Alabama': 'AL', 'Iowa': 'IA', 'Minnesota': 'MN',
                    'Nebraska': 'NE', 'Ohio': 'OH', 'Tennessee': 'TN',
                    'Utah': 'UT', 'Virginia': 'VA'
                }
                state_abbr = state_abbr_map.get(state) if state else None
                
                # Country and region (all WoodMac records are US Meta facilities)
                country = 'United States'
                region = 'AMER'
                
                # Parse MW values
                existing_mw = parse_mw_string(existing_mw_str)
                new_mw = parse_mw_string(new_mw_str)
                
                # Capacity field routing
                # WoodMac tracks expansions: existing_mw = commissioned, new_mw = planned
                commissioned_mw = existing_mw if existing_mw else None
                planned_mw = new_mw if new_mw else None
                
                # Full capacity = existing + new
                if existing_mw and new_mw:
                    full_capacity = existing_mw + new_mw
                elif new_mw:
                    full_capacity = new_mw
                elif existing_mw:
                    full_capacity = existing_mw
                else:
                    full_capacity = None
                
                # Planned + UC (assume new_mw is planned until construction starts)
                planned_plus_uc = planned_mw if planned_mw else None
                
                # Determine facility status
                facility_status = determine_facility_status(
                    announced, land_acq, construction, cod_date
                )
                
                # Actual live date (use COD if available)
                actual_live_date = cod_date if cod_date else None
                
                # ============================================================
                # COMBINE METADATA INTO EXISTING FIELDS
                # ============================================================
                
                # Combine references into additional_references field
                combined_refs = combine_references(initial_ref, addl_refs)
                
                # Build structured notes from descriptive metadata
                # Include: partner, workloads, energy, cooling, connectivity, prior_use, buildings, notes
                structured_notes = build_woodmac_notes(
                    partner=partner,
                    workloads=workloads,
                    energy=energy,
                    cooling=cooling,
                    prior_use=prior_use,
                    buildings=bldg_count,
                    source_notes=source_notes
                )
                
                # Add connectivity to notes if present
                if connectivity and structured_notes:
                    structured_notes = f"{structured_notes} | Connectivity: {connectivity}"
                elif connectivity:
                    structured_notes = f"Connectivity: {connectivity}"
                
                # ============================================================
                # BUILD INSERT ROW
                # ============================================================
                
                insert_row = [
                    geom,                    # SHAPE@
                    unique_id,               # unique_id
                    'WoodMac',              # source
                    str(project_id),        # source_unique_id
                    current_date,           # date_reported
                    'Building',             # record_level
                    campus_id,              # campus_id
                    project_name,           # campus_name
                    building_designation,   # building_designation
                    company,                # company_source
                    company,                # company_clean
                    city,                   # city
                    state,                  # state
                    state_abbr,             # state_abbr
                    market,                 # market
                    country,                # country
                    region,                 # region
                    lat,                    # latitude
                    lon,                    # longitude
                    facility_status,        # facility_status
                    announced,              # announced
                    land_acq,               # land_acquisition
                    construction,           # construction_started
                    cod_date,               # cod
                    actual_live_date,       # actual_live_date
                    commissioned_mw,        # commissioned_power_mw
                    planned_mw,             # planned_power_mw
                    full_capacity,          # full_capacity_mw
                    planned_plus_uc,        # planned_plus_uc_mw
                    total_cost,             # total_cost_usd_million (NEW)
                    land_cost,              # land_cost_usd_million (NEW)
                    site_acres,             # total_site_acres (NEW)
                    dc_acres,               # data_center_acres (NEW)
                    combined_refs,          # additional_references (EXISTING)
                    structured_notes,       # notes (EXISTING - structured)
                    current_date            # ingest_date
                ]
                
                # Insert row
                i_cursor.insertRow(insert_row)
                inserted_count += 1
                
                # Print progress with key details
                status_icon = "🟢" if facility_status == "Active" else "🟡" if facility_status == "Under Construction" else "⚪"
                mw_display = f"{full_capacity:.1f} MW" if full_capacity else "N/A"
                print(f"  {status_icon} {inserted_count}: {company} - {city}, {state}")
                print(f"     └─ {project_name} | {facility_status} | {mw_display}")
                
            except Exception as e:
                error_count += 1
                print(f"  ❌ Error processing record {project_id}: {str(e)}")
                continue

print("-" * 80)
print(f"\n✅ WoodMac Ingestion Complete!")
print(f"   • Inserted: {inserted_count} records")
print(f"   • Errors: {error_count} records")
print(f"   • Source: WoodMac")

# Verify insertion
total_buildings = int(arcpy.management.GetCount(gold_buildings)[0])
print(f"\n📊 gold_buildings total: {total_buildings} records")

# Show source distribution
print(f"\n📊 Updated Source Distribution:")
print("-" * 40)
source_counts = {}
with arcpy.da.SearchCursor(gold_buildings, ['source']) as cursor:
    for row in cursor:
        source = row[0]
        source_counts[source] = source_counts.get(source, 0) + 1

for source in sorted(source_counts.keys()):
    count = source_counts[source]
    pct = (count / total_buildings) * 100
    print(f"  • {source:20s}: {count:4d} ({pct:5.1f}%)")

print("\n" + "="*80)
print("✓ STEP 2 COMPLETE - Ready for campus rollup")
print("="*80)
