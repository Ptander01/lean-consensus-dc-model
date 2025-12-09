import arcpy
import os
from datetime import datetime
import re

# ============================================================================
# HELPER FUNCTIONS (EMBEDDED)
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

def status_to_rank(facility_status):
    """Convert facility_status to numeric rank for campus aggregation"""
    rank_map = {
        'Active': 1,
        'Under Construction': 2,
        'Permitting': 3,
        'Announced': 4,
        'Land Acquisition': 5,
        'Rumor': 6,
        'Unknown': 7
    }
    return rank_map.get(facility_status, 7)

def parse_company_name(organizations_str):
    """
    Extract primary company from Organizations field
    Examples: "Meta (FKA Facebook)" → "Meta"
              "Oracle | OpenAI | Related Digital" → "Oracle"
    """
    if not organizations_str:
        return 'Unknown'
    
    # Split by pipe (multiple orgs)
    orgs = str(organizations_str).split('|')
    
    # Take first org
    primary = orgs[0].strip()
    
    # Clean "Meta (FKA Facebook)" → "Meta"
    if 'Meta' in primary:
        return 'Meta'
    elif 'Oracle' in primary:
        return 'Oracle'
    elif 'OpenAI' in primary:
        return 'OpenAI'
    else:
        # Remove parenthetical
        primary = re.sub(r'\s*\([^)]*\)', '', primary)
        return primary.strip()

def parse_cost_string(cost_str):
    """
    Parse cost string to millions USD
    Examples: "USD 800M" → 800.0
              "USD 10,000M" → 10000.0
              "USD 1,500M" → 1500.0
    """
    if not cost_str or str(cost_str).strip() in ['', 'None', 'NULL']:
        return None
    
    try:
        # Extract numeric part (handle commas)
        numeric = re.sub(r'[^\d,.]', '', str(cost_str))
        numeric = numeric.replace(',', '')
        
        if numeric:
            return float(numeric)
    except:
        pass
    
    return None

def parse_building_size(size_str):
    """
    Parse building size to square feet
    Examples: "715,000" → 715000.0
              "4.00M" → 4000000.0
              "2.47M" → 2470000.0
              "480M" → 480000000.0
    """
    if not size_str or str(size_str).strip() in ['', 'None', 'NULL', '0']:
        return None
    
    try:
        size_str = str(size_str).strip()
        
        # Handle "M" suffix (millions)
        if 'M' in size_str.upper():
            numeric = re.sub(r'[^\d.]', '', size_str)
            if numeric:
                return float(numeric) * 1_000_000
        
        # Handle comma-separated (thousands)
        else:
            numeric = size_str.replace(',', '')
            if numeric:
                return float(numeric)
    except:
        pass
    
    return None

def parse_city_from_location(location_str, county=None):
    """
    Extract city name from Location field
    Examples: 
      "Meta Aiken Data Center, Aiken County, South Carolina, United States of America (building)" → "Aiken"
      "Cheyenne, Wyoming, United States of America (city)" → "Cheyenne"
      "Lebanon, IN 46052, United States of America (city)" → "Lebanon"
    """
    if not location_str:
        # Fall back to county name if no location
        if county:
            return str(county).replace(' County', '').replace(' Parish', '')
        return None
    
    location_str = str(location_str)
    
    # Remove everything after "(building)" or "(city)" or "(hamlet)" etc.
    location_str = re.sub(r'\s*\([^)]*\)', '', location_str)
    
    # Split by comma
    parts = [p.strip() for p in location_str.split(',')]
    
    # Filter out known non-city parts
    non_city = ['United States of America', 'USA']
    parts = [p for p in parts if p not in non_city and 'County' not in p and 'Parish' not in p]
    
    # Look for part with state abbreviation (e.g., "Lebanon, IN 46052")
    for part in parts:
        if re.search(r'\b[A-Z]{2}\b', part):  # State abbr
            # City is usually before state abbr
            city_match = re.match(r'^([^,]+)', part)
            if city_match:
                return city_match.group(1).strip()
    
    # If multiple parts, second part is often city
    if len(parts) >= 2:
        # Skip if first part looks like a building/facility name
        if 'data center' in parts[0].lower() or 'meta' in parts[0].lower() or 'oracle' in parts[0].lower():
            return parts[1]
        else:
            return parts[0]
    
    # Single part or couldn't determine - use first part
    if parts:
        return parts[0]
    
    # Last resort: use county
    if county:
        return str(county).replace(' County', '').replace(' Parish', '')
    
    return None

def get_state_abbr(state_name):
    """Convert full state name to abbreviation"""
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
    
    return state_map.get(str(state_name).strip())

def map_npm_status(npm_status):
    """
    Map NPM status to gold schema domain
    NPM values: Operational, Under Construction, Planned
    Gold values: Active, Under Construction, Permitting, Announced, Land Acquisition, Rumor, Unknown
    """
    if not npm_status:
        return 'Unknown'
    
    status_lower = str(npm_status).lower().strip()
    
    if 'operational' in status_lower:
        return 'Active'
    elif 'under construction' in status_lower:
        return 'Under Construction'
    elif 'planned' in status_lower:
        return 'Announced'
    else:
        return 'Unknown'

def build_npm_notes(onsite_gen=None, backup_gen=None, coords_precision=None, organizations=None):
    """Build structured notes from NPM metadata"""
    notes_parts = []
    
    if onsite_gen:
        notes_parts.append(f"Onsite generation: {onsite_gen} MW")
    if backup_gen:
        notes_parts.append(f"Backup generation: {backup_gen} MW")
    if coords_precision:
        notes_parts.append(f"Coordinates precision: {coords_precision}")
    if organizations and '|' in str(organizations):
        notes_parts.append(f"Partners: {organizations}")
    
    return " | ".join(notes_parts) if notes_parts else None

# ============================================================================
# MAIN INGESTION SCRIPT
# ============================================================================

# Environment setup
gdb_path = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
arcpy.env.workspace = gdb_path
arcpy.env.overwriteOutput = True

# Feature class paths
npm_fc = os.path.join(gdb_path, "NewProjectMedia_MetaOracle_ExportFeatures")
gold_buildings = os.path.join(gdb_path, "gold_buildings")

print("="*80)
print("NEW PROJECT MEDIA (NPM) INGESTION SCRIPT")
print("="*80)

# Verify source exists
if not arcpy.Exists(npm_fc):
    print(f"❌ ERROR: Source feature class not found: {npm_fc}")
    exit()

# Get source record count
source_count = int(arcpy.management.GetCount(npm_fc)[0])
print(f"\n📊 Source: NewProjectMedia_MetaOracle_ExportFeatures")
print(f"   Records: {source_count}")

# Define source fields to read (ALL LOWERCASE - matching actual field names)
source_fields = [
    'SHAPE@',
    'project',                      # lowercase
    'organizations',                # lowercase
    'status',                       # lowercase
    'total_mws',                    # lowercase
    'building_size__sq_ft_',        # lowercase
    'land_size__acre_',             # lowercase
    'planned_operation_date',       # lowercase
    'country',                      # lowercase
    'state___region',               # lowercase (3 underscores!)
    'county',                       # lowercase
    'onsite_generation__mw_',       # lowercase
    'backup_generation__mw_',       # lowercase
    'lat',                          # Use separate lat field
    'lon',                          # Use separate lon field
    'location',                     # lowercase
    'coordinates_precision',        # lowercase
    'cost',                         # lowercase
    'modified'                      # lowercase
]

print(f"\n✅ All required fields found in source")

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
    'company_source',
    'company_clean',
    'city',
    'state',
    'state_abbr',
    'county',
    'country',
    'region',
    'latitude',
    'longitude',
    'facility_status',
    'status_rank_tmp',
    'announced',
    'cod',
    'actual_live_date',
    'full_capacity_mw',
    'facility_sqft',
    'total_site_acres',
    'total_cost_usd_million',
    'notes',
    'ingest_date'
]

# Process records
current_date = datetime.now()
inserted_count = 0
error_count = 0
skipped_count = 0

print(f"\n🔄 Processing NPM records...")
print("-" * 80)

with arcpy.da.SearchCursor(npm_fc, source_fields) as s_cursor:
    with arcpy.da.InsertCursor(gold_buildings, insert_fields) as i_cursor:
        
        for row in s_cursor:
            try:
                # Unpack source row
                (geom, project, organizations, status, total_mws, building_size,
                 land_size, planned_op_date, country, state_region, county,
                 onsite_gen, backup_gen, lat, lon, location, coords_precision,
                 cost, modified) = row
                
                # ============================================================
                # VALIDATION - Skip if no geometry or no project name
                # ============================================================
                
                if not geom or not project:
                    skipped_count += 1
                    print(f"  ⚠ Skipped: No geometry or project name")
                    continue
                
                # ============================================================
                # FIELD TRANSFORMATIONS
                # ============================================================
                
                # Parse company name
                company_source = str(organizations) if organizations else 'Unknown'
                company_clean = parse_company_name(organizations)
                
                # Clean campus name (remove company prefix)
                campus_name = str(project).strip() if project else 'Unknown'
                # Remove "Meta - " or "Oracle - " prefix for cleaner campus_name
                campus_name = re.sub(r'^(Meta|Oracle|OpenAI)\s*-\s*', '', campus_name)
                
                # Generate unique_id (use slugified project name as ID)
                project_slug = slugify(project)
                unique_id = f"npm_{project_slug}"
                
                # Coordinates (already split into lat/lon fields!)
                # Skip if no valid coordinates
                if lat is None or lon is None:
                    skipped_count += 1
                    print(f"  ⚠ Skipped: {campus_name} - No valid coordinates")
                    continue
                
                # Parse city from location
                city = parse_city_from_location(location, county)
                
                # State and region
                state = str(state_region).strip() if state_region else None
                state_abbr = get_state_abbr(state)
                country_clean = 'United States'  # All NPM records are US
                region = 'AMER'
                
                # Generate campus_id
                campus_id = generate_campus_id(
                    company=company_clean,
                    city=city if city else (str(county).replace(' County', '').replace(' Parish', '') if county else 'Unknown'),
                    campus_name=campus_name
                )
                
                # Facility status
                facility_status = map_npm_status(status)
                status_rank = status_to_rank(facility_status)
                
                # Dates
                date_reported = modified if modified else current_date
                
                # Use planned_op_date for cod if status is Operational
                if facility_status == 'Active' and planned_op_date:
                    cod_date = planned_op_date
                    actual_live_date = planned_op_date
                    announced_date = None
                elif planned_op_date:
                    # Future date = announced/planned
                    announced_date = planned_op_date
                    cod_date = planned_op_date
                    actual_live_date = None
                else:
                    announced_date = None
                    cod_date = None
                    actual_live_date = None
                
                # Capacity
                full_capacity_mw = float(total_mws) if total_mws else None
                
                # Building size
                facility_sqft = parse_building_size(building_size)
                
                # Land size (already in acres)
                total_site_acres = float(land_size) if land_size else None
                
                # Cost
                total_cost_usd_million = parse_cost_string(cost)
                
                # Build notes
                notes = build_npm_notes(
                    onsite_gen=onsite_gen,
                    backup_gen=backup_gen,
                    coords_precision=coords_precision,
                    organizations=organizations if organizations and '|' in str(organizations) else None
                )
                
                # ============================================================
                # BUILD INSERT ROW
                # ============================================================
                
                insert_row = [
                    geom,                       # SHAPE@
                    unique_id,                  # unique_id
                    'NewProjectMedia',          # source
                    project_slug,               # source_unique_id
                    date_reported,              # date_reported
                    'Building',                 # record_level
                    campus_id,                  # campus_id
                    campus_name,                # campus_name
                    company_source,             # company_source
                    company_clean,              # company_clean
                    city,                       # city
                    state,                      # state
                    state_abbr,                 # state_abbr
                    county,                     # county
                    country_clean,              # country
                    region,                     # region
                    lat,                        # latitude
                    lon,                        # longitude
                    facility_status,            # facility_status
                    status_rank,                # status_rank_tmp
                    announced_date,             # announced
                    cod_date,                   # cod
                    actual_live_date,           # actual_live_date
                    full_capacity_mw,           # full_capacity_mw
                    facility_sqft,              # facility_sqft
                    total_site_acres,           # total_site_acres
                    total_cost_usd_million,     # total_cost_usd_million
                    notes,                      # notes
                    current_date                # ingest_date
                ]
                
                # Insert row
                i_cursor.insertRow(insert_row)
                inserted_count += 1
                
                # Print progress
                status_icon = "🟢" if facility_status == "Active" else "🟡" if facility_status == "Under Construction" else "⚪"
                mw_display = f"{full_capacity_mw:.0f} MW" if full_capacity_mw else "N/A"
                cost_display = f"${total_cost_usd_million:.0f}M" if total_cost_usd_million else "N/A"
                print(f"  {status_icon} {inserted_count}: {company_clean} - {city}, {state}")
                print(f"     └─ {campus_name} | {facility_status} | {mw_display} | {cost_display}")
                
            except Exception as e:
                error_count += 1
                print(f"  ❌ Error processing record: {str(e)}")
                import traceback
                traceback.print_exc()
                continue

print("-" * 80)
print(f"\n✅ NPM Ingestion Complete!")
print(f"   • Inserted: {inserted_count} records")
print(f"   • Skipped: {skipped_count} records (no geom/coords)")
print(f"   • Errors: {error_count} records")
print(f"   • Source: NewProjectMedia")

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
print("✓ NPM INGESTION COMPLETE - Ready for campus rollup")
print("="*80)