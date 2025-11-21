import arcpy
import os
from datetime import datetime

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------
gdb_path = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
arcpy.env.workspace = gdb_path
arcpy.env.overwriteOutput = True

synergy_source = os.path.join(gdb_path, "Synergy_MetaOracle_Consensus")
gold_buildings = os.path.join(gdb_path, "gold_buildings")
gold_campus = os.path.join(gdb_path, "gold_campus")

print("=" * 80)
print("SYNERGY DATA INGESTION")
print("=" * 80)

# --------------------------------------------------
# HELPER FUNCTIONS
# --------------------------------------------------

def parse_quarter_to_date(opened_str, year_int):
    """Convert Q318 format to date (end of quarter)"""
    if not opened_str or not year_int:
        return None
    
    try:
        opened_str = str(opened_str).strip().upper()
        
        # Handle special cases
        if 'PRE-' in opened_str:
            # "pre-2013" → use year - 1, Q4
            return datetime(year_int - 1, 12, 31)
        
        # Extract quarter (Q1, Q2, Q3, Q4)
        if opened_str.startswith('Q'):
            quarter_char = opened_str[1]
            quarter = int(quarter_char)
            
            # Quarter end months
            quarter_end = {1: 3, 2: 6, 3: 9, 4: 12}
            month = quarter_end.get(quarter, 12)
            
            # Last day of quarter
            if month == 3:
                day = 31
            elif month == 6:
                day = 30
            elif month == 9:
                day = 30
            else:  # December
                day = 31
            
            return datetime(year_int, month, day)
        
        # Fallback: just use Dec 31 of year
        return datetime(year_int, 12, 31)
        
    except Exception as e:
        print(f"  Warning: Could not parse '{opened_str}' for year {year_int}: {e}")
        if year_int:
            return datetime(year_int, 12, 31)
        return None

def standardize_region(synergy_region):
    """Map Synergy region names to AMER/EMEA/APAC"""
    if not synergy_region:
        return None
    
    region_upper = synergy_region.upper().strip()
    
    if region_upper in ['NORTH AMERICA', 'US', 'USA', 'UNITED STATES']:
        return 'AMER'
    elif region_upper in ['EUROPE', 'EMEA']:
        return 'EMEA'
    elif region_upper in ['APAC', 'ASIA PACIFIC', 'ASIA']:
        return 'APAC'
    elif region_upper in ['LATAM', 'LATIN AMERICA', 'SOUTH AMERICA']:
        return 'AMER'
    else:
        return 'OTHER'

def slugify(text):
    """Convert text to slug format"""
    if not text:
        return ''
    import re
    text = str(text).lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    return text.strip('-')

# --------------------------------------------------
# STEP 1: ANALYZE SOURCE DATA
# --------------------------------------------------
print("\n📊 STEP 1: Analyzing Synergy source data...")

synergy_count = int(arcpy.management.GetCount(synergy_source)[0])
print(f"  • Source records: {synergy_count}")

# Count unique facilities (by company + city)
unique_facilities = set()
company_counts = {}
region_counts = {}

with arcpy.da.SearchCursor(synergy_source, ['company', 'city_or_subregion', 'region', 'quantity']) as cursor:
    for row in cursor:
        company = row[0]
        city = row[1]
        region = row[2]
        quantity = row[3]
        
        if company and city:
            unique_facilities.add((company, city))
        
        company_counts[company] = company_counts.get(company, 0) + 1
        region_counts[region] = region_counts.get(region, 0) + 1

print(f"  • Unique facilities (company + city): {len(unique_facilities)}")
print(f"\n  Companies:")
for company, count in sorted(company_counts.items(), key=lambda x: x[1], reverse=True):
    print(f"    • {company}: {count} records")

print(f"\n  Regions:")
for region, count in sorted(region_counts.items(), key=lambda x: x[1], reverse=True):
    std_region = standardize_region(region)
    print(f"    • {region} → {std_region}: {count} records")

# --------------------------------------------------
# STEP 2: CHECK FOR DUPLICATES WITH EXISTING DATA
# --------------------------------------------------
print("\n🔍 STEP 2: Checking for duplicates with existing data...")

# Get existing records from Synergy source
existing_synergy = set()
with arcpy.da.SearchCursor(gold_buildings, ['campus_id', 'source']) as cursor:
    for row in cursor:
        if row[1] == 'Synergy':
            existing_synergy.add(row[0])

if existing_synergy:
    print(f"  ⚠ Warning: Found {len(existing_synergy)} existing Synergy records in gold_buildings")
    print(f"  These will be skipped to avoid duplicates")
else:
    print(f"  ✓ No existing Synergy records found")

# --------------------------------------------------
# STEP 3: MAP AND APPEND TO gold_buildings
# --------------------------------------------------
print("\n📥 STEP 3: Mapping Synergy data to gold_buildings schema...")

# Get current count
before_count = int(arcpy.management.GetCount(gold_buildings)[0])

# Prepare insert cursor fields
insert_fields = [
    'SHAPE@', 'unique_id', 'source', 'source_unique_id', 'date_reported',
    'record_level', 'campus_id', 'campus_name', 'company_source', 'company_clean',
    'address', 'postal_code', 'city', 'market', 'state', 'state_abbr', 'county',
    'country', 'region', 'latitude', 'longitude',
    'actual_live_date', 'facility_status', 'owned_leased', 'ingest_date'
]

# Read from Synergy and insert
synergy_fields = [
    'field1', 'company', 'region', 'country', 'city_or_us_state', 'city_or_subregion',
    'owned_or_leased_partner', 'quantity', 'opened', 'year_opened', 'search',
    'x', 'y', 'State_Abbr', 'Metro_admin', 'County', 'ZipCode', 'Market', 'SHAPE@'
]

inserted_count = 0
skipped_count = 0
current_date = datetime.now()

print(f"  • Processing {synergy_count} Synergy records...")

with arcpy.da.SearchCursor(synergy_source, synergy_fields) as s_cursor:
    with arcpy.da.InsertCursor(gold_buildings, insert_fields) as i_cursor:
        for row in s_cursor:
            field1, company, syn_region, country, state_name, city, owned_leased, \
            quantity, opened, year_opened, search, x, y, state_abbr, metro_admin, \
            county, zipcode, market, shape = row
            
            # Skip if no company or city
            if not company or not city:
                skipped_count += 1
                continue
            
            # Create campus_id
            company_slug = slugify(company)
            city_slug = slugify(city)
            campus_id = f"{company_slug}|{city_slug}|synergy"
            
            # Skip if already exists
            if campus_id in existing_synergy:
                skipped_count += 1
                continue
            
            # Create unique_id
            unique_id = f"synergy_{field1 if field1 else inserted_count}"
            
            # Standardize region
            region = standardize_region(syn_region)
            
            # Parse opened date
            live_date = parse_quarter_to_date(opened, year_opened)
            
            # Map owned/leased
            if owned_leased == 'O':
                owned_leased_val = 'Owned'
            elif owned_leased == 'L':
                owned_leased_val = 'Leased'
            else:
                owned_leased_val = None
            
            # Determine facility_status based on live_date
            if live_date and live_date <= current_date:
                facility_status = 'Active'
            elif live_date:
                facility_status = 'Announced'
            else:
                facility_status = 'Unknown'
            
            # Campus name (simplified)
            campus_name = f"{company} {city}"
            
            # For US records, use city_or_us_state as state
            if country and 'united states' in country.lower():
                state = state_name
            else:
                state = None
            
            # Build insert row
            insert_row = [
                shape,  # SHAPE@
                unique_id,
                'Synergy',
                str(field1) if field1 else None,
                current_date,  # date_reported
                'Building',
                campus_id,
                campus_name,
                company,  # company_source
                company,  # company_clean
                None,  # address
                zipcode,
                city,
                market,
                state,
                state_abbr,
                county,
                country,
                region,
                y,  # latitude
                x,  # longitude
                live_date,
                facility_status,
                owned_leased_val,
                current_date  # ingest_date
            ]
            
            i_cursor.insertRow(insert_row)
            inserted_count += 1

print(f"  ✓ Inserted: {inserted_count} records")
print(f"  • Skipped: {skipped_count} records")

# Verify
after_count = int(arcpy.management.GetCount(gold_buildings)[0])
print(f"\n  gold_buildings count: {before_count} → {after_count} (+{after_count - before_count})")

# --------------------------------------------------
# STEP 4: RE-RUN CAMPUS ROLLUP
# --------------------------------------------------
print("\n🔄 STEP 4: Re-running campus rollup...")

# Clear gold_campus
arcpy.management.TruncateTable(gold_campus)

# Get unique campus_ids
unique_campuses = set()
with arcpy.da.SearchCursor(gold_buildings, ['campus_id']) as cursor:
    for row in cursor:
        if row[0]:
            unique_campuses.add(row[0])

print(f"  • Unique campus_ids in gold_buildings: {len(unique_campuses)}")

# Create temp layers
timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
dissolved_fc = os.path.join(gdb_path, f"temp_synergy_dissolved_{timestamp}")
points_fc = os.path.join(gdb_path, f"temp_synergy_points_{timestamp}")

# Dissolve
stat_fields = [
    ['company_clean', 'FIRST'], ['campus_name', 'FIRST'], ['city', 'FIRST'],
    ['market', 'FIRST'], ['state', 'FIRST'], ['state_abbr', 'FIRST'],
    ['county', 'FIRST'], ['country', 'FIRST'], ['region', 'FIRST'],
    ['postal_code', 'FIRST'], ['address', 'FIRST'],
    ['planned_power_mw', 'SUM'], ['uc_power_mw', 'SUM'],
    ['commissioned_power_mw', 'SUM'], ['full_capacity_mw', 'SUM'],
    ['facility_sqft', 'SUM'], ['whitespace_sqft', 'SUM'],
    ['actual_live_date', 'MIN'], ['status_rank_tmp', 'MIN'],
    ['cancelled', 'MAX'], ['unique_id', 'COUNT']
]

arcpy.analysis.PairwiseDissolve(
    gold_buildings, dissolved_fc, ['campus_id'], stat_fields, multi_part="MULTI_PART"
)

dissolved_count = int(arcpy.management.GetCount(dissolved_fc)[0])
print(f"  • Dissolved: {dissolved_count} campus features")

# Feature to Point
arcpy.management.FeatureToPoint(dissolved_fc, points_fc, "INSIDE")

# Add and map fields (same as before)
campus_fields = [f for f in arcpy.ListFields(gold_campus) 
                 if f.type not in ['OID', 'Geometry'] and f.name != 'campus_id']

for field in campus_fields:
    if not arcpy.ListFields(points_fc, field.name):
        arcpy.management.AddField(points_fc, field.name, field.type,
                                 field_length=field.length if hasattr(field, 'length') else None)

# Field mapping
field_mapping = {
    'company_clean': 'FIRST_company_clean', 'campus_name': 'FIRST_campus_name',
    'city': 'FIRST_city', 'market': 'FIRST_market', 'state': 'FIRST_state',
    'state_abbr': 'FIRST_state_abbr', 'county': 'FIRST_county',
    'country': 'FIRST_country', 'region': 'FIRST_region',
    'postal_code': 'FIRST_postal_code', 'address': 'FIRST_address',
    'planned_power_mw': 'SUM_planned_power_mw', 'uc_power_mw': 'SUM_uc_power_mw',
    'commissioned_power_mw': 'SUM_commissioned_power_mw',
    'full_capacity_mw': 'SUM_full_capacity_mw',
    'facility_sqft_sum': 'SUM_facility_sqft', 'whitespace_sqft_sum': 'SUM_whitespace_sqft',
    'building_count': 'COUNT_unique_id', 'first_live_date': 'MIN_actual_live_date',
    'cancelled': 'MAX_cancelled'
}

temp_fields = [f.name for f in arcpy.ListFields(points_fc)]
for target, source in field_mapping.items():
    if target in temp_fields and source in temp_fields:
        arcpy.management.CalculateField(points_fc, target, f'!{source}!', "PYTHON3")

# Derived fields
arcpy.management.CalculateField(points_fc, 'planned_plus_uc_mw',
    '(!planned_power_mw! if !planned_power_mw! else 0) + (!uc_power_mw! if !uc_power_mw! else 0)', "PYTHON3")

status_map = {1:'Active', 2:'Under Construction', 3:'Permitting', 
              4:'Announced', 5:'Land Acquisition', 6:'Rumor'}
with arcpy.da.UpdateCursor(points_fc, ['MIN_status_rank_tmp', 'facility_status_agg']) as cur:
    for row in cur:
        row[1] = status_map.get(row[0], 'Unknown')
        cur.updateRow(row)

arcpy.management.CalculateField(points_fc, 'record_level', "'Campus'", "PYTHON3")

with arcpy.da.UpdateCursor(points_fc, ['ingest_date']) as cur:
    for row in cur:
        row[0] = current_date
        cur.updateRow(row)

# Append to gold_campus
arcpy.management.Append(points_fc, gold_campus, schema_type="NO_TEST")

campus_count = int(arcpy.management.GetCount(gold_campus)[0])
print(f"  • gold_campus: {campus_count} records")

# Cleanup
for fc in [dissolved_fc, points_fc]:
    if arcpy.Exists(fc):
        arcpy.management.Delete(fc)

# --------------------------------------------------
# FINAL SUMMARY
# --------------------------------------------------
print("\n" + "=" * 80)
print("SYNERGY INGESTION COMPLETE")
print("=" * 80)

# Count by source
source_counts = {}
with arcpy.da.SearchCursor(gold_buildings, ['source']) as cursor:
    for row in cursor:
        source = row[0] or '(null)'
        source_counts[source] = source_counts.get(source, 0) + 1

print(f"\n📊 gold_buildings by source:")
for source, count in sorted(source_counts.items()):
    pct = (count / after_count * 100) if after_count > 0 else 0
    print(f"  • {source:<20} {count:>4} ({pct:>5.1f}%)")

print(f"\n📊 gold_campus:")
print(f"  • Total records: {campus_count}")

# Region breakdown
region_counts = {}
with arcpy.da.SearchCursor(gold_campus, ['region']) as cursor:
    for row in cursor:
        region = row[0] or '(null)'
        region_counts[region] = region_counts.get(region, 0) + 1

print(f"\n  By region:")
for region in ['AMER', 'EMEA', 'APAC', 'OTHER', '(null)']:
    if region in region_counts:
        print(f"    • {region}: {region_counts[region]}")

print("\n" + "=" * 80)
print("✅ Ready to ingest next source (WoodMac, Semianalysis, or NPM)!")
print("=" * 80) 

gold_buildings = os.path.join(gdb_path, "gold_buildings")
gold_campus = os.path.join(gdb_path, "gold_campus")

print("=" * 80)
print("FIXING SYNERGY REGION MAPPING")
print("=" * 80)

# Check current "OTHER" region records from Synergy
print("\n🔍 Checking Synergy records with region='OTHER'...")

other_count = 0
nam_countries = set()

with arcpy.da.SearchCursor(gold_buildings, ['source', 'region', 'country', 'city']) as cursor:
    for row in cursor:
        if row[0] == 'Synergy' and row[1] == 'OTHER':
            other_count += 1
            nam_countries.add(row[2])

print(f"  • Found {other_count} Synergy records with region='OTHER'")
print(f"  • Countries: {', '.join(sorted(nam_countries))}")

# Fix: Set region='AMER' for Synergy records in US/Canada/Mexico
print(f"\n🔧 Updating region for North American Synergy records...")

update_count = 0

# Map of countries that should be AMER
amer_countries = ['United States', 'USA', 'US', 'Canada', 'Mexico']

with arcpy.da.UpdateCursor(gold_buildings, ['source', 'region', 'country']) as cursor:
    for row in cursor:
        if row[0] == 'Synergy' and row[1] == 'OTHER':
            country = row[2]
            if country and any(c.lower() in country.lower() for c in amer_countries):
                row[1] = 'AMER'
                cursor.updateRow(row)
                update_count += 1

print(f"  ✓ Updated {update_count} records: 'OTHER' → 'AMER'")

# Also update gold_campus
print(f"\n🔧 Updating gold_campus regions...")

campus_update_count = 0

with arcpy.da.UpdateCursor(gold_campus, ['region', 'country']) as cursor:
    for row in cursor:
        if row[0] == 'OTHER':
            country = row[1]
            if country and any(c.lower() in country.lower() for c in amer_countries):
                row[0] = 'AMER'
                cursor.updateRow(row)
                campus_update_count += 1

print(f"  ✓ Updated {campus_update_count} campus records")

# Verify
print(f"\n✅ Verification:")

# gold_buildings by region
print(f"\n  gold_buildings by region:")
region_counts_b = {}
with arcpy.da.SearchCursor(gold_buildings, ['region']) as cursor:
    for row in cursor:
        region = row[0] or '(null)'
        region_counts_b[region] = region_counts_b.get(region, 0) + 1

for region in ['AMER', 'EMEA', 'APAC', 'OTHER', '(null)']:
    if region in region_counts_b:
        print(f"    • {region}: {region_counts_b[region]}")

# gold_campus by region
print(f"\n  gold_campus by region:")
region_counts_c = {}
with arcpy.da.SearchCursor(gold_campus, ['region']) as cursor:
    for row in cursor:
        region = row[0] or '(null)'
        region_counts_c[region] = region_counts_c.get(region, 0) + 1

for region in ['AMER', 'EMEA', 'APAC', 'OTHER', '(null)']:
    if region in region_counts_c:
        print(f"    • {region}: {region_counts_c[region]}")

print("\n" + "=" * 80)
print("✓ REGION FIX COMPLETE")
print("=" * 80)
