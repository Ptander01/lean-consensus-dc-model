"""
Semianalysis Building Ingestion Script (CORRECTED)
Ingests Semianalysis Meta/Facebook building data into gold_buildings feature class.

FIXES APPLIED (Dec 9, 2024):
- Added latitude, longitude fields to insert (were missing, causing NULL coordinates)
- Added gold_lat, gold_lon fields as backup coordinates
- Added company_clean field (hardcoded to 'Meta')
- Added company_source field (from source data)
- Added record_level field
- Added date_reported field
- Improved region mapping (NorthAmerica → AMER)
- Improved country standardization (USA → United States)

Author: Meta Data Center GIS Team
Last Updated: 2024-12-09
"""

import arcpy
from datetime import datetime, timedelta
from collections import defaultdict
import re

# ============================================================================
# CONFIGURATION
# ============================================================================

arcpy.env.workspace = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"

source_fc = "SemiAnalysis_Building_MetaOracle_ExportFeatures"
target_fc = "gold_buildings"

SOURCE_NAME = "Semianalysis"

# Region mapping to standardize values
REGION_MAP = {
    'NorthAmerica': 'AMER',
    'North America': 'AMER',
    'NORTHAMERICA': 'AMER',
    'EMEA': 'EMEA',
    'APAC': 'APAC',
    'AMER': 'AMER',
    None: None,
    '': None
}

# Country standardization
COUNTRY_MAP = {
    'USA': 'United States',
    'US': 'United States',
    'U.S.': 'United States',
    'U.S.A.': 'United States',
    'United States': 'United States',
    'United States of America': 'United States'
}

# Country to region mapping for unmapped regions
COUNTRY_TO_REGION = {
    # Americas
    'United States': 'AMER',
    'USA': 'AMER',
    'Canada': 'AMER',
    'Mexico': 'AMER',
    'Brazil': 'AMER',
    'Chile': 'AMER',
    'Colombia': 'AMER',
    'Argentina': 'AMER',
    # EMEA
    'United Kingdom': 'EMEA',
    'UK': 'EMEA',
    'Ireland': 'EMEA',
    'Germany': 'EMEA',
    'France': 'EMEA',
    'Netherlands': 'EMEA',
    'Sweden': 'EMEA',
    'Denmark': 'EMEA',
    'Norway': 'EMEA',
    'Finland': 'EMEA',
    'Spain': 'EMEA',
    'Italy': 'EMEA',
    'Poland': 'EMEA',
    'Serbia': 'EMEA',
    'UAE': 'EMEA',
    'United Arab Emirates': 'EMEA',
    'Saudi Arabia': 'EMEA',
    'Israel': 'EMEA',
    'South Africa': 'EMEA',
    # APAC
    'Singapore': 'APAC',
    'Japan': 'APAC',
    'Australia': 'APAC',
    'New Zealand': 'APAC',
    'India': 'APAC',
    'Indonesia': 'APAC',
    'Malaysia': 'APAC',
    'Taiwan': 'APAC',
    'South Korea': 'APAC',
    'Hong Kong': 'APAC',
    'Philippines': 'APAC',
    'Thailand': 'APAC',
    'Vietnam': 'APAC'
}

# State abbreviation mapping
STATE_ABBR_MAP = {
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

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def excel_serial_to_date(serial_number):
    """Convert Excel serial date to Python datetime."""
    if serial_number is None or serial_number == '':
        return None

    try:
        serial = float(serial_number)

        if serial > 60:
            excel_epoch = datetime(1899, 12, 30)
            return excel_epoch + timedelta(days=serial)
        elif serial > 0:
            excel_epoch = datetime(1899, 12, 31)
            return excel_epoch + timedelta(days=serial - 1)
        else:
            return None
    except (ValueError, TypeError, OverflowError):
        return None


def slugify_text(text):
    """Convert text to URL-safe lowercase format."""
    if not text:
        return None
    # Convert to lowercase, replace spaces with hyphens
    safe_text = text.lower().strip()
    safe_text = re.sub(r'[^\w\s-]', '', safe_text)  # Remove special chars
    safe_text = re.sub(r'[-\s]+', '-', safe_text)   # Normalize hyphens/spaces
    return safe_text


def parse_cluster_name(cluster):
    """Parse cluster field to extract campus name and building designation."""
    if not cluster:
        return None, None

    clean = re.sub(r'^(Meta|Facebook)_', '', cluster)
    parts = clean.split('_')

    if len(parts) >= 2:
        campus_name = parts[0]
        building_num = parts[-1]
        building_designation = f"Building {building_num}"
        return campus_name, building_designation
    elif len(parts) == 1:
        return parts[0], None
    else:
        return None, None


def create_campus_id(company, city, campus_name):
    """
    Create standardized campus_id with CONSISTENT LOWERCASE formatting.

    Format: meta|city|campus (all lowercase, no spaces)
    Example: meta|altoona|altoona
    """
    if not all([company, city, campus_name]):
        return None

    # Normalize company name
    company_clean = "meta" if company.lower() in ["meta", "facebook"] else company.lower()

    # Slugify city and campus names (lowercase, URL-safe)
    city_clean = slugify_text(city)
    campus_clean = slugify_text(campus_name)

    return f"{company_clean}|{city_clean}|{campus_clean}"


def create_unique_id(uuid_str):
    """
    Create unique_id with ORIGINAL UUID format (keep hyphens).

    Format: Semianalysis_abc-123-def-456
    """
    if not uuid_str:
        return None

    # Keep original UUID format - DO NOT replace hyphens
    return f"Semianalysis_{uuid_str}"


def standardize_region(region, country):
    """
    Standardize region to valid values: AMER, EMEA, APAC.
    Falls back to country-based lookup if region is invalid.
    """
    # First try direct mapping
    if region in REGION_MAP:
        mapped = REGION_MAP[region]
        if mapped:
            return mapped

    # Fallback to country-based region
    if country:
        country_std = COUNTRY_MAP.get(country, country)
        if country_std in COUNTRY_TO_REGION:
            return COUNTRY_TO_REGION[country_std]

    return None


def standardize_country(country):
    """Standardize country name (e.g., USA → United States)."""
    if not country:
        return None
    return COUNTRY_MAP.get(country, country)


def get_state_abbr(state_name):
    """Convert state name to abbreviation."""
    if not state_name:
        return None
    # If already abbreviation, return as-is
    if len(str(state_name)) == 2:
        return str(state_name).upper()
    return STATE_ABBR_MAP.get(str(state_name).strip(), None)


def safe_float(value):
    """Safely convert to float."""
    if value in [None, '', ' ', 'None']:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def check_existing_records(target_fc):
    """
    Check for existing Semianalysis records before inserting.
    Returns set of existing unique_id values.
    """
    existing_ids = set()

    try:
        with arcpy.da.SearchCursor(target_fc, ['unique_id'],
                                   where_clause="source = 'Semianalysis'") as cursor:
            for row in cursor:
                if row[0]:
                    existing_ids.add(row[0])
    except Exception as e:
        print(f"   ⚠️  Warning: Could not check existing records: {e}")
        print("   Proceeding with insertion (may create duplicates if re-run)")

    return existing_ids


# ============================================================================
# MAIN INGESTION LOGIC
# ============================================================================

def ingest_semianalysis():
    """Ingest SemiAnalysis building-level data with all required fields."""

    print("=" * 80)
    print("SEMIANALYSIS BUILDING INGESTION - CORRECTED (v2.0)")
    print("=" * 80)
    print(f"Started: {datetime.now()}")
    print(f"Source: {source_fc}")
    print(f"Target: {target_fc}")

    # ========================================================================
    # STEP 0: Check for existing records
    # ========================================================================

    print("\n[0/5] Checking for existing Semianalysis records...")
    existing_ids = check_existing_records(target_fc)

    if len(existing_ids) > 0:
        print(f"   ⚠️  WARNING: Found {len(existing_ids)} existing Semianalysis records!")
        print("   These will be SKIPPED to prevent duplicates.")
        print("   If you want to re-ingest, delete existing records first:")
        print(f"   DELETE FROM {target_fc} WHERE source = 'Semianalysis'")
    else:
        print("   ✓ No existing Semianalysis records found - proceeding with fresh ingestion")

    # ========================================================================
    # STEP 1: Read source data
    # ========================================================================

    print("\n[1/5] Reading source data...")

    source_fields = [f.name for f in arcpy.ListFields(source_fc)]
    has_pivoted_capacity = any(f.startswith('mw_') for f in source_fields)

    # Check for lat/long field names (may vary)
    lat_field = 'lat' if 'lat' in source_fields else 'latitude'
    lon_field = 'long' if 'long' in source_fields else 'longitude'

    print(f"   Coordinate fields: {lat_field}, {lon_field}")
    print(f"   Has pivoted capacity: {has_pivoted_capacity}")

    if has_pivoted_capacity:
        print("   ✓ Source has pre-pivoted capacity fields (mw_YYYY)")
        read_fields = ['unique_id', 'cluster', 'company', 'city', 'us_state', 'country',
                       'region', 'market', 'zip_code', lat_field, lon_field,
                       'installed_capacity_mw', 'total_planned_mw',
                       'total_under_construction_mw', 'start_date__quarter_end',
                       'live_date__quarter_end', 'ds', 'SHAPE@']
        capacity_year_fields = [f for f in source_fields if f.startswith('mw_')]
        read_fields.extend(capacity_year_fields)
    else:
        print("   ✓ Source has time-series format (year/capacity columns)")
        read_fields = ['unique_id', 'cluster', 'company', 'city', 'us_state', 'country',
                       'region', 'market', 'zip_code', lat_field, lon_field,
                       'year', 'capacity', 'installed_capacity_mw',
                       'total_planned_mw', 'total_under_construction_mw',
                       'start_date__quarter_end', 'live_date__quarter_end', 'ds',
                       'SHAPE@']

    # Filter to only fields that exist
    available_fields = [f for f in read_fields if f in source_fields or f == 'SHAPE@']
    missing_fields = [f for f in read_fields if f not in source_fields and f != 'SHAPE@']
    if missing_fields:
        print(f"   ⚠️  Missing source fields (will be NULL): {missing_fields}")
    read_fields = available_fields

    buildings = defaultdict(lambda: {
        'records': [],
        'geometry': None,
        'cluster': None,
        'company': None,
        'city': None,
        'us_state': None,
        'country': None,
        'region': None,
        'market': None,
        'zip_code': None,
        'lat': None,
        'lon': None,
        'installed_capacity_mw': None,
        'total_planned_mw': None,
        'total_under_construction_mw': None,
        'start_date': None,
        'live_date': None,
        'ds': None,
        'capacity_by_year': {}
    })

    excluded_count = 0
    total_records = 0

    with arcpy.da.SearchCursor(source_fc, read_fields,
                               where_clause="company IN ('Meta', 'Facebook')") as cursor:
        field_indices = {field: i for i, field in enumerate(read_fields)}

        for row in cursor:
            total_records += 1

            # Extract values by field name
            unique_id = row[field_indices.get('unique_id', -1)] if 'unique_id' in field_indices else None
            cluster = row[field_indices.get('cluster', -1)] if 'cluster' in field_indices else None
            company = row[field_indices.get('company', -1)] if 'company' in field_indices else None
            city = row[field_indices.get('city', -1)] if 'city' in field_indices else None
            us_state = row[field_indices.get('us_state', -1)] if 'us_state' in field_indices else None
            country = row[field_indices.get('country', -1)] if 'country' in field_indices else None
            region = row[field_indices.get('region', -1)] if 'region' in field_indices else None
            market = row[field_indices.get('market', -1)] if 'market' in field_indices else None
            zip_code = row[field_indices.get('zip_code', -1)] if 'zip_code' in field_indices else None
            lat = row[field_indices.get(lat_field, -1)] if lat_field in field_indices else None
            lon = row[field_indices.get(lon_field, -1)] if lon_field in field_indices else None
            installed_cap = row[field_indices.get('installed_capacity_mw', -1)] if 'installed_capacity_mw' in field_indices else None
            total_planned = row[field_indices.get('total_planned_mw', -1)] if 'total_planned_mw' in field_indices else None
            total_uc = row[field_indices.get('total_under_construction_mw', -1)] if 'total_under_construction_mw' in field_indices else None
            start_date = row[field_indices.get('start_date__quarter_end', -1)] if 'start_date__quarter_end' in field_indices else None
            live_date = row[field_indices.get('live_date__quarter_end', -1)] if 'live_date__quarter_end' in field_indices else None
            ds = row[field_indices.get('ds', -1)] if 'ds' in field_indices else None
            geometry = row[field_indices.get('SHAPE@', -1)] if 'SHAPE@' in field_indices else None

            if not unique_id and not cluster:
                excluded_count += 1
                continue

            building_key = unique_id if unique_id else cluster
            building = buildings[building_key]

            # Handle capacity by year
            if has_pivoted_capacity:
                for cap_field in capacity_year_fields:
                    if cap_field in field_indices:
                        year_str = cap_field.replace('mw_', '')
                        building['capacity_by_year'][year_str] = row[field_indices[cap_field]]
            else:
                year = row[field_indices.get('year', -1)] if 'year' in field_indices else None
                capacity = row[field_indices.get('capacity', -1)] if 'capacity' in field_indices else None
                if year is not None:
                    year_str = str(int(year))
                    building['capacity_by_year'][year_str] = capacity

            # Store attributes (keep first non-null value)
            if geometry:
                building['geometry'] = geometry
            if cluster:
                building['cluster'] = cluster
            if company:
                building['company'] = company
            if city:
                building['city'] = city
            if us_state:
                building['us_state'] = us_state
            if country:
                building['country'] = country
            if region:
                building['region'] = region
            if market:
                building['market'] = market
            if zip_code:
                building['zip_code'] = zip_code
            if lat:
                building['lat'] = lat
            if lon:
                building['lon'] = lon
            if installed_cap:
                building['installed_capacity_mw'] = installed_cap
            if total_planned:
                building['total_planned_mw'] = total_planned
            if total_uc:
                building['total_under_construction_mw'] = total_uc
            if start_date:
                building['start_date'] = start_date
            if live_date:
                building['live_date'] = live_date
            if ds:
                building['ds'] = ds

    print(f"   Total source records: {total_records}")
    print(f"   Excluded (no ID/cluster): {excluded_count}")
    print(f"   Unique buildings: {len(buildings)}")

    # ========================================================================
    # STEP 2: Capacity pivot diagnostics
    # ========================================================================

    print("\n[2/5] Capacity Pivot Diagnostics...")

    capacity_stats = {
        'buildings_with_all_10_years': 0,
        'buildings_with_partial': 0,
        'buildings_with_no_capacity': 0,
        'years_distribution': defaultdict(int)
    }

    expected_years = [str(y) for y in range(2023, 2033)]

    for building_key, building_data in buildings.items():
        years_present = [y for y in expected_years if y in building_data['capacity_by_year']]

        if len(years_present) == 10:
            capacity_stats['buildings_with_all_10_years'] += 1
        elif len(years_present) > 0:
            capacity_stats['buildings_with_partial'] += 1
        else:
            capacity_stats['buildings_with_no_capacity'] += 1

        for year in years_present:
            capacity_stats['years_distribution'][year] += 1

    print(f"   Buildings with all 10 years (2023-2032): {capacity_stats['buildings_with_all_10_years']}")
    print(f"   Buildings with partial data: {capacity_stats['buildings_with_partial']}")
    print(f"   Buildings with no capacity data: {capacity_stats['buildings_with_no_capacity']}")

    # ========================================================================
    # STEP 3: Transform and insert into gold_buildings
    # ========================================================================

    print("\n[3/5] Inserting into gold_buildings...")

    # COMPLETE field list matching gold_buildings schema
    # This includes ALL required fields that were previously missing
    insert_fields = [
        'SHAPE@XY',
        # Core Identity (7 fields)
        'unique_id', 'source', 'source_unique_id', 'campus_id', 'campus_name',
        'building_designation', 'record_level',
        # Coordinates (4 fields) - FIXED: Now including all coordinate fields
        'latitude', 'longitude', 'gold_lat', 'gold_lon',
        # Company (2 fields) - FIXED: Now including company_clean
        'company_clean', 'company_source',
        # Geography (9 fields)
        'address', 'city', 'state', 'state_abbr', 'postal_code', 'county',
        'country', 'region', 'market',
        # Capacity - Current (6 fields)
        'commissioned_power_mw', 'uc_power_mw', 'planned_power_mw',
        'planned_plus_uc_mw', 'full_capacity_mw', 'available_power_kw',
        # Capacity - Forecast (10 fields)
        'mw_2023', 'mw_2024', 'mw_2025', 'mw_2026', 'mw_2027',
        'mw_2028', 'mw_2029', 'mw_2030', 'mw_2031', 'mw_2032',
        # Timeline (3 fields used)
        'date_reported', 'actual_live_date', 'construction_started',
        # Metadata
        'ingest_date', 'notes'
    ]

    insert_count = 0
    skipped_count = 0
    date_conversion_errors = 0
    coord_issues = 0

    with arcpy.da.InsertCursor(target_fc, insert_fields) as cursor:
        for building_key, building_data in buildings.items():

            # Create unique_id FIRST to check for duplicates
            unique_id = create_unique_id(building_key) if building_key else None

            # DUPLICATE CHECK: Skip if already exists
            if unique_id in existing_ids:
                skipped_count += 1
                continue

            # Parse cluster name
            campus_name, building_designation = parse_cluster_name(building_data['cluster'])
            campus_id = create_campus_id(building_data['company'], building_data['city'], campus_name)

            # Get coordinates - CRITICAL FIX
            lat = safe_float(building_data['lat'])
            lon = safe_float(building_data['lon'])

            # If lat/lon missing, try to extract from geometry
            if (lat is None or lon is None) and building_data['geometry']:
                try:
                    geom = building_data['geometry']
                    if hasattr(geom, 'centroid'):
                        lon = geom.centroid.X
                        lat = geom.centroid.Y
                    elif hasattr(geom, 'X'):
                        lon = geom.X
                        lat = geom.Y
                except:
                    pass

            if lat is None or lon is None:
                coord_issues += 1

            # Standardize country and region - CRITICAL FIX
            country_std = standardize_country(building_data['country'])
            region_std = standardize_region(building_data['region'], country_std)

            # Get state abbreviation
            state_abbr = get_state_abbr(building_data['us_state'])

            # Convert dates
            try:
                start_date_converted = excel_serial_to_date(building_data['start_date'])
                live_date_converted = excel_serial_to_date(building_data['live_date'])
                if isinstance(building_data['ds'], (int, float, str)):
                    ds_converted = excel_serial_to_date(building_data['ds'])
                else:
                    ds_converted = building_data['ds']
            except Exception as e:
                date_conversion_errors += 1
                start_date_converted = None
                live_date_converted = None
                ds_converted = None

            # Calculate capacity fields
            capacity_by_year = building_data['capacity_by_year']
            commissioned_mw = safe_float(building_data['installed_capacity_mw'])
            planned_mw = safe_float(building_data['total_planned_mw'])
            uc_mw = safe_float(building_data['total_under_construction_mw'])

            # Calculate derived capacity
            planned_plus_uc = None
            if planned_mw is not None or uc_mw is not None:
                planned_plus_uc = (planned_mw or 0) + (uc_mw or 0)

            full_capacity = None
            if any([commissioned_mw, planned_mw, uc_mw]):
                full_capacity = (commissioned_mw or 0) + (planned_mw or 0) + (uc_mw or 0)

            # Build notes
            notes = f"Source company: {building_data['company']}"

            # Build geometry tuple
            shape_xy = (lon, lat) if lon and lat else None

            # Build row values - ORDER MUST MATCH insert_fields
            row_values = [
                shape_xy,                          # SHAPE@XY
                # Core Identity
                unique_id,                         # unique_id
                SOURCE_NAME,                       # source
                building_key,                      # source_unique_id
                campus_id,                         # campus_id
                campus_name,                       # campus_name
                building_designation,              # building_designation
                'Building',                        # record_level
                # Coordinates - FIXED
                lat,                               # latitude
                lon,                               # longitude
                lat,                               # gold_lat (backup)
                lon,                               # gold_lon (backup)
                # Company - FIXED
                'Meta',                            # company_clean (hardcoded for Semianalysis)
                building_data['company'],          # company_source
                # Geography
                None,                              # address
                building_data['city'],             # city
                building_data['us_state'],         # state
                state_abbr,                        # state_abbr
                building_data['zip_code'],         # postal_code
                None,                              # county
                country_std,                       # country (standardized)
                region_std,                        # region (standardized)
                building_data['market'],           # market
                # Capacity - Current
                commissioned_mw,                   # commissioned_power_mw
                uc_mw,                             # uc_power_mw
                planned_mw,                        # planned_power_mw
                planned_plus_uc,                   # planned_plus_uc_mw
                full_capacity,                     # full_capacity_mw
                None,                              # available_power_kw
                # Capacity - Forecast (mw_2023 through mw_2032)
                safe_float(capacity_by_year.get('2023')),
                safe_float(capacity_by_year.get('2024')),
                safe_float(capacity_by_year.get('2025')),
                safe_float(capacity_by_year.get('2026')),
                safe_float(capacity_by_year.get('2027')),
                safe_float(capacity_by_year.get('2028')),
                safe_float(capacity_by_year.get('2029')),
                safe_float(capacity_by_year.get('2030')),
                safe_float(capacity_by_year.get('2031')),
                safe_float(capacity_by_year.get('2032')),
                # Timeline
                ds_converted,                      # date_reported
                live_date_converted,               # actual_live_date
                start_date_converted,              # construction_started
                # Metadata
                datetime.now(),                    # ingest_date
                notes                              # notes
            ]

            cursor.insertRow(row_values)
            insert_count += 1

    print(f"   ✓ Successfully inserted {insert_count} buildings")
    if skipped_count > 0:
        print(f"   ⏭️  Skipped {skipped_count} duplicate records")
    if coord_issues > 0:
        print(f"   ⚠️  Records with coordinate issues: {coord_issues}")
    if date_conversion_errors > 0:
        print(f"   ⚠️  Date conversion errors: {date_conversion_errors}")

    # ========================================================================
    # STEP 4: Post-Ingestion Validation
    # ========================================================================

    print("\n[4/5] Post-Ingestion Validation...")

    validation_query = "source = 'Semianalysis'"

    # Count total
    total_semi = int(arcpy.GetCount_management(target_fc, validation_query)[0])
    print(f"   Total Semianalysis records: {total_semi}")

    # Check required fields
    required_checks = {
        'latitude': 0,
        'longitude': 0,
        'company_clean': 0,
        'region': 0,
        'country': 0,
        'city': 0
    }

    with arcpy.da.SearchCursor(target_fc,
                               list(required_checks.keys()),
                               where_clause=validation_query) as cursor:
        for row in cursor:
            for i, field in enumerate(required_checks.keys()):
                if row[i] is None or row[i] == '':
                    required_checks[field] += 1

    print("\n   Required Field Completeness:")
    all_pass = True
    for field, null_count in required_checks.items():
        pct = ((total_semi - null_count) / total_semi * 100) if total_semi > 0 else 0
        status = "✓" if null_count == 0 else "⚠️"
        if null_count > 0:
            all_pass = False
        print(f"     {field}: {pct:.1f}% ({null_count} nulls) {status}")

    # Check region values
    print("\n   Region Distribution:")
    region_counts = defaultdict(int)
    with arcpy.da.SearchCursor(target_fc, ['region'],
                               where_clause=validation_query) as cursor:
        for row in cursor:
            region_counts[row[0] if row[0] else 'NULL'] += 1

    for region, count in sorted(region_counts.items(), key=lambda x: -x[1]):
        valid = "✓" if region in ['AMER', 'EMEA', 'APAC'] else "⚠️ INVALID"
        print(f"     {region}: {count} {valid}")

    # ========================================================================
    # STEP 5: Sample Records
    # ========================================================================

    print("\n[5/5] Sample Records Verification...")

    sample_fields = ['unique_id', 'campus_name', 'city', 'country', 'region',
                     'latitude', 'longitude', 'company_clean', 'mw_2025']

    print("\n   First 3 Semianalysis records:")
    sample_count = 0
    with arcpy.da.SearchCursor(target_fc, sample_fields,
                               where_clause=validation_query,
                               sql_clause=(None, 'ORDER BY unique_id')) as cursor:
        for row in cursor:
            if sample_count >= 3:
                break
            uid, campus, city, country, region, lat, lon, company, mw_2025 = row
            print(f"\n   Record {sample_count + 1}:")
            print(f"     unique_id: {uid}")
            print(f"     campus_name: {campus}")
            print(f"     city/country/region: {city} / {country} / {region}")
            print(f"     coordinates: ({lat}, {lon})")
            print(f"     company_clean: {company}")
            print(f"     mw_2025: {mw_2025}")
            sample_count += 1

    # ========================================================================
    # SUMMARY
    # ========================================================================

    print("\n" + "=" * 80)
    print("✅ SEMIANALYSIS INGESTION COMPLETE")
    print("=" * 80)
    print(f"\n📊 SUMMARY:")
    print(f"   • Buildings inserted: {insert_count}")
    print(f"   • Buildings skipped (duplicates): {skipped_count}")
    print(f"   • Total Semianalysis in gold_buildings: {total_semi}")
    print(f"   • Capacity years populated: {capacity_stats['buildings_with_all_10_years']} with all 10 years")
    print(f"   • Required fields validation: {'✅ PASS' if all_pass else '⚠️ CHECK ABOVE'}")
    print(f"\n⚠️  NEXT STEPS:")
    print(f"   1. Run validate_gold_buildings_data.py to verify all data quality checks")
    print(f"   2. Run campus_rollup.py to update gold_campus")
    print(f"\nCompleted: {datetime.now()}")

    return insert_count


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
