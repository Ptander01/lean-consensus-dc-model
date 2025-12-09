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

    🔧 FIX #1: Normalize all components to lowercase to prevent duplicates
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

    🔧 FIX #2: Keep hyphens in UUID - don't replace them
    This prevents creating duplicate records with different ID formats.

    Format: Semianalysis_abc-123-def-456
    """
    if not uuid_str:
        return None

    # Keep original UUID format - DO NOT replace hyphens
    return f"Semianalysis_{uuid_str}"


def check_existing_records(target_fc):
    """
    🔧 FIX #3: Check for existing Semianalysis records before inserting.
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
        print(f"   Proceeding with insertion (may create duplicates if re-run)")

    return existing_ids


# ============================================================================
# MAIN INGESTION LOGIC
# ============================================================================

def ingest_semianalysis():
    """Ingest SemiAnalysis building-level data."""

    print("=" * 80)
    print("SEMIANALYSIS BUILDING INGESTION - FIXED DUPLICATE PREVENTION")
    print("=" * 80)

    # ========================================================================
    # STEP 0: Check for existing records
    # ========================================================================

    print("\nStep 0: Checking for existing Semianalysis records...")
    existing_ids = check_existing_records(target_fc)

    if len(existing_ids) > 0:
        print(f"   ⚠️  WARNING: Found {len(existing_ids)} existing Semianalysis records!")
        print(f"   These will be SKIPPED to prevent duplicates.")
        print(f"   If you want to re-ingest, delete existing records first.")
    else:
        print(f"   ✓ No existing Semianalysis records found - proceeding with fresh ingestion")

    # ========================================================================
    # STEP 1: Read source data
    # ========================================================================

    print("\nStep 1: Reading source data...")

    source_fields = [f.name for f in arcpy.ListFields(source_fc)]
    has_pivoted_capacity = any(f.startswith('mw_') for f in source_fields)

    if has_pivoted_capacity:
        print("   ✓ Source has pre-pivoted capacity fields (mw_YYYY)")
        read_fields = ['unique_id', 'cluster', 'company', 'city', 'us_state', 'country',
                      'region', 'installed_capacity_mw', 'total_planned_mw',
                      'total_under_construction_mw', 'start_date__quarter_end',
                      'live_date__quarter_end', 'ds', 'SHAPE@']
        capacity_year_fields = [f for f in source_fields if f.startswith('mw_')]
        read_fields.extend(capacity_year_fields)
    else:
        print("   ✓ Source has time-series format (year/capacity columns)")
        read_fields = ['unique_id', 'cluster', 'company', 'city', 'us_state', 'country',
                      'region', 'year', 'capacity', 'installed_capacity_mw',
                      'total_planned_mw', 'total_under_construction_mw',
                      'start_date__quarter_end', 'live_date__quarter_end', 'ds',
                      'SHAPE@']

    buildings = defaultdict(lambda: {
        'records': [],
        'geometry': None,
        'cluster': None,
        'company': None,
        'city': None,
        'us_state': None,
        'country': None,
        'region': None,
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
        for row in cursor:
            total_records += 1

            if has_pivoted_capacity:
                (unique_id, cluster, company, city, us_state, country, region,
                 installed_cap, total_planned, total_uc, start_date, live_date, ds,
                 geometry, *capacity_values) = row
                year = None
                capacity = None
            else:
                (unique_id, cluster, company, city, us_state, country, region, year,
                 capacity, installed_cap, total_planned, total_uc, start_date,
                 live_date, ds, geometry) = row

            if not unique_id and not cluster:
                excluded_count += 1
                continue

            building_key = unique_id if unique_id else cluster
            building = buildings[building_key]

            if has_pivoted_capacity:
                for i, field_name in enumerate(capacity_year_fields):
                    year_str = field_name.replace('mw_', '')
                    building['capacity_by_year'][year_str] = capacity_values[i]
            else:
                if year is not None:
                    year_str = str(int(year))
                    building['capacity_by_year'][year_str] = capacity

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
    print(f"   Excluded (Oracle/colo): {excluded_count}")
    print(f"   Unique buildings: {len(buildings)}")

    # ========================================================================
    # STEP 2: Capacity pivot diagnostics
    # ========================================================================

    print("\nStep 2: Capacity Pivot Diagnostics...")

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
    print(f"\n   Year coverage:")
    for year in expected_years:
        count = capacity_stats['years_distribution'][year]
        percentage = (count / len(buildings) * 100) if len(buildings) > 0 else 0
        print(f"     {year}: {count} buildings ({percentage:.1f}%)")

    # ========================================================================
    # STEP 3: Transform and insert into gold_buildings
    # ========================================================================

    print("\nStep 3: Inserting into gold_buildings...")

    insert_fields = [
        'unique_id', 'source', 'source_unique_id', 'campus_id', 'campus_name',
        'building_designation', 'city', 'state', 'country', 'region',
        'commissioned_power_mw', 'uc_power_mw', 'planned_power_mw', 'full_capacity_mw',
        'mw_2023', 'mw_2024', 'mw_2025', 'mw_2026', 'mw_2027', 'mw_2028',
        'mw_2029', 'mw_2030', 'mw_2031', 'mw_2032',
        'ingest_date', 'actual_live_date', 'construction_started', 'SHAPE@'
    ]

    insert_count = 0
    skipped_count = 0
    date_conversion_errors = 0

    with arcpy.da.InsertCursor(target_fc, insert_fields) as cursor:
        for building_key, building_data in buildings.items():

            # Create unique_id FIRST to check for duplicates
            unique_id = create_unique_id(building_key) if building_key else None

            # 🔧 DUPLICATE CHECK: Skip if already exists
            if unique_id in existing_ids:
                skipped_count += 1
                continue

            campus_name, building_designation = parse_cluster_name(building_data['cluster'])
            campus_id = create_campus_id(building_data['company'], building_data['city'], campus_name)

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

            capacity_by_year = building_data['capacity_by_year']

            row_values = [
                unique_id,
                "Semianalysis",
                building_key,
                campus_id,
                campus_name,
                building_designation,
                building_data['city'],
                building_data['us_state'],
                building_data['country'],
                building_data['region'],
                building_data['installed_capacity_mw'],
                building_data['total_under_construction_mw'],
                building_data['total_planned_mw'],
                building_data['installed_capacity_mw'],
                capacity_by_year.get('2023'),
                capacity_by_year.get('2024'),
                capacity_by_year.get('2025'),
                capacity_by_year.get('2026'),
                capacity_by_year.get('2027'),
                capacity_by_year.get('2028'),
                capacity_by_year.get('2029'),
                capacity_by_year.get('2030'),
                capacity_by_year.get('2031'),
                capacity_by_year.get('2032'),
                datetime.now(),
                live_date_converted,
                start_date_converted,
                building_data['geometry']
            ]

            cursor.insertRow(row_values)
            insert_count += 1

    print(f"   ✓ Successfully inserted {insert_count} buildings")
    if skipped_count > 0:
        print(f"   ⏭️  Skipped {skipped_count} duplicate records")
    if date_conversion_errors > 0:
        print(f"   ⚠️  Date conversion errors: {date_conversion_errors}")

    # ========================================================================
    # STEP 4: Validation
    # ========================================================================

    print("\nStep 4: Post-Ingestion Validation...")

    validation_fields = ['unique_id', 'campus_id', 'campus_name', 'building_designation',
                        'mw_2023', 'mw_2024', 'mw_2025', 'mw_2026', 'mw_2027',
                        'mw_2028', 'mw_2029', 'mw_2030', 'mw_2031', 'mw_2032',
                        'actual_live_date', 'construction_started']

    print("\n   Sample Buildings - Capacity Verification:")

    sample_count = 0
    with arcpy.da.SearchCursor(target_fc, validation_fields,
                               where_clause="source = 'Semianalysis'",
                               sql_clause=(None, 'ORDER BY unique_id')) as cursor:
        for row in cursor:
            if sample_count >= 3:
                break

            uid, cid, campus, building, *capacity_years, live_date, start_date = row

            print(f"\n   Building: {campus} - {building}")
            print(f"     unique_id: {uid}")
            print(f"     campus_id: {cid}")

            years_with_data = sum(1 for c in capacity_years if c is not None)
            print(f"     Capacity years populated: {years_with_data}/10")

            if years_with_data > 0:
                print(f"     Sample capacity values:")
                for i, year in enumerate(range(2023, 2033)):
                    if capacity_years[i] is not None:
                        print(f"       {year}: {capacity_years[i]:.1f} MW")

            print(f"     Construction started: {start_date}")
            print(f"     Live date: {live_date}")

            sample_count += 1

    # ========================================================================
    # STEP 5: Check for remaining duplicates
    # ========================================================================

    print("\n" + "=" * 80)
    print("DUPLICATE CHECK")
    print("=" * 80)

    campus_id_counts = defaultdict(int)
    with arcpy.da.SearchCursor(target_fc, ['campus_id'],
                               where_clause="source = 'Semianalysis'") as cursor:
        for row in cursor:
            if row[0]:
                campus_id_counts[row[0]] += 1

    unique_campuses = len(campus_id_counts)
    total_semi_records = sum(campus_id_counts.values())

    print(f"   Total Semianalysis records: {total_semi_records}")
    print(f"   Unique campus_id values: {unique_campuses}")

    # Check for duplicates
    duplicates = {cid: count for cid, count in campus_id_counts.items() if count > len(buildings) / unique_campuses}

    if duplicates:
        print(f"   ⚠️  WARNING: Possible duplicate campus_id values detected:")
        for cid, count in list(duplicates.items())[:5]:
            print(f"     - {cid}: {count} buildings")
    else:
        print(f"   ✓ No duplicate campus_id patterns detected")

    print("\n" + "=" * 80)
    print("INGESTION COMPLETE")
    print("=" * 80)
    print(f"\nSummary:")
    print(f"  Total buildings inserted: {insert_count}")
    print(f"  Buildings skipped (duplicates): {skipped_count}")
    print(f"  Buildings with complete capacity data: {capacity_stats['buildings_with_all_10_years']}")
    print(f"  Buildings with partial capacity data: {capacity_stats['buildings_with_partial']}")
    print(f"  Buildings with no capacity data: {capacity_stats['buildings_with_no_capacity']}")
    print(f"  Date conversion errors: {date_conversion_errors}")
    print(f"  Unique campuses: {unique_campuses}")


if __name__ == "__main__":
    ingest_semianalysis()
