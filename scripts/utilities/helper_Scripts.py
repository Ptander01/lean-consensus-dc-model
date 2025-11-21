"""
Reusable helper functions for data center consensus project
"""
import re
from datetime import datetime

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
        clean_str = re.sub(r'[^\d.]', '', str(mw_str))
        if clean_str:
            return float(clean_str)
    except:
        pass
    return None

def determine_facility_status(announced, land_acq, construction, cod):
    """Determine facility status based on milestone dates"""
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
    """Extract building/phase designation from project name"""
    if not project_name:
        return None
    
    name_lower = str(project_name).lower()
    
    expansion_match = re.search(r'expansion\s*(\d+|[ivx]+)', name_lower)
    if expansion_match:
        phase_num = expansion_match.group(1)
        return f"Expansion Phase {phase_num.upper()}"
    
    building_match = re.search(r'building\s*(\d+|[a-z])', name_lower)
    if building_match:
        bldg_num = building_match.group(1)
        return f"Building {bldg_num.upper()}"
    
    return None

def build_structured_notes(**kwargs):
    """
    Build structured notes from metadata dictionary
    Usage: build_structured_notes(partner='X', workloads='AI', energy='Solar')
    """
    notes_parts = []
    
    field_labels = {
        'partner': 'Partner',
        'buildings': 'Buildings planned',
        'workloads': 'Workloads',
        'energy': 'Energy',
        'cooling': 'Cooling',
        'connectivity': 'Connectivity',
        'prior_use': 'Prior use',
        'notes': 'Notes'
    }
    
    for key, value in kwargs.items():
        if value:
            label = field_labels.get(key, key.title())
            notes_parts.append(f"{label}: {value}")
    
    return " | ".join(notes_parts) if notes_parts else None

def combine_references(initial_ref, addl_refs):
    """Combine multiple reference URLs into single field"""
    refs = []
    
    if initial_ref:
        refs.append(f"Initial: {initial_ref}")
    if addl_refs:
        refs.append(f"Additional: {addl_refs}")
    
    return "; ".join(refs) if refs else None

def standardize_region(country_or_region_name):
    """Map country/region name to AMER/EMEA/APAC/OTHER"""
    if not country_or_region_name:
        return None
    
    name = str(country_or_region_name).upper().strip()
    
    # AMER (Americas)
    amer = ['UNITED STATES', 'USA', 'US', 'CANADA', 'MEXICO', 
            'NAM', 'NORTH AMERICA', 'AMER']
    if any(a in name for a in amer):
        return 'AMER'
    
    # EMEA (Europe, Middle East, Africa)
    emea = ['EUROPE', 'EMEA', 'UNITED KINGDOM', 'UK', 'IRELAND', 'GERMANY', 
            'FRANCE', 'NETHERLANDS', 'BELGIUM', 'SPAIN', 'ITALY', 'SWEDEN',
            'NORWAY', 'DENMARK', 'FINLAND', 'POLAND', 'SWITZERLAND', 'AUSTRIA']
    if any(e in name for e in emea):
        return 'EMEA'
    
    # APAC (Asia Pacific)
    apac = ['APAC', 'ASIA', 'SINGAPORE', 'CHINA', 'JAPAN', 'SOUTH KOREA',
            'AUSTRALIA', 'NEW ZEALAND', 'INDIA', 'HONG KONG', 'TAIWAN']
    if any(a in name for a in apac):
        return 'APAC'
    
    return 'OTHER'

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
