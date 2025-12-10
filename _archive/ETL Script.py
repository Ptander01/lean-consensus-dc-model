# ------------------ CONFIG ------------------
import arcpy, datetime, re

gdb = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
source_fc = fr"{gdb}\DCM_MetaOracle_Consensus"
b_fc = fr"{gdb}\gold_buildings"
c_fc = fr"{gdb}\gold_campus"

arcpy.env.workspace = gdb
arcpy.env.overwriteOutput = True

# ------------------ (A) Domains & Feature Classes ------------------
def list_domain_names(workspace):
    return {d.name for d in arcpy.da.ListDomains(workspace)}

def ensure_domain(workspace, name, description, field_type, coded_values):
    existing = list_domain_names(workspace)
    if name not in existing:
        arcpy.management.CreateDomain(workspace, name, description, field_type, "CODED")
        for code, val in coded_values.items():
            arcpy.management.AddCodedValueToDomain(workspace, name, code, val)
        print(f"Created domain {name}")
    else:
        print(f"Domain {name} already exists")

def create_fc(name, fields):
    if arcpy.Exists(f"{gdb}\\{name}"):
        print(f"{name} already exists – skipping creation")
        return f"{gdb}\\{name}"
    sr = arcpy.SpatialReference(4326)
    fc = arcpy.management.CreateFeatureclass(gdb, name, "POINT", spatial_reference=sr)[0]
    for fld in fields:
        fname, ftype, flen, falias, fdom = fld
        arcpy.management.AddField(
            fc, fname, ftype,
            field_length=(flen if flen else None),
            field_alias=falias
        )
        if fdom:
            arcpy.management.AssignDomainToField(fc, fname, fdom)
    print(f"Created {name}")
    return fc

# Domains
ensure_domain(gdb, "dm_facility_status", "Facility lifecycle status", "TEXT",
    {"Rumor":"Rumor","Announced":"Announced","Land Acquisition":"Land Acquisition","Permitting":"Permitting",
     "Under Construction":"Under Construction","Active":"Active","Unknown":"Unknown"})
ensure_domain(gdb, "dm_record_level", "Granularity of record", "TEXT", {"Building":"Building","Campus":"Campus"})

# Feature class fields (abbreviated for brevity; expand as needed)
buildings_fields = [
    ("unique_id","TEXT",64,"Unique ID",None), ("source","TEXT",64,"Source",None),
    ("source_unique_id","TEXT",64,"Source Unique ID",None), ("date_reported","DATE",None,"Date Reported",None),
    ("record_level","TEXT",16,"Record Level","dm_record_level"), ("campus_id","TEXT",128,"Campus ID",None),
    ("campus_name","TEXT",128,"Campus/Project Name",None), ("company_clean","TEXT",128,"Company (Clean)",None),
    ("address","TEXT",255,"Address",None), ("postal_code","TEXT",16,"Zip/Postal",None), ("city","TEXT",128,"City",None),
    ("market","TEXT",128,"Market",None), ("state","TEXT",64,"State",None), ("state_abbr","TEXT",8,"State Abbr",None),
    ("county","TEXT",128,"County",None), ("country","TEXT",64,"Country",None), ("region","TEXT",16,"Region",None),
    ("latitude","DOUBLE",None,"Latitude",None), ("longitude","DOUBLE",None,"Longitude",None),
    ("planned_power_mw","DOUBLE",None,"Planned Power (MW)",None), ("uc_power_mw","DOUBLE",None,"Under Construction Power (MW)",None),
    ("commissioned_power_mw","DOUBLE",None,"Commissioned Power (MW)",None), ("full_capacity_mw","DOUBLE",None,"Full Capacity (MW)",None),
    ("planned_plus_uc_mw","DOUBLE",None,"Planned + UC (MW)",None), ("facility_sqft","DOUBLE",None,"Facility Sq Ft",None),
    ("whitespace_sqft","DOUBLE",None,"Whitespace Sq Ft",None), ("actual_live_date","DATE",None,"Actual Live Date",None),
    ("facility_status","TEXT",32,"Facility Status","dm_facility_status"), ("cancelled","SHORT",None,"Cancelled",None),
    ("ingest_date","DATE",None,"Ingest Date",None)
]
campus_fields = [
    ("campus_id","TEXT",128,"Campus ID",None), ("campus_name","TEXT",128,"Campus/Project Name",None),
    ("company_clean","TEXT",128,"Company (Clean)",None), ("record_level","TEXT",16,"Record Level","dm_record_level"),
    ("address","TEXT",255,"Address",None), ("postal_code","TEXT",16,"Zip/Postal",None), ("city","TEXT",128,"City",None),
    ("market","TEXT",128,"Market",None), ("state","TEXT",64,"State",None), ("state_abbr","TEXT",8,"State Abbr",None),
    ("county","TEXT",128,"County",None), ("country","TEXT",64,"Country",None), ("region","TEXT",16,"Region",None),
    ("latitude","DOUBLE",None,"Latitude",None), ("longitude","DOUBLE",None,"Longitude",None),
    ("planned_power_mw","DOUBLE",None,"Planned Power (MW)",None), ("uc_power_mw","DOUBLE",None,"Under Construction Power (MW)",None),
    ("commissioned_power_mw","DOUBLE",None,"Commissioned Power (MW)",None), ("full_capacity_mw","DOUBLE",None,"Full Capacity (MW)",None),
    ("planned_plus_uc_mw","DOUBLE",None,"Planned + UC (MW)",None), ("facility_sqft_sum","DOUBLE",None,"Facility Sq Ft (SUM)",None),
    ("whitespace_sqft_sum","DOUBLE",None,"Whitespace Sq Ft (SUM)",None), ("building_count","SHORT",None,"# of Buildings",None),
    ("first_live_date","DATE",None,"First Live Date",None), ("facility_status_agg","TEXT",32,"Facility Status (Agg)","dm_facility_status"),
    ("cancelled","SHORT",None,"Cancelled",None)
]
create_fc("gold_buildings", buildings_fields)
create_fc("gold_campus", campus_fields)

# ------------------ (B) Ingest DCM into gold_buildings ------------------
# Add temp/raw fields for derivation
temp_fields = [
    ("stage_raw","TEXT",64,""), ("name_raw","TEXT",256,""), ("parent_id_raw","LONG",None,""),
    ("power_mw_raw","DOUBLE",None,""), ("year_operational_raw","LONG",None,""),
    ("address_details_raw","TEXT",255,""), ("company_name_raw","TEXT",128,""),
    ("link_company_raw","TEXT",255,""), ("link_profile_raw","TEXT",255,"")
]
existing = {f.name for f in arcpy.ListFields(b_fc)}
for nm,tp,ln,al in temp_fields:
    if nm not in existing:
        arcpy.management.AddField(b_fc, nm, tp, field_length=ln if ln else None)

# Build FieldMappings
def field_exists(fc, name):
    return any(f.name.lower() == name.lower() for f in arcpy.ListFields(fc))
def map_from_candidates(fms, target_fc, source_fc, target_name, candidates):
    fm = arcpy.FieldMap()
    used = None
    for cand in candidates:
        if field_exists(source_fc, cand):
            fm.addInputField(source_fc, cand)
            used = cand
            break
    if not used: return
    out_field = [f for f in arcpy.ListFields(target_fc) if f.name.lower() == target_name.lower()][0]
    of = arcpy.Field(); of.name = out_field.name; of.aliasName = out_field.aliasName; of.type = out_field.type
    fm.outputField = of
    fms.addFieldMap(fm)
fms = arcpy.FieldMappings(); fms.addTable(b_fc)
map_from_candidates(fms, b_fc, source_fc, "source_unique_id", ["id"])
map_from_candidates(fms, b_fc, source_fc, "date_reported", ["ds"])
for tgt, cands in [
    ("company_clean", ["company_name"]), ("address", ["address"]), ("postal_code", ["postal_code","postal"]),
    ("city", ["city"]), ("market", ["market","metro_admin"]), ("state", ["state"]), ("state_abbr", ["state_abbr"]),
    ("county", ["county"]), ("country", ["country"]), ("region", ["region"]), ("latitude", ["latitude"]),
    ("longitude", ["longitude"]), ("pue", ["pue"]), ("facility_sqft", ["building_sqft"]), ("whitespace_sqft", ["whitespace_sqft"]),
    ("tier_design", ["tier_design"])
]: map_from_candidates(fms, b_fc, source_fc, tgt, cands)
for tgt, cand in [
    ("stage_raw","stage"), ("name_raw","name"), ("parent_id_raw","parent_id"), ("power_mw_raw","power_mw"),
    ("year_operational_raw","year_operational"), ("address_details_raw","address_details"),
    ("company_name_raw","company_name"), ("link_company_raw","link_company"), ("link_profile_raw","link_profile")
]: map_from_candidates(fms, b_fc, source_fc, tgt, [cand])
arcpy.management.Append(inputs=[source_fc], target=b_fc, schema_type="NO_TEST", field_mapping=fms)
print("Appended DCM to gold_buildings.")

# Select new rows (ingest_date IS NULL)
layer_name = "bld_lyr"
arcpy.management.MakeFeatureLayer(b_fc, layer_name)
arcpy.management.SelectLayerByAttribute(layer_name, "NEW_SELECTION", "ingest_date IS NULL")
arcpy.management.CalculateField(layer_name, "unique_id", "'dcm_' + str(!source_unique_id!)", "PYTHON3")
arcpy.management.CalculateField(layer_name, "source", "'DataCenterMap'", "PYTHON3")
arcpy.management.CalculateField(layer_name, "ingest_date", "datetime.datetime.now()", "PYTHON3", "import datetime")
addr_cb = """
def merge_addr(a, b):
    a = (a or '').strip()
    b = (b or '').strip()
    return a if not b else (a + '; ' + b) if a else b
"""
arcpy.management.CalculateField(layer_name, "address", "merge_addr(!address!, !address_details_raw!)", "PYTHON3", addr_cb)
rl_cb = """
def record_level(n, pid):
    n = (n or '')
    return "Building" if (" - Building" in n or (pid and pid != 0)) else "Campus"
"""
arcpy.management.CalculateField(layer_name, "record_level", "record_level(!name_raw!, !parent_id_raw!)", "PYTHON3", rl_cb)
cn_cb = """
def campus_name(n):
    n = (n or '')
    if " - Building" in n: return n.split(" - Building")[0].strip()
    if n.endswith(" Campus"): return n[:-7].strip()
    if n.endswith(" Data Center"): return n[:-12].strip()
    return n.strip()
"""
arcpy.management.CalculateField(layer_name, "campus_name", "campus_name(!name_raw!)", "PYTHON3", cn_cb)
cid_cb = r"""
import re
def slug(t):
    t = (t or '').strip().lower().replace('&',' and ')
    t = re.sub(r'[^a-z0-9]+','-', t)
    t = re.sub(r'-+','-', t).strip('-')
    return t
def make_cid(co, ci, cp, lat, lon):
    co = slug(co); ci = slug(ci); cp = slug(cp)
    if not cp:
        try: cp = slug(f"{round(float(lat),4)},{round(float(lon),4)}")
        except: cp = "unknown"
    return f"{co}|{ci}|{cp}"
"""
arcpy.management.CalculateField(layer_name, "campus_id", "make_cid(!company_clean!, !city!, !campus_name!, !latitude!, !longitude!)", "PYTHON3", cid_cb)
fs_cb = """
def map_status(s):
    s = (s or '').strip().lower()
    if 'operational' in s: return 'Active'
    if 'under construction' in s: return 'Under Construction'
    if 'planned' in s: return 'Planned'
    if 'land banked' in s: return 'Land Acquisition'
    return 'Unknown'
"""
arcpy.management.CalculateField(layer_name, "facility_status", "map_status(!stage_raw!)", "PYTHON3", fs_cb)
arcpy.management.CalculateField(layer_name, "cancelled", "1 if (!stage_raw! and 'cancel' in !stage_raw!.lower()) else 0", "PYTHON3")
cap_cb = """
def route_commissioned(stage, mw):
    s = (stage or '').lower(); return mw if ('operational' in s) else None
def route_uc(stage, mw):
    s = (stage or '').lower(); return mw if ('under construction' in s) else None
def route_planned(stage, mw):
    s = (stage or '').lower(); return mw if ('planned' in s or 'land banked' in s) else None
"""
arcpy.management.CalculateField(layer_name, "commissioned_power_mw", "route_commissioned(!stage_raw!, !power_mw_raw!)", "PYTHON3", cap_cb)
arcpy.management.CalculateField(layer_name, "uc_power_mw", "route_uc(!stage_raw!, !power_mw_raw!)", "PYTHON3", cap_cb)
arcpy.management.CalculateField(layer_name, "planned_power_mw", "route_planned(!stage_raw!, !power_mw_raw!)", "PYTHON3", cap_cb)
arcpy.management.CalculateField(layer_name, "planned_plus_uc_mw", "( !planned_power_mw! or 0 ) + ( !uc_power_mw! or 0 )", "PYTHON3")
ald_cb = """
import datetime
def live_date(y):
    try:
        yi = int(y)
        if yi > 0: return datetime.datetime(yi, 12, 31)
    except: pass
    return None
"""
arcpy.management.CalculateField(layer_name, "actual_live_date", "live_date(!year_operational_raw!)", "PYTHON3", ald_cb)
arcpy.management.SelectLayerByAttribute(layer_name, "CLEAR_SELECTION")
arcpy.management.Delete(layer_name)
for nm, *_ in temp_fields:
    try: arcpy.management.DeleteField(b_fc, nm)
    except Exception as e: print(f"Could not delete temp field {nm}: {e}")
print("gold_buildings is ready.")

# ------------------ (C) Campus Rollup: Pairwise Dissolve + Feature To Point ------------------
# Add status_rank_tmp for campus status aggregation
rank_field = "status_rank_tmp"
if rank_field not in [f.name for f in arcpy.ListFields(b_fc)]:
    arcpy.management.AddField(b_fc, rank_field, "SHORT")
rank_cb = """
def rank(s):
    s = (s or '').lower()
    if s == 'active': return 1
    if s == 'under construction': return 2
    if s == 'permitting': return 3
    if s == 'announced': return 4
    if s == 'land acquisition': return 5
    if s == 'rumor': return 6
    return 9
"""
arcpy.management.CalculateField(b_fc, rank_field, "rank(!facility_status!)", "PYTHON3", rank_cb)

# Pairwise Dissolve
diss_fc = fr"{gdb}\_campus_dissolve"
if arcpy.Exists(diss_fc): arcpy.management.Delete(diss_fc)
stats = ";".join([
    "company_clean FIRST","campus_name FIRST","city FIRST","market FIRST","state FIRST","state_abbr FIRST",
    "county FIRST","country FIRST","region FIRST","postal_code FIRST",
    "planned_power_mw SUM","uc_power_mw SUM","commissioned_power_mw SUM","full_capacity_mw SUM",
    "facility_sqft SUM","whitespace_sqft SUM","actual_live_date MIN",f"{rank_field} MIN",
    "cancelled MAX","unique_id COUNT"
])
arcpy.analysis.PairwiseDissolve(
    in_features=b_fc, out_feature_class=diss_fc, dissolve_field="campus_id",
    statistics_fields=stats, multi_part="MULTI_PART", concatenation_separator=""
)
print("Pairwise dissolve complete.")

# Feature To Point
campus_pts = fr"{gdb}\_campus_pts"
if arcpy.Exists(campus_pts): arcpy.management.Delete(campus_pts)
arcpy.management.FeatureToPoint(diss_fc, campus_pts, "INSIDE")
print("Feature To Point complete.")

# Add target fields to campus_pts
target_text = ["company_clean","campus_name","city","market","state","state_abbr","county","country","region","postal_code"]
target_nums = ["planned_power_mw","uc_power_mw","commissioned_power_mw","full_capacity_mw","planned_plus_uc_mw",
               "facility_sqft_sum","whitespace_sqft_sum","building_count","cancelled","latitude","longitude"]
target_dates = ["first_live_date"]
target_status = ["facility_status_agg"]
existing = {f.name for f in arcpy.ListFields(campus_pts)}
for nm in target_text:
    if nm not in existing: arcpy.management.AddField(campus_pts, nm, "TEXT", field_length=128)
for nm in target_nums:
    if nm not