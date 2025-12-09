# Granularity-aware spatial accuracy stats (enhanced + robust field detection)
# - Auto-detects field names in accuracy_analysis_multi_source
# - Computes building- and campus-level stats with:
#   recall, min/max, mean, std, MAD, percentiles (P10..P90), threshold shares, match-quality bands
# - Breakdowns by region_derived and new_build_status
# - Worst-case list (top 25 largest distances per source)
# - Includes Synergy in accuracy analysis (still excluded later from consensus KPIs)
#
# Console messages include emojis for quick scanning.

import arcpy
import pandas as pd
import numpy as np
from collections import Counter
from datetime import datetime
import os

print("="*80)
print("Granularity-aware spatial accuracy stats — Enhanced")
print("="*80)
print(f"Started: {datetime.now():%Y-%m-%d %H:%M:%S}\n")

# CONFIG
gdb = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\Default.gdb"
matches_fc = os.path.join(gdb, "accuracy_analysis_multi_source")
canonical_fc = os.path.join(gdb, "meta_canonical_v2")
outdir = r"C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\analysis_outputs"
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
os.makedirs(outdir, exist_ok=True)

# Helper: auto-detect a field by candidate names (case-insensitive)
def detect_field(fc, candidates):
    names = [f.name for f in arcpy.ListFields(fc)]
    lname_map = {n.lower(): n for n in names}
    for cand in candidates:
        if cand.lower() in lname_map:
            return lname_map[cand.lower()]
    # try partial contains match
    for cand in candidates:
        for n in names:
            if cand.lower() in n.lower():
                return n
    return None

# Discover field names in matches table
src_field = detect_field(matches_fc, ["source_name","source","vendor","ext_source","origin_source"])
dist_field = detect_field(matches_fc, ["distance_m","haversine_m","join_distance_m","distance","dist_m"])
building_key_field_matches = detect_field(matches_fc, ["canonical_datacenter","datacenter","building","canonical_building"])
campus_key_field_matches = detect_field(matches_fc, ["canonical_location_key","location_key","campus_key","campus"])

print("🔎 Field detection (accuracy_analysis_multi_source):")
print(f"- Source field: {src_field}")
print(f"- Distance field: {dist_field}")
print(f"- Building key field (matches): {building_key_field_matches}")
print(f"- Campus key field (matches): {campus_key_field_matches}\n")

missing = []
if not src_field: missing.append("source_name")
if not dist_field: missing.append("distance_m")
if not building_key_field_matches: missing.append("canonical_datacenter")
if not campus_key_field_matches: missing.append("canonical_location_key")
if missing:
    print("❌ Missing expected fields in matches_fc:", ", ".join(missing))
    print("Tip: Open the attribute table and confirm the actual column names. The script tries auto-detection by exact/partial matches.")
    raise RuntimeError("Required fields not found in accuracy_analysis_multi_source")

# Discover canonical fields
building_key_field_canon = detect_field(canonical_fc, ["datacenter","building"])
campus_key_field_canon = detect_field(canonical_fc, ["location_key","campus"])
region_field_canon = detect_field(canonical_fc, ["region_derived","region"])
status_field_canon = detect_field(canonical_fc, ["new_build_status","build_status"])
shape_field = "SHAPE@XY"

print("🔎 Field detection (meta_canonical_v2):")
print(f"- Building key: {building_key_field_canon}")
print(f"- Campus key: {campus_key_field_canon}")
print(f"- Region: {region_field_canon}")
print(f"- Build status: {status_field_canon}\n")

missing_canon = []
if not building_key_field_canon: missing_canon.append("datacenter")
if not campus_key_field_canon: missing_canon.append("location_key")
if not region_field_canon: missing_canon.append("region_derived")
if not status_field_canon: missing_canon.append("new_build_status")
if missing_canon:
    print("⚠️ Canonical is missing expected fields:", ", ".join(missing_canon))
    print("Proceeding with available fields; some breakdowns may be skipped.")

# Load matches to DataFrame
fields_to_read = [src_field, dist_field, building_key_field_matches, campus_key_field_matches]
rows = []
with arcpy.da.SearchCursor(matches_fc, fields_to_read) as cur:
    for row in cur:
        rows.append(row)
df = pd.DataFrame(rows, columns=["source_name","distance_m","building_key","campus_key"])

# Filter to valid distances within 5 km
df = df[(df["distance_m"].notna()) & (df["distance_m"] >= 0) & (df["distance_m"] <= 5000)]

# Build canonical maps for denominators and breakdowns
buildings_with_xy = set()
building_region_map = {}
building_status_map_counts = {}

with arcpy.da.SearchCursor(canonical_fc, [building_key_field_canon, region_field_canon, status_field_canon, shape_field]) as cur:
    for bkey, region, status, (x, y) in cur:
        if bkey and x not in (None, 0) and y not in (None, 0):
            buildings_with_xy.add(bkey)
            if region_field_canon:
                building_region_map.setdefault(bkey, []).append(region)
            if status_field_canon:
                building_status_map_counts.setdefault(bkey, []).append(status)

# collapse to mode (most common) for building-level region/status
def mode_or_none(values):
    if not values:
        return None
    c = Counter([v for v in values if v is not None])
    return c.most_common(1)[0][0] if c else None

building_region_mode = {k: mode_or_none(v) for k, v in building_region_map.items()}
building_status_mode = {k: mode_or_none(v) for k, v in building_status_map_counts.items()}

campuses_with_xy = set()
campus_region_map = {}
campus_status_map = {}

with arcpy.da.SearchCursor(canonical_fc, [campus_key_field_canon, region_field_canon, status_field_canon, shape_field]) as cur:
    for ckey, region, status, (x, y) in cur:
        if ckey and x not in (None, 0) and y not in (None, 0):
            campuses_with_xy.add(ckey)
            if region_field_canon:
                campus_region_map[ckey] = region
            if status_field_canon:
                campus_status_map[ckey] = status

building_denominator = len(buildings_with_xy)
campus_denominator = len(campuses_with_xy)
print(f"📏 Denominators resolved:")
print(f"- Unique canonical buildings with valid coords: {building_denominator}")
print(f"- Unique canonical campuses with valid coords: {campus_denominator}\n")

# Utility: summarize stats with MAD, percentiles, thresholds, match-quality bands
def summarize(nearest_df, entity_col, denominator):
    # Recall
    recall_series = nearest_df.groupby("source_name")[entity_col].nunique().rename("entities_detected")
    recall = (recall_series / denominator).rename("recall")

    # Distance stats
    dists = nearest_df.groupby("source_name")["distance_m"]
    med = dists.median()
    mad = dists.apply(lambda s: np.median(np.abs(s - np.median(s))))  # median absolute deviation

    stats = pd.DataFrame({
        "count_entities": dists.count(),
        "min_m": dists.min(),
        "p10_m": dists.quantile(0.10),
        "p25_m": dists.quantile(0.25),
        "median_m": med,
        "p75_m": dists.quantile(0.75),
        "p90_m": dists.quantile(0.90),
        "max_m": dists.max(),
        "mean_m": dists.mean(),
        "std_m": dists.std(),
        "mad_m": mad,
        "within_100m": dists.apply(lambda s: (s <= 100).mean()),
        "within_500m": dists.apply(lambda s: (s <= 500).mean()),
        "within_1km": dists.apply(lambda s: (s <= 1000).mean()),
        "within_5km": dists.apply(lambda s: (s <= 5000).mean()),
        # Match-quality bands
        "share_excellent_lt1km": dists.apply(lambda s: (s < 1000).mean()),
        "share_good_1to3km": dists.apply(lambda s: ((s >= 1000) & (s <= 3000)).mean()),
        "share_fair_3to5km": dists.apply(lambda s: ((s > 3000) & (s <= 5000)).mean()),
    })

    out = stats.join(recall, how="left").reset_index()
    cols = ["source_name","recall","count_entities","min_m","p10_m","p25_m","median_m","p75_m","p90_m","max_m",
            "mean_m","std_m","mad_m","within_100m","within_500m","within_1km","within_5km",
            "share_excellent_lt1km","share_good_1to3km","share_fair_3to5km"]
    return out[cols].sort_values(["recall","median_m"], ascending=[False, True])

# Compute nearest per entity per source (ONE_TO_MANY reduced to nearest)
def nearest_by(df, entity_col):
    g = df.groupby(["source_name", entity_col])["distance_m"].min().reset_index()
    g = g[g[entity_col].notna()]
    return g

# Building-level
nearest_building = nearest_by(df, "building_key")
building_stats = summarize(nearest_building, "building_key", building_denominator)
building_csv = os.path.join(outdir, f"spatial_accuracy_building_{ts}.csv")
building_stats.to_csv(building_csv, index=False)

# Campus-level
nearest_campus = nearest_by(df, "campus_key")
campus_stats = summarize(nearest_campus, "campus_key", campus_denominator)
campus_csv = os.path.join(outdir, f"spatial_accuracy_campus_{ts}.csv")
campus_stats.to_csv(campus_csv, index=False)

# Region breakdowns
def add_region(nearest_df, entity_col, region_map):
    nearest_df = nearest_df.copy()
    nearest_df["region_derived"] = nearest_df[entity_col].map(region_map)
    return nearest_df.dropna(subset=["region_derived"])

building_with_region = add_region(nearest_building, "building_key", building_region_mode)
campus_with_region = add_region(nearest_campus, "campus_key", campus_region_map)

def summarize_by_group(nearest_df, entity_col, denominator_by_group, group_col):
    rows = []
    for (src, grp), sub in nearest_df.groupby(["source_name", group_col]):
        # denominator for the group (unique entities present in canonical for that group)
        denom = denominator_by_group.get(grp, None)
        if denom is None or denom == 0:
            continue
        ent_count = sub[entity_col].nunique()
        recall = ent_count / denom
        s = sub["distance_m"]
        row = {
            "source_name": src,
            group_col: grp,
            "recall": recall,
            "count_entities": ent_count,
            "min_m": s.min(),
            "p10_m": s.quantile(0.10),
            "p25_m": s.quantile(0.25),
            "median_m": s.median(),
            "p75_m": s.quantile(0.75),
            "p90_m": s.quantile(0.90),
            "max_m": s.max(),
            "mean_m": s.mean(),
            "std_m": s.std(),
            "mad_m": np.median(np.abs(s - np.median(s))),
            "within_100m": (s <= 100).mean(),
            "within_500m": (s <= 500).mean(),
            "within_1km": (s <= 1000).mean(),
            "within_5km": (s <= 5000).mean(),
            "share_excellent_lt1km": (s < 1000).mean(),
            "share_good_1to3km": ((s >= 1000) & (s <= 3000)).mean(),
            "share_fair_3to5km": ((s > 3000) & (s <= 5000)).mean(),
        }
        rows.append(row)
    return pd.DataFrame(rows)

# Build denominators per region for buildings
building_region_denoms = {}
for b, r in building_region_mode.items():
    if r is not None:
        building_region_denoms[r] = building_region_denoms.get(r, 0) + 1

# Campuses per region denominators
campus_region_denoms = {}
for c, r in campus_region_map.items():
    if r is not None:
        campus_region_denoms[r] = campus_region_denoms.get(r, 0) + 1

building_by_region = summarize_by_group(building_with_region, "building_key", building_region_denoms, "region_derived")
campus_by_region = summarize_by_group(campus_with_region, "campus_key", campus_region_denoms, "region_derived")

building_by_region_csv = os.path.join(outdir, f"spatial_accuracy_building_by_region_{ts}.csv")
campus_by_region_csv = os.path.join(outdir, f"spatial_accuracy_campus_by_region_{ts}.csv")
building_by_region.to_csv(building_by_region_csv, index=False)
campus_by_region.to_csv(campus_by_region_csv, index=False)

# Status breakdowns (mode for building; direct for campus)
def add_status(nearest_df, entity_col, status_map):
    nearest_df = nearest_df.copy()
    nearest_df["new_build_status"] = nearest_df[entity_col].map(status_map)
    return nearest_df.dropna(subset=["new_build_status"])

building_with_status = add_status(nearest_building, "building_key", building_status_mode)
campus_with_status = add_status(nearest_campus, "campus_key", campus_status_map)

# Build denominators per status for buildings
building_status_denoms = {}
for b, s in building_status_mode.items():
    if s is not None:
        building_status_denoms[s] = building_status_denoms.get(s, 0) + 1

# Campuses per status denominators
campus_status_denoms = {}
for c, s in campus_status_map.items():
    if s is not None:
        campus_status_denoms[s] = campus_status_denoms.get(s, 0) + 1

building_by_status = summarize_by_group(building_with_status, "building_key", building_status_denoms, "new_build_status")
campus_by_status = summarize_by_group(campus_with_status, "campus_key", campus_status_denoms, "new_build_status")

building_by_status_csv = os.path.join(outdir, f"spatial_accuracy_building_by_status_{ts}.csv")
campus_by_status_csv = os.path.join(outdir, f"spatial_accuracy_campus_by_status_{ts}.csv")
building_by_status.to_csv(building_by_status_csv, index=False)
campus_by_status.to_csv(campus_by_status_csv, index=False)

# Worst-case lists (top 25 largest distances per source), both levels
def worst_case(nearest_df, entity_col):
    # For each source, take top 25 largest distances
    frames = []
    for src, sub in nearest_df.groupby("source_name"):
        frames.append(sub.sort_values("distance_m", ascending=False).head(25).assign(level=entity_col, source_name=src))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

worst_building = worst_case(nearest_building, "building_key")
worst_campus = worst_case(nearest_campus, "campus_key")

worst_building_csv = os.path.join(outdir, f"worst_case_building_{ts}.csv")
worst_campus_csv = os.path.join(outdir, f"worst_case_campus_{ts}.csv")
worst_building.to_csv(worst_building_csv, index=False)
worst_campus.to_csv(worst_campus_csv, index=False)

# TXT summary
txt = os.path.join(outdir, f"spatial_accuracy_summary_{ts}.txt")
with open(txt, "w", encoding="utf-8") as f:
    f.write("Granularity-aware spatial accuracy summary (enhanced)\n")
    f.write(f"Started: {datetime.now():%Y-%m-%d %H:%M:%S}\n\n")
    f.write(f"Denominators:\n- Buildings: {building_denominator}\n- Campuses: {campus_denominator}\n\n")
    f.write("Resolved matches fields:\n")
    f.write(f"- source_name={src_field}, distance_m={dist_field}, building_key={building_key_field_matches}, campus_key={campus_key_field_matches}\n\n")
    f.write("Campus-level stats by source:\n")
    f.write(campus_stats.to_string(index=False))
    f.write("\n\nBuilding-level stats by source:\n")
    f.write(building_stats.to_string(index=False))
    f.write("\n\nNotes:\n- Recall = unique entities matched within 5 km / denominator.\n")
    f.write("- Distances computed on nearest match per entity per source (reduces ONE_TO_MANY to nearest).\n")
    f.write("- Match-quality bands: Excellent <1 km, Good 1–3 km, Fair 3–5 km.\n")
    f.write("- Per-region and per-status breakdowns written to separate CSVs.\n")
    f.write("- Worst-case lists show top 25 farthest matches per source for targeted QA.\n")
    f.write("- Synergy is included in all analyses here; still excluded later from consensus KPIs.\n")

print("✅ COMPLETE")
print(f"- Wrote: {building_csv}")
print(f"- Wrote: {campus_csv}")
print(f"- Wrote: {building_by_region_csv}")
print(f"- Wrote: {campus_by_region_csv}")
print(f"- Wrote: {building_by_status_csv}")
print(f"- Wrote: {campus_by_status_csv}")
print(f"- Wrote: {worst_building_csv}")
print(f"- Wrote: {worst_campus_csv}")
print(f"- Wrote: {txt}")
print("="*80)
