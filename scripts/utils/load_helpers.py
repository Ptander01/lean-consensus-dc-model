"""
Helper Functions for DC Consensus Model
Run this manually when you want to use shortcut commands.
"""

import arcpy
import os
from datetime import datetime
from collections import Counter

# ====== PROJECT PATHS ======
PROJECT_DIR = r'C:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model'
SCRIPT_DIR = os.path.join(PROJECT_DIR, 'scripts')
GDB = os.path.join(PROJECT_DIR, 'Default.gdb')

# ====== SHORTCUT FUNCTIONS ======

def run_script(name, subfolder=None):
    """Run a script by name from the scripts folder or subfolder.

    Args:
        name: Script filename (e.g., 'ingest_dch.py')
        subfolder: Optional subfolder name (e.g., 'ingestion', 'qa', 'processing')
    """
    if subfolder:
        path = os.path.join(SCRIPT_DIR, subfolder, name)
    else:
        path = os.path.join(SCRIPT_DIR, name)

    if not os.path.exists(path):
        print(f"Error: Script not found at {path}")
        return

    print(f"\nRunning: {name}")
    print("=" * 70)
    exec(open(path, encoding='utf-8').read(), globals())

def run_pipeline():
    """Run full pipeline: ingest all sources, rollup, validate."""
    print("\n" + "=" * 70)
    print("RUNNING FULL PIPELINE")
    print("=" * 70)

    start_time = datetime.now()

    # Ingest all sources
    run_script('ingest_dch.py', 'ingestion')
    print("\n")
    run_script('ingest_dcm.py', 'ingestion')
    print("\n")
    run_script('ingest_synergy.py', 'ingestion')
    print("\n")
    run_script('ingest_woodmac.py', 'ingestion')
    print("\n")

    # Rollup to campus
    run_script('campus_rollup_new.py', 'processing')
    print("\n")

    # Validate
    run_script('qa_validation.py', 'qa')

    elapsed = datetime.now() - start_time
    print("\n" + "=" * 70)
    print(f"PIPELINE COMPLETE - Elapsed time: {elapsed}")
    print("=" * 70)

def ingest_all():
    """Run all ingestion scripts."""
    print("\n" + "=" * 70)
    print("INGESTING ALL SOURCES")
    print("=" * 70)

    run_script('ingest_dch.py', 'ingestion')
    print("\n")
    run_script('ingest_dcm.py', 'ingestion')
    print("\n")
    run_script('ingest_synergy.py', 'ingestion')
    print("\n")
    run_script('ingest_woodmac.py', 'ingestion')

def ingest_dch():
    """Ingest DataCenterHawk only."""
    run_script('ingest_dch.py', 'ingestion')

def ingest_dcm():
    """Ingest DataCenterMap only."""
    run_script('ingest_dcm.py', 'ingestion')

def ingest_synergy():
    """Ingest Synergy only."""
    run_script('ingest_synergy.py', 'ingestion')

def ingest_woodmac():
    """Ingest WoodMac only."""
    run_script('ingest_woodmac.py', 'ingestion')

def rollup_only():
    """Just run campus rollup."""
    run_script('campus_rollup_new.py', 'processing')

def validate_only():
    """Just run validation."""
    run_script('qa_validation.py', 'qa')

def quick_qa():
    """Quick quality check - record counts by source."""
    buildings_fc = os.path.join(GDB, 'gold_buildings')
    campus_fc = os.path.join(GDB, 'gold_campus')

    print("\n" + "=" * 70)
    print("QUICK QA - RECORD COUNTS")
    print("=" * 70)

    # Buildings by source
    with arcpy.da.SearchCursor(buildings_fc, ['source']) as cursor:
        sources = Counter([row[0] for row in cursor])

    total_buildings = sum(sources.values())

    print("\n📊 Buildings by Source:")
    print("-" * 40)
    for source in sorted(sources.keys()):
        count = sources[source]
        pct = (count / total_buildings) * 100
        print(f"  {source:20s}: {count:4d} ({pct:5.1f}%)")
    print("-" * 40)
    print(f"  {'TOTAL':20s}: {total_buildings:4d}")

    # Campus count
    campus_count = int(arcpy.management.GetCount(campus_fc)[0])
    print(f"\n📍 Campuses: {campus_count:,}")

    # Unique campus_ids
    unique_campus_ids = set()
    with arcpy.da.SearchCursor(buildings_fc, ['campus_id']) as cursor:
        for row in cursor:
            unique_campus_ids.add(row[0])

    print(f"   Unique campus_ids: {len(unique_campus_ids)}")

    print("=" * 70 + "\n")

def show_sources():
    """Show current source distribution."""
    quick_qa()

def show_commands():
    """Display available commands."""
    print("\n" + "=" * 70)
    print("AVAILABLE COMMANDS")
    print("=" * 70)

    print("\n🔄 Full Workflows:")
    print("  run_pipeline()      # Full ETL: ingest all → rollup → validate")
    print("  ingest_all()        # Ingest all sources")

    print("\n📥 Individual Ingestions:")
    print("  ingest_dch()        # DataCenterHawk only")
    print("  ingest_dcm()        # DataCenterMap only")
    print("  ingest_synergy()    # Synergy only")
    print("  ingest_woodmac()    # WoodMac only")

    print("\n🔧 Individual Steps:")
    print("  rollup_only()       # Campus rollup only")
    print("  validate_only()     # Validation only")

    print("\n📊 Quick Checks:")
    print("  quick_qa()          # Record counts by source")
    print("  show_sources()      # Same as quick_qa()")
    print("  show_commands()     # This help message")

    print("\n📂 Paths:")
    print(f"  Project: {PROJECT_DIR}")
    print(f"  Scripts: {SCRIPT_DIR}")
    print(f"  GDB: {GDB}")

    print("\n💡 Custom Script:")
    print("  run_script('your_script.py', 'subfolder')  # Optional subfolder")

    print("=" * 70 + "\n")

# Show message on load
print("\n" + "=" * 70)
print("HELPER FUNCTIONS LOADED")
print("=" * 70)
print("\nType show_commands() to see all available commands")
print("\nQuick start:")
print("  quick_qa()          # Check current state")
print("  run_pipeline()      # Run full workflow")
print("=" * 70 + "\n")
