"""
Script Validator - Test scripts without running ArcGIS operations
Usage:
    exec(open(r"c:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\scripts\test_scripts.py", encoding='utf-8').read())

Then run:
    test_script('ingestion/ingest_dch.py')
    test_all_scripts()
"""

import sys
import os
from pathlib import Path

# Add scripts folder to path
SCRIPT_DIR = r'c:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\scripts'
sys.path.insert(0, SCRIPT_DIR)

def test_script(script_path):
    """
    Test a script by checking syntax and imports without executing main logic.

    Args:
        script_path: Relative path from scripts folder (e.g., 'ingestion/ingest_dch.py')
    """
    full_path = os.path.join(SCRIPT_DIR, script_path)

    if not os.path.exists(full_path):
        print(f"❌ File not found: {script_path}")
        return False

    print(f"\n{'='*70}")
    print(f"Testing: {script_path}")
    print('='*70)

    # Test 1: Syntax Check
    print("\n[1/3] Checking Python syntax...")
    try:
        with open(full_path, 'r', encoding='utf-8') as f:
            code = f.read()
        compile(code, full_path, 'exec')
        print("   ✓ Syntax is valid")
    except SyntaxError as e:
        print(f"   ✗ Syntax error at line {e.lineno}: {e.msg}")
        return False

    # Test 2: Import Check
    print("\n[2/3] Checking imports...")
    try:
        # Check for import statements
        imports = [line.strip() for line in code.split('\n')
                  if line.strip().startswith(('import ', 'from '))]

        if imports:
            print(f"   Found {len(imports)} import statements:")
            for imp in imports[:5]:  # Show first 5
                print(f"      {imp}")
            if len(imports) > 5:
                print(f"      ... and {len(imports) - 5} more")

        # Try importing common dependencies
        try:
            import arcpy
            print("   ✓ arcpy available")
        except ImportError:
            print("   ⚠ arcpy not available (expected in standalone Python)")

        try:
            import pandas
            print("   ✓ pandas available")
        except ImportError:
            print("   ⚠ pandas not available")

    except Exception as e:
        print(f"   ⚠ Import check warning: {e}")

    # Test 3: Code Structure
    print("\n[3/3] Analyzing code structure...")
    try:
        lines = code.split('\n')
        total_lines = len(lines)
        code_lines = len([l for l in lines if l.strip() and not l.strip().startswith('#')])
        comment_lines = len([l for l in lines if l.strip().startswith('#')])
        blank_lines = len([l for l in lines if not l.strip()])

        # Find functions
        functions = [l.strip() for l in lines if l.strip().startswith('def ')]

        print(f"   Total lines: {total_lines}")
        print(f"   Code lines: {code_lines}")
        print(f"   Comment lines: {comment_lines}")
        print(f"   Blank lines: {blank_lines}")
        print(f"   Functions defined: {len(functions)}")

        if functions:
            print(f"\n   Functions found:")
            for func in functions[:5]:  # Show first 5
                func_name = func.split('(')[0].replace('def ', '')
                print(f"      • {func_name}()")
            if len(functions) > 5:
                print(f"      ... and {len(functions) - 5} more")

    except Exception as e:
        print(f"   ⚠ Structure analysis warning: {e}")

    print(f"\n{'='*70}")
    print(f"✅ Test complete: {script_path}")
    print('='*70)
    return True

def test_folder(folder_name):
    """
    Test all Python files in a folder.

    Args:
        folder_name: Folder name (e.g., 'ingestion', 'qa', 'analysis')
    """
    folder_path = os.path.join(SCRIPT_DIR, folder_name)

    if not os.path.exists(folder_path):
        print(f"❌ Folder not found: {folder_name}")
        return

    print(f"\n{'#'*70}")
    print(f"# Testing all scripts in: {folder_name}/")
    print('#'*70)

    py_files = [f for f in os.listdir(folder_path) if f.endswith('.py')]

    if not py_files:
        print(f"No Python files found in {folder_name}/")
        return

    results = []
    for py_file in sorted(py_files):
        script_path = os.path.join(folder_name, py_file)
        success = test_script(script_path)
        results.append((py_file, success))

    # Summary
    print(f"\n{'#'*70}")
    print(f"# Summary for {folder_name}/")
    print('#'*70)
    passed = sum(1 for _, s in results if s)
    total = len(results)

    for filename, success in results:
        status = "✓" if success else "✗"
        print(f"   {status} {filename}")

    print(f"\nPassed: {passed}/{total}")
    print('#'*70)

def test_all_scripts():
    """Test all scripts in all folders."""
    folders = ['ingestion', 'processing', 'qa', 'utils', 'analysis', 'consensus']

    print("\n" + "="*70)
    print("TESTING ALL SCRIPTS IN REPOSITORY")
    print("="*70)

    all_results = {}

    for folder in folders:
        folder_path = os.path.join(SCRIPT_DIR, folder)
        if os.path.exists(folder_path):
            py_files = [f for f in os.listdir(folder_path) if f.endswith('.py')]
            all_results[folder] = len(py_files)

    print(f"\nFound {sum(all_results.values())} Python files across {len(all_results)} folders")
    print("\nTesting each folder...\n")

    for folder in folders:
        if folder in all_results:
            test_folder(folder)

    print("\n" + "="*70)
    print("ALL TESTS COMPLETE")
    print("="*70)

def quick_check(script_path):
    """Quick syntax-only check (fastest)."""
    full_path = os.path.join(SCRIPT_DIR, script_path)

    try:
        with open(full_path, 'r', encoding='utf-8') as f:
            compile(f.read(), full_path, 'exec')
        print(f"✓ {script_path} - syntax valid")
        return True
    except SyntaxError as e:
        print(f"✗ {script_path} - syntax error at line {e.lineno}: {e.msg}")
        return False
    except FileNotFoundError:
        print(f"✗ {script_path} - file not found")
        return False

# Show help on load
print("="*70)
print("SCRIPT VALIDATOR LOADED")
print("="*70)
print("\nAvailable commands:")
print("  test_script('ingestion/ingest_dch.py')  # Test a single script")
print("  test_folder('ingestion')                # Test all scripts in folder")
print("  test_all_scripts()                      # Test everything")
print("  quick_check('qa/qa_validation.py')     # Quick syntax check only")
print("\nExamples:")
print("  test_script('analysis/multi_source_spatial_accuracy.py')")
print("  test_folder('consensus')")
print("="*70 + "\n")
