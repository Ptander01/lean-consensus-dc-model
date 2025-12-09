# Copy Scripts to fbsource
# This script copies your organized Scripts folder to fbsource repository

param(
    [string]$FbsourcePath = "C:\fbsource\fbcode\idc\lsim\dc_consensus_scripts",
    [switch]$DryRun
)

$SourcePath = "c:\Users\ptanderson\Documents\ArcGIS\Projects\Lean Consensus DC Model\scripts"

Write-Host "=" * 70 -ForegroundColor Cyan
Write-Host "Copying Scripts to fbsource" -ForegroundColor Cyan
Write-Host "=" * 70 -ForegroundColor Cyan
Write-Host ""

# Check if fbsource path exists
if (-not (Test-Path (Split-Path $FbsourcePath -Parent))) {
    Write-Host "ERROR: fbsource path does not exist!" -ForegroundColor Red
    Write-Host "Please ensure fbsource is cloned at: $(Split-Path $FbsourcePath -Parent)" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "To clone fbsource, run:" -ForegroundColor Yellow
    Write-Host "  cd C:\" -ForegroundColor White
    Write-Host "  sl clone fbsource" -ForegroundColor White
    exit 1
}

# Create target directory if it doesn't exist
if (-not (Test-Path $FbsourcePath)) {
    Write-Host "Creating directory: $FbsourcePath" -ForegroundColor Yellow
    if (-not $DryRun) {
        New-Item -Path $FbsourcePath -ItemType Directory -Force | Out-Null
    }
}

Write-Host "Source: $SourcePath" -ForegroundColor Green
Write-Host "Target: $FbsourcePath" -ForegroundColor Green
Write-Host ""

# Define folders to copy
$folders = @("ingestion", "processing", "qa", "utils", "analysis", "consensus")

# Files to copy from root
$rootFiles = @("ETL Script.py", "test_scripts.py")

$totalFiles = 0
$totalFolders = 0

# Copy folders
foreach ($folder in $folders) {
    $sourceFolderPath = Join-Path $SourcePath $folder
    $targetFolderPath = Join-Path $FbsourcePath $folder
    
    if (Test-Path $sourceFolderPath) {
        Write-Host "Copying folder: $folder/" -ForegroundColor Cyan
        
        $pyFiles = Get-ChildItem -Path $sourceFolderPath -Filter "*.py" -File
        
        foreach ($file in $pyFiles) {
            $targetFile = Join-Path $targetFolderPath $file.Name
            
            Write-Host "  + $folder/$($file.Name)" -ForegroundColor Gray
            
            if (-not $DryRun) {
                # Create target folder if needed
                if (-not (Test-Path $targetFolderPath)) {
                    New-Item -Path $targetFolderPath -ItemType Directory -Force | Out-Null
                }
                
                # Copy file
                Copy-Item -Path $file.FullName -Destination $targetFile -Force
            }
            
            $totalFiles++
        }
        
        $totalFolders++
    }
}

# Copy root files
Write-Host "`nCopying root files:" -ForegroundColor Cyan
foreach ($fileName in $rootFiles) {
    $sourceFile = Join-Path $SourcePath $fileName
    $targetFile = Join-Path $FbsourcePath $fileName
    
    if (Test-Path $sourceFile) {
        Write-Host "  + $fileName" -ForegroundColor Gray
        
        if (-not $DryRun) {
            Copy-Item -Path $sourceFile -Destination $targetFile -Force
        }
        
        $totalFiles++
    }
}

# Create a README
$readmeContent = @"
# Data Center Consensus Analysis Scripts

This repository contains Python scripts for analyzing data center consensus data from multiple external sources.

## Project Structure

``````
dc_consensus_scripts/
├── ingestion/          # Data source ingestion scripts (7 files)
├── processing/         # Data processing & transformation (4 files)
├── qa/                 # Quality assurance scripts (5 files)
├── utils/              # Helper functions & utilities (3 files)
├── analysis/           # Analysis & reporting (7 files)
├── consensus/          # Consensus building logic (2 files)
├── ETL Script.py       # Main ETL orchestrator
└── test_scripts.py     # Testing utility (no ArcGIS execution)
``````

## Statistics

- **Total Lines of Code**: 9,727
- **Total Files**: 29 Python files
- **Primary Language**: Python 3.x with ArcGIS Pro (arcpy)

## Usage

These scripts are designed to run in ArcGIS Pro Python environment. They analyze data center location data from multiple vendors (DataCenterHawk, DataCenterMap, Semianalysis, etc.) against Meta's canonical data center locations.

## Key Components

- **Ingestion**: Import data from various external sources
- **Processing**: Transform and rollup data to campus level
- **QA**: Validate data quality and fix inconsistencies
- **Analysis**: Spatial accuracy analysis and capacity validation
- **Consensus**: Build consensus from multiple sources via spatial clustering

## Dependencies

- ArcGIS Pro 3.x
- Python 3.9+
- pandas
- numpy

## Author

Meta Data Center GIS Team

## Last Updated

$(Get-Date -Format "yyyy-MM-dd")
"@

$readmePath = Join-Path $FbsourcePath "README.md"
Write-Host "`nCreating README.md" -ForegroundColor Cyan

if (-not $DryRun) {
    Set-Content -Path $readmePath -Value $readmeContent -Encoding UTF8
}

# Summary
Write-Host ""
Write-Host "=" * 70 -ForegroundColor Cyan
Write-Host "COPY COMPLETE" -ForegroundColor Green
Write-Host "=" * 70 -ForegroundColor Cyan
Write-Host ""
Write-Host "Copied:" -ForegroundColor Yellow
Write-Host "  Folders: $totalFolders" -ForegroundColor White
Write-Host "  Files: $totalFiles" -ForegroundColor White
Write-Host ""
Write-Host "Target directory: $FbsourcePath" -ForegroundColor Green
Write-Host ""

if ($DryRun) {
    Write-Host "DRY RUN - No files were actually copied" -ForegroundColor Yellow
    Write-Host "Run without -DryRun to perform the actual copy" -ForegroundColor Yellow
} else {
    Write-Host "Next steps:" -ForegroundColor Cyan
    Write-Host "  1. cd $FbsourcePath" -ForegroundColor White
    Write-Host "  2. sl status                    # Check what files were added" -ForegroundColor White
    Write-Host "  3. sl add .                     # Add all new files" -ForegroundColor White
    Write-Host "  4. sl commit -m 'Add DC consensus analysis scripts'" -ForegroundColor White
    Write-Host "  5. sl submit                    # Create diff for review" -ForegroundColor White
}

Write-Host ""
Write-Host "=" * 70 -ForegroundColor Cyan
