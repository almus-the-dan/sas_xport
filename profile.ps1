#
# Profile the sas_xport reader and writer with samply, then report the
# top functions from this crate by inclusive/self-time percentage.
#
# Usage:
#   .\profile.ps1 [-Records N] [-Top N]
#   .\profile.ps1                        # 1M records, top 10 functions
#   .\profile.ps1 -Records 500000        # 500K records, top 10 functions
#   .\profile.ps1 -Top 20               # 1M records, top 20 functions
#
# Requirements:
#   cargo install --locked samply

param(
    [int]$Records = 1000000,
    [int]$Top = 10
)

$ErrorActionPreference = "Stop"

try {
    $ProfileBin = "target/profiling/examples/profile.exe"
    $ReportBin  = "target/profiling/examples/profile_report.exe"

    # --- Preflight ---

    if (-not (Get-Command samply -ErrorAction SilentlyContinue)) {
        Write-Error "Error: 'samply' not found. Install with: cargo install --locked samply"
        exit 1
    }

    Write-Host "Building profiling binaries..."
    cargo build --example profile --example profile_report --profile profiling -p sas_xport --all-features --quiet
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    # --- Profile each phase ---

    function Run-Phase {
        param([string]$Phase)

        $JsonFile = "target/profile_${Phase}.json.gz"

        Write-Host ""
        Write-Host "Recording $Phase phase ($Records records)..."
        samply record --save-only --unstable-presymbolicate -o $JsonFile `
            -- "./$ProfileBin" --phase $Phase --records $Records 2>$null
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "samply record failed for $Phase phase (exit code $LASTEXITCODE)"
            return
        }

        Write-Host ""
        Write-Host "=== $($Phase.ToUpper()) ==="
        & "./$ReportBin" --input $JsonFile --top $Top

        $SymsFile = $JsonFile -replace '\.gz$', '.syms.json'
        Remove-Item -Force $JsonFile, $SymsFile -ErrorAction SilentlyContinue
    }

    Run-Phase "sync-write"
    Run-Phase "sync-read"
    Run-Phase "async-write"
    Run-Phase "async-read"

    Write-Host ""
    Write-Host "Done."
}
finally {
    Pop-Location
}