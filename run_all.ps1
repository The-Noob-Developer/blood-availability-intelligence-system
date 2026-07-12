$ErrorActionPreference = "Stop"

$repoRoot = $PSScriptRoot
$pythonExe = Join-Path $repoRoot "venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $pythonExe)) {
    $pythonExe = "python"
}

$services = @(
    @{
        Title  = "Donation API"
        Script = "`$Host.UI.RawUI.WindowTitle = 'Donation API'; Set-Location -LiteralPath '$repoRoot'; & '$pythonExe' -m uvicorn streaming.donations.donations_api:app --reload --port 8000"
    },
    @{
        Title  = "Donation Consumer"
        Script = "`$Host.UI.RawUI.WindowTitle = 'Donation Consumer'; Set-Location -LiteralPath '$repoRoot'; & '$pythonExe' -m streaming.donations.donations_consumer"
    },
    @{
        Title  = "Requests API"
        Script = "`$Host.UI.RawUI.WindowTitle = 'Requests API'; Set-Location -LiteralPath '$repoRoot'; & '$pythonExe' -m uvicorn streaming.requests.requests_api:app --reload --port 8002"
    },
    @{
        Title  = "Requests Consumer"
        Script = "`$Host.UI.RawUI.WindowTitle = 'Requests Consumer'; Set-Location -LiteralPath '$repoRoot'; & '$pythonExe' -m streaming.requests.requests_consumer"
    },
    @{
        Title  = "Allocation API"
        Script = "`$Host.UI.RawUI.WindowTitle = 'Allocation API'; Set-Location -LiteralPath '$repoRoot'; & '$pythonExe' -m uvicorn streaming.allocation.allocation_api:app --reload --port 8001"
    },
    @{
        Title  = "Allocation Service"
        Script = "`$Host.UI.RawUI.WindowTitle = 'Allocation Service'; Set-Location -LiteralPath '$repoRoot'; & '$pythonExe' -m streaming.allocation.allocation_service"
    },
    @{
        Title  = "Frontend"
        Script = "`$Host.UI.RawUI.WindowTitle = 'Frontend'; Set-Location -LiteralPath '$repoRoot'; & '$pythonExe' -m streamlit run frontend/app.py"
    }
)

$wtCommand = Get-Command wt.exe -ErrorAction SilentlyContinue

if ($null -ne $wtCommand) {
    $arguments = @("-w", "0")

    foreach ($service in $services) {
        if ($arguments.Count -gt 2) {
            $arguments += ";"
        }

        $arguments += @(
            "new-tab",
            "--title", $service.Title,
            "powershell.exe",
            "-NoExit",
            "-ExecutionPolicy", "Bypass",
            "-Command", $service.Script
        )
    }

    # Add tabs to the current Windows Terminal window
    & $wtCommand.Source @arguments
    return
}

# Fallback if Windows Terminal is not available
foreach ($service in $services) {
    Start-Process powershell.exe -WorkingDirectory $repoRoot -ArgumentList @(
        "-NoExit",
        "-ExecutionPolicy", "Bypass",
        "-Command", $service.Script
    )
}