$ErrorActionPreference = "Stop"

# Start the ChatKit FastAPI backend on Windows.

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Resolve-Path (Join-Path $ScriptDir "..")
Set-Location $ProjectRoot

function Get-PythonCommand {
  $candidates = @()
  $pythonCmd = Get-Command python -ErrorAction SilentlyContinue
  if ($pythonCmd) {
    $candidates += $pythonCmd.Source
  }
  if (Get-Command py -ErrorAction SilentlyContinue) {
    $candidates += "py"
  }
  $candidates += (Join-Path $env:LocalAppData "Programs\Python\Python311\python.exe")
  $candidates += (Join-Path $env:LocalAppData "Programs\Python\Python312\python.exe")
  $candidates += "python"

  foreach ($candidate in $candidates | Select-Object -Unique) {
    if ($candidate -like "*.exe" -and -not (Test-Path $candidate)) {
      continue
    }
    try {
      & $candidate --version *> $null
      if ($LASTEXITCODE -eq 0) {
        return $candidate
      }
    } catch {
      continue
    }
  }
  throw "Python was not found on PATH. Install Python 3.11+ and restart your terminal."
}

function Import-EnvFileIfNeeded {
  param (
    [string]$Path
  )

  if ($env:OPENAI_API_KEY) {
    return
  }
  if (-not (Test-Path $Path)) {
    return
  }

  Write-Host "Sourcing OPENAI_API_KEY from $Path"
  Get-Content $Path | ForEach-Object {
    $line = $_.Trim()
    if (-not $line -or $line.StartsWith("#")) {
      return
    }
    $parts = $line.Split("=", 2)
    if ($parts.Length -eq 2) {
      $key = $parts[0].Trim()
      $value = $parts[1].Trim().Trim("'`"")
      if ($key) {
        [Environment]::SetEnvironmentVariable($key, $value, "Process")
      }
    }
  }
}

$pythonCmd = Get-PythonCommand

if (-not (Test-Path ".venv")) {
  Write-Host "Creating virtual env in $ProjectRoot\.venv ..."
  & $pythonCmd -m venv .venv
}

$activatePath = Join-Path $ProjectRoot ".venv\Scripts\Activate.ps1"
if (-not (Test-Path $activatePath)) {
  throw "Virtual environment activation script not found at $activatePath"
}
. $activatePath

Write-Host "Installing backend deps (editable) ..."
pip install -e . | Out-Null

$envFile = Resolve-Path (Join-Path $ProjectRoot "..\.env.local") -ErrorAction SilentlyContinue
if ($envFile) {
  Import-EnvFileIfNeeded -Path $envFile.Path
}

if (-not $env:OPENAI_API_KEY) {
  throw "Set OPENAI_API_KEY in your environment or in .env.local before running this script."
}

Write-Host "Starting ChatKit backend on http://127.0.0.1:8000 ..."
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
