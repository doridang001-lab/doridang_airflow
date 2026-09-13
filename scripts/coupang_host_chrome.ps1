<#
쿠팡 수집용 "진짜 크롬" 띄우기 (호스트, 1회 준비)

Docker/Xvfb 크롬은 Akamai 봇탐지에 걸려 로그인이 거부됩니다("권한이 존재하지 않습니다").
수동 크롬은 통과하므로, 자동화가 이 실제 크롬에 attach 하도록 디버그 포트로 띄웁니다.

사용법:
  1) 이 스크립트 실행:  powershell -ExecutionPolicy Bypass -File C:\airflow\scripts\coupang_host_chrome.ps1
  2) doridang 계정으로 로그인된 기본 Chrome 프로필(Default)을 사용
  3) 창은 최소화해도 됨 — 수집은 백그라운드로 진행, 다른 작업 가능
  4) 닫지 말 것 (닫으면 수집 시 재로그인 필요)

Docker Airflow에서는 Chrome DevTools Host 헤더 제한 때문에 Docker Desktop 호스트 IP(예: 192.168.65.254:9222)로 이 크롬에 attach합니다.
doridang 계정(doridang001@gmail.com)이 로그인된 기본 Chrome 프로필(Default)만 사용합니다.
#>

param(
    [switch]$CheckOnly
)

try {
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
    $OutputEncoding = [System.Text.Encoding]::UTF8
} catch {
    Write-Warning "콘솔 UTF-8 설정을 건너뜁니다: $($_.Exception.Message)"
}

$chrome = "C:\Program Files\Google\Chrome\Application\chrome.exe"
if (-not (Test-Path $chrome)) {
    $chrome = "C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
}
if (-not (Test-Path $chrome)) {
    Write-Error "chrome.exe 를 찾을 수 없습니다. 경로를 직접 지정하세요."
    exit 1
}

$userDataDir = Join-Path $env:LOCALAPPDATA "Google\Chrome\User Data"
$profileDirectory = "Default"
$port = 9222
$defaultExtensionId = "ocpdgnoaajajnlehamcalfcpholjhfbe"
$fallbackExtDir = "C:\airflow\coupang_extension_build"
$requiredExtFiles = @("manifest.json", "runner.html", "runner.js")
$doridangCompanyName = [string]::Concat([char[]]@(0xC8FC, 0xC2DD, 0xD68C, 0xC0AC, 0x20, 0xB3C4, 0xB9AC, 0xB2F9))
$doridangName = [string]::Concat([char[]]@(0xB3C4, 0xB9AC, 0xB2F9))
$collectFolderName = [string]::Concat([char[]]@(0xC601, 0xC5C5, 0xAD00, 0xB9AC, 0xBD80, 0x5F, 0xC218, 0xC9D1))

function Join-ChildPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Base,
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Children
    )

    $parts = @($Base) + $Children
    return [System.IO.Path]::Combine([string[]]$parts)
}

function ConvertTo-NativeArgumentList {
    param([string[]]$Arguments)

    return (($Arguments | ForEach-Object {
        $arg = [string]$_
        if ($arg -match '[\s"]') {
            '"' + ($arg -replace '"', '\"') + '"'
        } else {
            $arg
        }
    }) -join ' ')
}

function Test-CoupangExtensionDir {
    param([string]$Path)

    if (-not $Path -or -not (Test-Path $Path)) {
        return $false
    }
    foreach ($file in $requiredExtFiles) {
        if (-not (Test-Path (Join-Path $Path $file))) {
            return $false
        }
    }
    return $true
}

if (-not (Test-Path (Join-Path $userDataDir $profileDirectory))) {
    Write-Error "doridang Chrome 프로필을 찾을 수 없습니다: $userDataDir\$profileDirectory"
    exit 1
}

function Resolve-CoupangExtensionDir {
    $profileDir = Join-Path $userDataDir $profileDirectory
    $preferenceFiles = @(
        (Join-Path $profileDir "Secure Preferences"),
        (Join-Path $profileDir "Preferences")
    )

    foreach ($prefPath in $preferenceFiles) {
        if (-not (Test-Path $prefPath)) {
            continue
        }
        try {
            $prefs = Get-Content -Path $prefPath -Encoding utf8 -Raw | ConvertFrom-Json
            $settings = $prefs.extensions.settings
            if ($settings -and ($settings.PSObject.Properties.Name -contains $defaultExtensionId)) {
                $path = $settings.$defaultExtensionId.path
                if (Test-CoupangExtensionDir -Path $path) {
                    Write-Host "등록된 쿠팡 확장 경로 사용: $path" -ForegroundColor Cyan
                    return $path
                }
                Write-Warning "등록된 쿠팡 확장 경로가 유효하지 않습니다: $path"
            }
        } catch {
            Write-Warning "Chrome 확장 설정 확인을 건너뜁니다: $prefPath | $($_.Exception.Message)"
        }
    }

    if (Test-CoupangExtensionDir -Path $fallbackExtDir) {
        Write-Host "저장소 쿠팡 확장 경로 사용: $fallbackExtDir" -ForegroundColor Cyan
        return $fallbackExtDir
    }

    Write-Error "쿠팡 확장 경로를 찾을 수 없습니다: Chrome 등록 확장 또는 $fallbackExtDir"
    exit 1
}

$extDir = Resolve-CoupangExtensionDir

$localStatePath = Join-Path $userDataDir "Local State"
if (Test-Path $localStatePath) {
    try {
        $localState = Get-Content -Path $localStatePath -Encoding utf8 -Raw | ConvertFrom-Json
        $profileInfo = $localState.profile.info_cache.$profileDirectory
        if ($profileInfo.user_name -and $profileInfo.user_name -ne "doridang001@gmail.com") {
            Write-Error "Default Chrome 프로필이 doridang 계정이 아닙니다: $($profileInfo.user_name)"
            exit 1
        }
    } catch {
        Write-Warning "Chrome Local State 프로필 확인을 건너뜁니다: $($_.Exception.Message)"
    }
}

function Wait-DevToolsEndpoint {
    param(
        [int]$Port,
        [int]$TimeoutSeconds = 30
    )

    $endpoint = "http://127.0.0.1:$Port/json"
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -Uri $endpoint -TimeoutSec 3 -UseBasicParsing
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300) {
                Write-Host "DevTools endpoint ready: $endpoint" -ForegroundColor Green
                return
            }
        } catch {
            Start-Sleep -Seconds 1
        }
    }

    Write-Error "DevTools endpoint가 ${TimeoutSeconds}초 안에 준비되지 않았습니다: $endpoint"
    exit 1
}

function Test-DevToolsHasBlockedRunnerTab {
    param([int]$Port)

    try {
        $response = Invoke-WebRequest -Uri "http://127.0.0.1:$Port/json" -TimeoutSec 3 -UseBasicParsing
        $tabs = @($response.Content | ConvertFrom-Json)
    } catch {
        return $false
    }

    foreach ($tab in $tabs) {
        $url = [string]$tab.url
        $title = [string]$tab.title
        if ($url -like "chrome-extension://*/runner.html*" -and $title -eq $url) {
            return $true
        }
    }
    return $false
}

function Ensure-ChromeDownloadPref {
    param(
        [string]$UserDataDir,
        [string]$ProfileDirectory,
        [string]$TargetDir
    )

    $prefPath = Join-Path $UserDataDir "$ProfileDirectory\Preferences"
    if (-not (Test-Path $prefPath)) {
        throw "Preferences 파일 없음, 다운로드 경로를 확인할 수 없습니다: $prefPath"
    }

    try {
        $prefs = Get-Content -Path $prefPath -Raw -Encoding utf8 | ConvertFrom-Json
    } catch {
        throw "Preferences 파싱 실패, 다운로드 경로를 확인할 수 없습니다: $($_.Exception.Message)"
    }

    $current = $null
    if ($prefs.PSObject.Properties.Name -contains "download") {
        $current = $prefs.download.default_directory
    }
    if ($current -eq $TargetDir) {
        Write-Host "다운로드 경로 pref 정상: $current" -ForegroundColor Green
        return
    }

    $running = @(Get-Process chrome -ErrorAction SilentlyContinue)
    if ($running.Count -gt 0) {
        throw "다운로드 경로가 '$current' 로 잘못되어 있으나 Chrome이 실행 중이라 교정할 수 없습니다. Chrome을 완전히 종료한 뒤 다시 실행하세요."
    }

    if ($prefs.PSObject.Properties.Name -notcontains "download") {
        $prefs | Add-Member -NotePropertyName "download" -NotePropertyValue ([pscustomobject]@{}) -Force
    }
    $prefs.download | Add-Member -NotePropertyName "default_directory" -NotePropertyValue $TargetDir -Force
    $prefs.download | Add-Member -NotePropertyName "prompt_for_download" -NotePropertyValue $false -Force

    if ($prefs.PSObject.Properties.Name -notcontains "savefile") {
        $prefs | Add-Member -NotePropertyName "savefile" -NotePropertyValue ([pscustomobject]@{}) -Force
    }
    $prefs.savefile | Add-Member -NotePropertyName "default_directory" -NotePropertyValue $TargetDir -Force

    $prefs | ConvertTo-Json -Depth 100 -Compress | Out-File -FilePath $prefPath -Encoding utf8 -NoNewline
    Write-Host "다운로드 경로 pref 교정: '$current' -> '$TargetDir'" -ForegroundColor Yellow
}

function Resolve-CoupangCollectDownloadDir {
    if ($env:COLLECT_DB) {
        $candidate = Join-ChildPath $env:COLLECT_DB $collectFolderName
        if (Test-Path -Path $candidate) {
            return $candidate
        }
    }

    $userProfile = [Environment]::GetFolderPath("UserProfile")
    $onedriveCandidates = @(
        (Join-ChildPath $userProfile ("OneDrive - " + $doridangCompanyName)),
        (Join-ChildPath $userProfile ("OneDrive - " + $doridangName))
    )
    foreach ($base in $onedriveCandidates) {
        $candidate = Join-ChildPath $base "Collect_Data" $collectFolderName
        if (Test-Path -Path $candidate) {
            return $candidate
        }
    }

    throw "쿠팡 다운로드 대상 폴더를 찾을 수 없습니다: Collect_Data\영업관리부_수집"
}

# 다운로드 경로: 쿠팡 원본 CSV의 정식 수집 폴더와 일치시킨다.
$collectDownloadDir = Resolve-CoupangCollectDownloadDir
Write-Host "다운로드 경로: $collectDownloadDir" -ForegroundColor Cyan

if ($CheckOnly) {
    Write-Host "CheckOnly: Chrome 실행 없이 다운로드 대상 폴더 확인 완료" -ForegroundColor Green
    exit 0
}

New-Item -ItemType Directory -Path $collectDownloadDir -Force | Out-Null
Ensure-ChromeDownloadPref -UserDataDir $userDataDir -ProfileDirectory $profileDirectory -TargetDir $collectDownloadDir

# 이미 같은 포트로 떠 있으면 중복 실행 방지
$inUse = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
if ($inUse) {
    Write-Host "이미 디버그 포트 $port 로 크롬이 떠 있습니다. 새로 띄우지 않습니다." -ForegroundColor Yellow
    Write-Warning "이미 떠 있는 Chrome에는 실행 플래그(--download-default-directory 포함)가 적용되지 않습니다."
    Wait-DevToolsEndpoint -Port $port -TimeoutSeconds 10
    if (Test-DevToolsHasBlockedRunnerTab -Port $port) {
        Write-Error "기존 Chrome의 runner.html 탭이 차단 페이지로 보입니다. Chrome을 완전히 종료한 뒤 다시 실행하세요."
        exit 1
    }
    exit 0
}

$runningChrome = @(Get-Process chrome -ErrorAction SilentlyContinue)
if ($runningChrome.Count -gt 0) {
    Write-Error "Chrome이 이미 실행 중이지만 디버그 포트 $port 가 열려 있지 않습니다. Chrome을 완전히 종료한 뒤 다시 실행하세요."
    exit 1
}

$chromeArgs = @(
    "--remote-debugging-port=$port",
    "--remote-debugging-address=0.0.0.0",
    "--remote-allow-origins=*",
    "--user-data-dir=$userDataDir",
    "--profile-directory=$profileDirectory",
    "--no-first-run",
    "--no-default-browser-check",
    "--download-default-directory=$collectDownloadDir"
)
$chromeArgs += "--load-extension=$extDir"
Write-Host "확장 로드: $extDir" -ForegroundColor Cyan
$chromeArgs += "about:blank"

Write-Host "doridang Chrome 실행 (debug port $port, profile $profileDirectory, user-data-dir $userDataDir)" -ForegroundColor Green
Start-Process -FilePath $chrome -WindowStyle Normal -ArgumentList (ConvertTo-NativeArgumentList $chromeArgs)
Wait-DevToolsEndpoint -Port $port -TimeoutSeconds 30
Write-Host "완료. 확장이 실제 ID의 runner.html을 열고 topHalfBtn 자동 클릭 후 수집이 시작됩니다." -ForegroundColor Green
