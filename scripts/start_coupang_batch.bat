@echo off
setlocal

set "CHROME_64=C:\Program Files\Google\Chrome\Application\chrome.exe"
set "CHROME_32=C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
set "URL=chrome-extension://ocpdgnoaajajnlehamcalfcpholjhfbe/runner.html?auto=1"

if exist "%CHROME_64%" (
    start "" "%CHROME_64%" "%URL%"
) else if exist "%CHROME_32%" (
    start "" "%CHROME_32%" "%URL%"
) else (
    echo [ERROR] Chrome 설치 경로를 찾을 수 없습니다.
    exit /b 1
)

endlocal
