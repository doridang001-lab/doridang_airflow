@echo off
REM Watch latest hall weekly report, copy it to Desktop, and open it in Excel.
REM Run this in an interactive Windows user session so Excel is visible.

cd /d "%~dp0\.."
".venv\Scripts\python.exe" "scripts\watch_hall_report.py"
