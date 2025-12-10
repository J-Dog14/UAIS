@echo off
REM Daily automated run of mobility assessment processing
REM This script should be scheduled to run daily via Windows Task Scheduler

cd /d "%~dp0\..\.."
call venv\Scripts\activate.bat

REM Log start time
echo %date% %time% - Mobility processing started >> python\mobility\daily_run.log

python python\mobility\main.py

REM Log completion time and exit code
if %ERRORLEVEL% EQU 0 (
    echo %date% %time% - Mobility processing completed successfully >> python\mobility\daily_run.log
) else (
    echo %date% %time% - Mobility processing FAILED with error code %ERRORLEVEL% >> python\mobility\daily_run.log
)
