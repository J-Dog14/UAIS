@echo off
REM Daily automated run of mobility assessment processing
REM This script should be scheduled to run daily via Windows Task Scheduler

cd /d "%~dp0\..\.."
call venv\Scripts\activate.bat
python python\mobility\main.py

REM Log the run
echo %date% %time% - Mobility processing completed >> python\mobility\daily_run.log
