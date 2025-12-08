@echo off
REM Daily automated run of Proteus processing
REM This script should be scheduled to run daily via Windows Task Scheduler

cd /d "%~dp0\..\.."
call venv\Scripts\activate.bat

REM Set environment variables (if not already set)
if not defined PROTEUS_EMAIL set PROTEUS_EMAIL=jimmy@8ctanebaseball.com
if not defined PROTEUS_PASSWORD set PROTEUS_PASSWORD=DerekCarr4
if not defined PROTEUS_LOCATION set PROTEUS_LOCATION=byoungphysicaltherapy

python python\proteus\main.py

REM Log the run
echo %date% %time% - Proteus processing completed >> python\proteus\daily_run.log
