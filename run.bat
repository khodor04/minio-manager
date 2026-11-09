@echo off
title MinIO Manager
cd /d "%~dp0"
call venv\Scripts\activate.bat
echo.
echo  MinIO Manager starting...
echo  Open http://127.0.0.1:8765 in your browser
echo  Press Ctrl+C to stop
echo.
start "" http://127.0.0.1:8765
uvicorn app:app --host 127.0.0.1 --port 8765
pause
