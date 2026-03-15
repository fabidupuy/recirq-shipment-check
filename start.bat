@echo off
echo ============================================
echo   RecirQ Global - Shipment Check Server
echo ============================================
echo.

REM Check if Python is available
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python is not installed or not in PATH.
    echo Please install Python from https://python.org
    pause
    exit /b 1
)

REM Install dependencies
echo Installing dependencies...
pip install -r requirements.txt --quiet

echo.
echo Starting server...
echo Open your browser to: http://localhost:5000
echo.
python app.py
pause
