@echo off
title NEXHUS STARTUP

echo ====================================
echo Activando entorno virtual...
echo ====================================
call venv\Scripts\activate.bat

echo.
echo ====================================
echo Iniciando Redis...
echo ====================================
start "Redis" cmd /k "cd /d C:\Users\LOPEZHEJ\Downloads\Redis-x64-5.0.14.1 && redis-server.exe"

timeout /t 3 >nul

echo.
echo ====================================
echo Iniciando Celery...
echo ====================================
start "Celery Worker" cmd /k "call venv\Scripts\activate.bat && celery -A config worker -l info -P solo --concurrency=1"

timeout /t 3 >nul

echo.
echo ====================================
echo Iniciando Daphne...
echo ====================================
start "Daphne Server" cmd /k "call venv\Scripts\activate.bat && daphne -b 0.0.0.0 -p 8000 config.asgi:application"

echo.
echo ====================================
echo NEXHUS iniciado correctamente
echo ====================================

pause