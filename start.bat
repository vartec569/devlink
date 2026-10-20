@echo off
setlocal
cd /d %~dp0
title devlink

where node >nul 2>nul
if errorlevel 1 (
  echo Node.js not found. Install Node.js LTS from https://nodejs.org and run this again.
  pause
  exit /b 1
)

if not exist node_modules (
  echo Installing dependencies [one time]...
  call npm install --production
  if errorlevel 1 (
    echo npm install failed. Check your connection and run start.bat again.
    pause
    exit /b 1
  )
)

if not exist .dl-secret (
  echo First run: the server will generate .dl-secret [the API secret].
  echo Your controller needs that secret once to connect — it stays on this PC.
)

echo Starting devlink...
node server.js
pause
