@echo on
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul

REM CONFIG
set "TOKEN=0be1d15273a396ac285ee0f7b9a625c8DAEAAD9FCFE96680781DDE57F45E774BF4B57C2D"
set "RESOURCE=400531375"
set "OBJECT=400586661"
set "MYSQL_HOST=127.0.0.1"
set "MYSQL_USER=root"
set "MYSQL_PASS=12345"
set "MYSQL_DB=telemetria_db"

REM TEMPLATES (espacos separando)
set "TEMPLATES=31 34 41 40 33 36 35 38 39 43"

cd /d "%~dp0"
set "LOGFILE=wialon_log.txt"
echo ==== START (Previous Month) %DATE% %TIME% ====>> "%LOGFILE%"

REM gerar arquivo com dias (uma linha por dia)
set "DAYFILE=%~dp0_days_list.tmp"
if exist "%DAYFILE%" del /f /q "%DAYFILE%" >nul 2>&1

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$start=(Get-Date).AddMonths(-1); $start = Get-Date -Year $start.Year -Month $start.Month -Day 1; $end = $start.AddMonths(1).AddDays(-1); for ($d=0; $d -lt $end.Day; $d++){ $start.AddDays($d).ToString('yyyy-MM-dd') }" > "%DAYFILE%"

if not exist "%DAYFILE%" (
  echo Failed to generate day list. >> "%LOGFILE%"
  exit /b 1
)

echo Day list written to "%DAYFILE%". >> "%LOGFILE%"

REM ---- Outer: day ----
for /f "usebackq delims=" %%D in ("%DAYFILE%") do (
  set "CUR_DAY=%%D"
  set "FROM=!CUR_DAY! 00:00:00"
  set "TO=!CUR_DAY! 23:59:59"

  echo ------------------------------------------>> "%LOGFILE%"
  echo START DAY !CUR_DAY! %DATE% %TIME%>> "%LOGFILE%"

  REM inner: iterate templates for this day
  for %%T in (%TEMPLATES%) do (
    call :RUN_TEMPLATE %%T !CUR_DAY!
  )

  echo END DAY !CUR_DAY! %DATE% %TIME%>> "%LOGFILE%"
)

del /f /q "%DAYFILE%" >nul 2>&1

echo ==== FINISH (Previous Month) %DATE% %TIME% ====>> "%LOGFILE%"
exit /b 0

:RUN_TEMPLATE
rem args: %1 = tpl, %2 = daystr
set "TPL=%~1"
set "DAYSTR=%~2"

echo ------------------------------------------>> "%LOGFILE%"
echo Running Template %TPL% for %DAYSTR% (%DATE% %TIME%) >> "%LOGFILE%"
echo Running Template %TPL% for day %DAYSTR%...

py wialon_report_sql.py ^
  --token "%TOKEN%" ^
  --resource-id %RESOURCE% ^
  --template-id %TPL% ^
  --object-id %OBJECT% ^
  --from "%FROM%" --to "%TO%" ^
  --format xlsx --output Relatorio_Wialon_%TPL%_%DAYSTR% ^
  --mysql-host "%MYSQL_HOST%" --mysql-user "%MYSQL_USER%" --mysql-pass "%MYSQL_PASS%" --mysql-db "%MYSQL_DB%" ^
  --timeout 3600 --http-timeout 1200 --verbose ^
  >> "%LOGFILE%" 2>&1

set "RC=%ERRORLEVEL%"
echo ExitCode (Template %TPL%, %DAYSTR%): %RC%>> "%LOGFILE%"

if "%RC%"=="0" goto :RC_OK_%RANDOM%
goto :RC_FAIL_%RANDOM%

:RC_OK_%RANDOM%
echo Template %TPL% (%DAYSTR%) succeeded >> "%LOGFILE%"
goto :RC_END_%RANDOM%

:RC_FAIL_%RANDOM%
echo Template %TPL% (%DAYSTR%) FAILED with code %RC% >> "%LOGFILE%"
goto :RC_END_%RANDOM%

:RC_END_%RANDOM%
exit /b
