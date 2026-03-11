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

REM TEMPLATES (espaços separando)
set "TEMPLATES=31 34 41 40 33 36 35 38 39 43"

cd /d "%~dp0"
set "LOGFILE=wialon_log.txt"
echo( ==== START (Selected Month) %DATE% %TIME% ====>> "%LOGFILE%"

REM --- Get month, year and optional single-day/keep args ---
set "INPUT_MONTH=%~1"
set "INPUT_YEAR=%~2"
set "INPUT_THIRD=%~3"

if "%INPUT_MONTH%"=="" (
  set /p "INPUT_MONTH=Digite o mês (1-12): "
)
if "%INPUT_YEAR%"=="" (
  set /p "INPUT_YEAR=Digite o ano (ex: 2025): "
)

REM Single day / keep parsing
set "SINGLE_DAY="
set "KEEP_FOLDER=0"
if not "%INPUT_THIRD%"=="" (
  if /i "%INPUT_THIRD%"=="keep" (
    set "KEEP_FOLDER=1"
  ) else (
    echo %INPUT_THIRD%|findstr /r "^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$" >nul
    if not errorlevel 1 (
      set "SINGLE_DAY=%INPUT_THIRD%"
    ) else (
      set "INPUT_FOURTH=%~4"
      if /i "%INPUT_FOURTH%"=="keep" set "KEEP_FOLDER=1"
    )
  )
)

REM Trim spaces
for /f "tokens=* delims= " %%A in ("%INPUT_MONTH%") do set "INPUT_MONTH=%%~A"
for /f "tokens=* delims= " %%A in ("%INPUT_YEAR%") do set "INPUT_YEAR=%%~A"

REM Validate numeric and range for month
echo %INPUT_MONTH%|findstr /r "^[0-9][0-9]*$" >nul
if errorlevel 1 (
  cmd /c "echo Mes invalido: "%INPUT_MONTH%". Abortando.>>"%LOGFILE%""
  cmd /c "echo Mes invalido: "%INPUT_MONTH%"."
  exit /b 1
)
echo %INPUT_YEAR%|findstr /r "^[0-9][0-9]*$" >nul
if errorlevel 1 (
  cmd /c "echo Ano invalido: "%INPUT_YEAR%". Abortando.>>"%LOGFILE%""
  cmd /c "echo Ano invalido: "%INPUT_YEAR%"."
  exit /b 1
)

set /a "MNUM=INPUT_MONTH+0" 2>nul
if %MNUM% LSS 1 (
  cmd /c "echo Mes invalido: "%INPUT_MONTH%". Abortando.>>"%LOGFILE%""
  cmd /c "echo Mes invalido: "%INPUT_MONTH%"."
  exit /b 1
)
if %MNUM% GTR 12 (
  cmd /c "echo Mes invalido: "%INPUT_MONTH%". Abortando.>>"%LOGFILE%""
  cmd /c "echo Mes invalido: "%INPUT_MONTH%"."
  exit /b 1
)

rem pad month to 2 digits
if %MNUM% LSS 10 ( set "MM=0%MNUM%" ) else ( set "MM=%MNUM%" )
set "YYYY=%INPUT_YEAR%"

cmd /c "echo Selected month=%MM% year=%YYYY%>>"%LOGFILE%""
cmd /c "echo Selected month=%MM% year=%YYYY%"

REM gerar arquivo com dias do mês escolhido (uma linha por dia) OR single day
set "DAYFILE=%~dp0_days_list.tmp"
if exist "%DAYFILE%" del /f /q "%DAYFILE%" >nul 2>&1

if not "%SINGLE_DAY%"=="" (
  echo %SINGLE_DAY%>"%DAYFILE%"
) else (
  powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "$start = Get-Date -Year %YYYY% -Month %MM% -Day 1; $end = $start.AddMonths(1).AddDays(-1); for ($d=0; $d -lt $end.Day; $d++) { $start.AddDays($d).ToString('yyyy-MM-dd') }" > "%DAYFILE%"
)

if not exist "%DAYFILE%" (
  cmd /c "echo Failed to generate day list.>>"%LOGFILE%""
  cmd /c "echo Failed to generate day list."
  exit /b 1
)

cmd /c "echo Day list written to "%DAYFILE%". >>"%LOGFILE%""
cmd /c "echo Day list written to "%DAYFILE%"."

REM ---- Outer: day ----
for /f "usebackq delims=" %%D in ("%DAYFILE%") do (
  set "CUR_DAY=%%D"
  set "FROM=!CUR_DAY! 00:00:00"
  set "TO=!CUR_DAY! 23:59:59"

  cmd /c "echo ------------------------------------------>>"%LOGFILE%""
  cmd /c "echo START DAY !CUR_DAY! %DATE% %TIME%>>"%LOGFILE%""
  cmd /c "echo START DAY !CUR_DAY! %DATE% %TIME%"

  REM create per-day folder
  set "DAY_FOLDER=reports_!CUR_DAY!"
  if not exist "!DAY_FOLDER!" mkdir "!DAY_FOLDER!"

  REM inner: iterate templates for this day (save outputs into day folder)
  for %%T in (%TEMPLATES%) do (
    REM Important: call returns here and must NOT exit script
    call :RUN_TEMPLATE %%T "%%D" "!FROM!" "!TO!" "!DAY_FOLDER!"
    REM after return, we log and continue to next template
    cmd /c "echo Finished template %%T for !CUR_DAY!>>"%LOGFILE%""
  )

  REM after all templates for the day finish, compress the folder into zip
  cmd /c "echo Compressing folder "!DAY_FOLDER!" ...>>"%LOGFILE%""
  powershell -NoProfile -Command ^
    "if (Test-Path -Path '%cd%\!DAY_FOLDER!') { Remove-Item -ErrorAction SilentlyContinue '%cd%\!DAY_FOLDER!.zip' ; Compress-Archive -Path '%cd%\!DAY_FOLDER!\*' -DestinationPath '%cd%\!DAY_FOLDER!.zip' }" >> "%LOGFILE%" 2>&1

  REM optionally remove folder if KEEP_FOLDER not set
  if "%KEEP_FOLDER%"=="0" (
    rmdir /s /q "!DAY_FOLDER!" >nul 2>&1
    if exist "!DAY_FOLDER!" (
      cmd /c "echo Failed to remove folder !DAY_FOLDER!>>"%LOGFILE%""
    ) else (
      cmd /c "echo Removed folder !DAY_FOLDER!>>"%LOGFILE%""
    )
  ) else (
    cmd /c "echo Keeping folder !DAY_FOLDER! as requested>>"%LOGFILE%""
  )

  cmd /c "echo END DAY !CUR_DAY! %DATE% %TIME%>>"%LOGFILE%""
  cmd /c "echo END DAY !CUR_DAY! %DATE% %TIME%"
)

del /f /q "%DAYFILE%" >nul 2>&1

cmd /c "echo ==== FINISH (Selected Month) %DATE% %TIME% ====>>"%LOGFILE%""
cmd /c "echo ==== FINISH (Selected Month) %DATE% %TIME%"
exit /b 0

:RUN_TEMPLATE
rem args: %1 = tpl, %2 = daystr, %3 = from, %4 = to, %5 = day_folder
set "TPL=%~1"
set "DAYSTR=%~2"
set "FROM_ARG=%~3"
set "TO_ARG=%~4"
set "DAY_FOLDER=%~5"

set "OUTPUT_NAME=Relatorio_Wialon_%TPL%_%DAYSTR%"
set "OUTPUT_PATH=%cd%\%DAY_FOLDER%\%OUTPUT_NAME%"

cmd /c "echo ------------------------------------------>>"%LOGFILE%""
cmd /c "echo Running Template %TPL% for %DAYSTR% (%DATE% %TIME%) >>"%LOGFILE%""
cmd /c "echo Running Template %TPL% for day %DAYSTR%..."

py wialon_report_sql.py ^
  --token "%TOKEN%" ^
  --resource-id %RESOURCE% ^
  --template-id %TPL% ^
  --object-id %OBJECT% ^
  --from "%FROM_ARG%" --to "%TO_ARG%" ^
  --format xlsx --output "%OUTPUT_PATH%" ^
  --mysql-host "%MYSQL_HOST%" --mysql-user "%MYSQL_USER%" --mysql-pass "%MYSQL_PASS%" --mysql-db "%MYSQL_DB%" ^
  --timeout 3600 --http-timeout 1200 --verbose ^
  >> "%LOGFILE%" 2>&1

set "RC=%ERRORLEVEL%"

cmd /c "echo ExitCode (Template %TPL%, %DAYSTR%): %RC%>>"%LOGFILE%""

if "%RC%"=="0" (
  cmd /c "echo Template %TPL% (%DAYSTR%) succeeded>>"%LOGFILE%""
  cmd /c "echo Template %TPL% (%DAYSTR%) succeeded"
) else (
  cmd /c "echo Template %TPL% (%DAYSTR%) FAILED with code %RC%>>"%LOGFILE%""
  cmd /c "echo Template %TPL% (%DAYSTR%) FAILED with code %RC%"
)

goto :eof
