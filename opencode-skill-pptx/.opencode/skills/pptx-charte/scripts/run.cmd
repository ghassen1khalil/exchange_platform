@echo off
rem ---------------------------------------------------------------------------
rem  Lanceur Windows du skill pptx-charte (aucun droit administrateur requis).
rem  Un .cmd n'est pas soumis a l'ExecutionPolicy de PowerShell.
rem  Usage : scripts\run.cmd commande options     exemple : scripts\run.cmd doctor
rem ---------------------------------------------------------------------------
setlocal EnableExtensions
set "HERE=%~dp0"
set "PY="
set "PYARGS="
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
set PYTHONDONTWRITEBYTECODE=1

rem 1) Python embarque eventuellement place dans le skill ou dans le profil utilisateur
if exist "%HERE%..\python\python.exe" set "PY=%HERE%..\python\python.exe"
if not defined PY if exist "%LOCALAPPDATA%\pptx-charte\python\python.exe" set "PY=%LOCALAPPDATA%\pptx-charte\python\python.exe"

rem 2) Lanceur py (installation python.org)
if not defined PY (
  where py >nul 2>nul
  if not errorlevel 1 (
    py -3 -c "import sys" >nul 2>nul
    if not errorlevel 1 ( set "PY=py" & set "PYARGS=-3" )
  )
)

rem 3) "python" dans le PATH (en ecartant le faux python du Microsoft Store)
if not defined PY (
  python -c "import sys" >nul 2>nul
  if not errorlevel 1 set "PY=python"
)

if not defined PY goto :nopython

"%PY%" %PYARGS% "%HERE%pptx_tool.py" %*
exit /b %ERRORLEVEL%

:nopython
echo [pptx-charte] Python 3.8+ introuvable. 1>&2
echo   Installez-le SANS droits admin, au choix : 1>&2
echo     winget install -e --id Python.Python.3.12 --scope user 1>&2
echo     ou python.org ^> "Windows embeddable package" a decompresser dans %%LOCALAPPDATA%%\pptx-charte\python 1>&2
echo     ou demandez-le au Centre logiciel / a l'IT. 1>&2
echo   Details : references\windows-sans-admin.md 1>&2
exit /b 9
