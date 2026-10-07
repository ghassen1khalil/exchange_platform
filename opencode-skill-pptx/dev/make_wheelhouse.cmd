@echo off
rem Prepare un dossier de wheels Windows pour une installation hors-ligne du skill.
rem Usage : make_wheelhouse.cmd 3.12     (version de Python CIBLE ; defaut 3.12)
setlocal
set "V=%~1"
if "%V%"=="" set "V=3.12"
set "OUT=%~dp0wheelhouse-%V%"
py -3 -m pip download -d "%OUT%" --only-binary=:all: --platform win_amd64 --python-version %V% --implementation cp python-pptx lxml Pillow XlsxWriter typing_extensions
if errorlevel 1 exit /b 1
echo.
echo Wheels pour Python %V% dans : %OUT%
echo Sur le poste cible :  scripts\run.cmd setup --wheelhouse "chemin\wheelhouse-%V%"
