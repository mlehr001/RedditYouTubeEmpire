@echo off
:: ============================================================
:: transfer.bat — Copy finished videos to your main PC
:: ============================================================
:: Set PC_OUTPUT_PATH in your .env file, OR edit the line below.
:: Network example:  \\DESKTOP-PC\Videos\RedditYouTubeEmpire
:: Drive example:    D:\RedditYouTubeEmpire
:: ============================================================

:: Load PC_OUTPUT_PATH from .env if present
for /f "tokens=1,* delims==" %%a in (.env) do (
    if "%%a"=="PC_OUTPUT_PATH" set DEST=%%b
)

if "%DEST%"=="" (
    echo ERROR: PC_OUTPUT_PATH is not set in your .env file.
    echo Edit .env and add:  PC_OUTPUT_PATH=\\YOUR-PC\SharedFolder
    pause
    exit /b 1
)

echo.
echo Transferring output\ to %DEST%
echo.

robocopy output "%DEST%" *.mp4 /E /COPYALL /R:3 /W:5 /NP /LOG:transfer_log.txt

echo.
echo Done! Check transfer_log.txt for details.
pause
