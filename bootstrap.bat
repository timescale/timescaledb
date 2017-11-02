echo off
SET BUILD_TYPE=%1
IF "%BUILD_TYPE%" == "" (SET BUILD_TYPE=Release)

SET BUILD_ARCH=%2
IF "%BUILD_ARCH%" == "" (SET BUILD_ARCH=x64)

SET BUILD_DIR=./build
IF NOT EXIST "%BUILD_DIR%" (
	mkdir "%BUILD_DIR%"
) ELSE (
	ECHO Build system already initialized in %BUILD_DIR%
	EXIT
)

cd %BUILD_DIR%
cmake .. -A %BUILD_ARCH% -B. -DCMAKE_BUILD_TYPE=%BUILD_TYPE%

ECHO ---
ECHO TimescaleDB build system initialized in %BUILD_DIR%.
ECHO To compile, do:
ECHO     cd %BUILD_DIR%
ECHO     MSBuild.exe timescaledb.sln
