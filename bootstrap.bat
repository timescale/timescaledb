@echo off
:: This bootstrap scripts set up the build environment for TimescaleDB
:: Any flags will be passed on to CMake, e.g.,
:: ./bootstrap.bat -DCMAKE_BUILD_TYPE="Debug"


:: Get source directory to build from
set ORIG=%0
for %%F in (%ORIG%) do set SRC_DIR=%%~dpF

SET BUILD_DIR=./build

IF EXIST "%BUILD_DIR%" (
	setlocal EnableDelayedExpansion
	ECHO Build system already initialized in %BUILD_DIR%
	SET /P resp="Do you want to remove it (this is IMMEDIATE and PERMANENT), y/n? "
	IF "!resp!" == "y" (
		rd /s /q "%BUILD_DIR%"
	) ELSE (
		ECHO Exiting
		EXIT
	)
)

mkdir "%BUILD_DIR%"
cd "%BUILD_DIR%"
cmake %SRC_DIR% -A x64 %*

ECHO ---
ECHO TimescaleDB build system initialized in %BUILD_DIR%.
ECHO To compile, do:
ECHO     cmake --build %BUILD_DIR% --config Release
