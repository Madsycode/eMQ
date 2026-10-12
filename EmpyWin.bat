@echo off
setlocal EnableExtensions

:: select the configuration; each one owns Targets\<Config> (Conan files, build tree, bin)
set "EMPY_CONFIG=%~1"
if "%EMPY_CONFIG%"=="" set "EMPY_CONFIG=Debug"
if not "%EMPY_CONFIG%"=="Debug" if not "%EMPY_CONFIG%"=="Release" (
    echo Usage: EmpyWin.bat Debug^|Release
    exit /b 1
)

set "EMPY_GENERATOR=%CMAKE_GENERATOR%"
if "%EMPY_GENERATOR%"=="" set "EMPY_GENERATOR=MinGW Makefiles"
set "EMPY_BUILD=Targets\%EMPY_CONFIG%"
set "EMPY_CONAN=Targets\Conan"
pushd "%~dp0"

:: Keep Conan's MSYS build tools from shadowing the native MinGW compilers.
set "EMPY_CONAN_COMPILERS="
if not "%EMPY_GENERATOR%"=="MinGW Makefiles" goto dependencies
for %%T in (gcc.exe g++.exe mingw32-make.exe) do (
    if "%%~$PATH:T"=="" (
        echo Missing %%T. Add the MinGW bin directory to PATH before building.
        goto failure
    )
)
for %%T in (gcc.exe) do set "EMPY_C_COMPILER=%%~$PATH:T"
for %%T in (g++.exe) do set "EMPY_CXX_COMPILER=%%~$PATH:T"
set "EMPY_CONAN_COMPILERS=-c "tools.build:compiler_executables={\"c\":\"%EMPY_C_COMPILER:\=/%\",\"cpp\":\"%EMPY_CXX_COMPILER:\=/%\"}""

:dependencies
:: install dependencies from the configured Conan profile
conan install ConanLists.txt --output-folder="%EMPY_CONAN%" --build=missing -s build_type=%EMPY_CONFIG% %EMPY_CONAN_COMPILERS%
if errorlevel 1 goto failure

:: configure, build and verify
cmake -S . -B "%EMPY_BUILD%" -G "%EMPY_GENERATOR%" -DCMAKE_BUILD_TYPE=%EMPY_CONFIG% -DCMAKE_TOOLCHAIN_FILE="%CD%\%EMPY_CONAN%\conan_toolchain.cmake"
if errorlevel 1 goto failure
cmake --build "%EMPY_BUILD%" --config %EMPY_CONFIG% --parallel %CMAKE_BUILD_PARALLEL_LEVEL%
if errorlevel 1 goto failure
ctest --test-dir "%EMPY_BUILD%" -C %EMPY_CONFIG% --output-on-failure
if errorlevel 1 goto failure

popd
endlocal
exit /b 0

:failure
popd
endlocal
exit /b 1
