@echo off
setlocal EnableExtensions

:: select configuration and keep compiler/toolchain outputs separate
set "EMPY_CONFIG=%~1"
if "%EMPY_CONFIG%"=="" set "EMPY_CONFIG=Debug"
if not "%EMPY_CONFIG%"=="Debug" if not "%EMPY_CONFIG%"=="Release" if not "%EMPY_CONFIG%"=="RelWithDebInfo" if not "%EMPY_CONFIG%"=="MinSizeRel" (
    echo Usage: EmpyWin.bat Debug^|Release^|RelWithDebInfo^|MinSizeRel
    exit /b 1
)
set "EMPY_GENERATOR=%CMAKE_GENERATOR%"
if "%EMPY_GENERATOR%"=="" set "EMPY_GENERATOR=MinGW Makefiles"
set "EMPY_BUILD=Targets\Build-%EMPY_CONFIG%"
set "EMPY_CONAN=Targets\Conan-%EMPY_CONFIG%"
pushd "%~dp0"

:: install dependencies from the configured Conan profile
conan install ConanLists.txt --output-folder="%EMPY_CONAN%" --build=missing -s build_type=%EMPY_CONFIG%
if errorlevel 1 goto failure

:: configure, build and verify
cmake -S . -B "%EMPY_BUILD%" -G "%EMPY_GENERATOR%" -DCMAKE_BUILD_TYPE=%EMPY_CONFIG% -DCMAKE_TOOLCHAIN_FILE="%CD%\%EMPY_CONAN%\conan_toolchain.cmake"
if errorlevel 1 goto failure
cmake --build "%EMPY_BUILD%" --config %EMPY_CONFIG% --parallel
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
