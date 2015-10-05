@echo off

rem   Licensed to the Apache Software Foundation (ASF) under one or more
rem   contributor license agreements.  See the NOTICE file distributed with
rem   this work for additional information regarding copyright ownership.
rem   The ASF licenses this file to You under the Apache License, Version 2.0
rem   (the "License"); you may not use this file except in compliance with
rem   the License.  You may obtain a copy of the License at
rem 
rem       http://www.apache.org/licenses/LICENSE-2.0
rem 
rem   Unless required by applicable law or agreed to in writing, software
rem   distributed under the License is distributed on an "AS IS" BASIS,
rem   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem   See the License for the specific language governing permissions and
rem   limitations under the License.

rem   =====================================================
rem   Environment variables that can be defined externally:
rem
rem   JMETER_BIN - JMeter bin directory (must end in \)
rem   JM_LAUNCH - java.exe (default) or javaw.exe
rem   JVM_ARGS - additional java options, e.g. -Dprop=val
rem   JM_START - set this to "start" to launch JMeter in a separate window
rem              this is used by the jmeterw.cmd script.
rem
rem   =====================================================

setlocal

rem Minimal version to run JMeter
set MINIMAL_VERSION=1.6.0

for /f "tokens=3" %%g in ('java -version 2^>^&1 ^| findstr /i "version"') do (
    rem @echo Debug Output: %%g
    set JAVAVER=%%g
)
if not defined JAVAVER (
    @echo Not able to find Java executable or version. Please check your Java installation.
    set ERRORLEVEL=2
    goto pause
)
set JAVAVER=%JAVAVER:"=%
for /f "delims=. tokens=1-3" %%v in ("%JAVAVER%") do (
    set current_minor=%%w
)

for /f "delims=. tokens=1-3" %%v in ("%MINIMAL_VERSION%") do (
    set minimal_minor=%%w
)

if not defined current_minor (
    @echo Not able to find Java executable or version. Please check your Java installation.
    set ERRORLEVEL=2
    goto pause
)
rem @echo Debug: CURRENT=%current_minor% - MINIMAL=%minimal_minor%
if %current_minor% LSS %minimal_minor% (
    @echo Error: Java version -- %JAVAVER% -- is too low to run JMeter. Needs a Java version greater than or equal to %MINIMAL_VERSION%
    set ERRORLEVEL=3
    goto pause
)

if .%JM_LAUNCH% == . set JM_LAUNCH=java.exe

if exist jmeter.bat goto winNT1
if .%JMETER_BIN% == . set JMETER_BIN=%~dp0

:winNT1
rem On NT/2K grab all arguments at once
set JMETER_CMD_LINE_ARGS=%*

rem The following link describes the -XX options:
rem http://www.oracle.com/technetwork/java/javase/tech/vmoptions-jsp-140102.html
rem http://java.sun.com/developer/TechTips/2000/tt1222.html has some more descriptions
rem Unfortunately TechTips no longer seem to be available

rem See the unix startup file for the rationale of the following parameters,
rem including some tuning recommendations
set HEAP=-Xms512m -Xmx512m
set NEW=-XX:NewSize=128m -XX:MaxNewSize=128m
set SURVIVOR=-XX:SurvivorRatio=8 -XX:TargetSurvivorRatio=50%
set TENURING=-XX:MaxTenuringThreshold=2
rem Java 8 remove Permanent generation, don't settings the PermSize
if %current_minor% LEQ "8" (
    rem Increase MaxPermSize if you use a lot of Javascript in your Test Plan :
    set PERM=-XX:PermSize=64m -XX:MaxPermSize=128m
)

set CLASS_UNLOAD=-XX:+CMSClassUnloadingEnabled
rem set DEBUG=-verbose:gc -XX:+PrintTenuringDistribution

rem Always dump on OOM (does not cost anything unless triggered)
set DUMP=-XX:+HeapDumpOnOutOfMemoryError

rem Additional settings that might help improve GUI performance on some platforms
rem See: http://java.sun.com/products/java-media/2D/perf_graphics.html

set DDRAW=
rem  Setting this flag to true turns off DirectDraw usage, which sometimes helps to get rid of a lot of rendering problems on Win32.
rem set DDRAW=%DDRAW% -Dsun.java2d.noddraw=true

rem  Setting this flag to false turns off DirectDraw offscreen surfaces acceleration by forcing all createVolatileImage calls to become createImage calls, and disables hidden acceleration performed on surfaces created with createImage .
rem set DDRAW=%DDRAW% -Dsun.java2d.ddoffscreen=false

rem Setting this flag to true enables hardware-accelerated scaling.
rem set DDRAW=%DDRAW% -Dsun.java2d.ddscale=true

rem Server mode
rem Collect the settings defined above
set ARGS=%DUMP% %HEAP% %NEW% %SURVIVOR% %TENURING% %PERM% %CLASS_UNLOAD% %DDRAW%

%JM_START% %JM_LAUNCH% %ARGS% %JVM_ARGS% -jar "%JMETER_BIN%ApacheJMeter.jar" %JMETER_CMD_LINE_ARGS%

rem If the errorlevel is not zero, then display it and pause

if NOT errorlevel 0 goto pause
if errorlevel 1 goto pause

goto end

:pause
echo errorlevel=%ERRORLEVEL%
pause

:end

