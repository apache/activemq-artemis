@echo off
rem Licensed to the Apache Software Foundation (ASF) under one
rem or more contributor license agreements.  See the NOTICE file
rem distributed with this work for additional information
rem regarding copyright ownership.  The ASF licenses this file
rem to you under the Apache License, Version 2.0 (the
rem "License"); you may not use this file except in compliance
rem with the License.  You may obtain a copy of the License at
rem 
rem   http://www.apache.org/licenses/LICENSE-2.0
rem 
rem Unless required by applicable law or agreed to in writing,
rem software distributed under the License is distributed on an
rem "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
rem KIND, either express or implied.  See the License for the
rem specific language governing permissions and limitations
rem under the License.

setlocal

if NOT "%ACTIVEMQ_HOME%"=="" goto CHECK_ACTIVEMQ_HOME
PUSHD .
CD %~dp0..
set ACTIVEMQ_HOME=%CD%
POPD

:CHECK_ACTIVEMQ_HOME
if exist "%ACTIVEMQ_HOME%\bin\activemq.cmd" goto CHECK_JAVA

:NO_HOME
echo ACTIVEMQ_HOME environment variable is set incorrectly. Please set ACTIVEMQ_HOME.
goto END

:CHECK_JAVA
set _JAVACMD=%JAVACMD%

if "%JAVA_HOME%" == "" goto NO_JAVA_HOME
if not exist "%JAVA_HOME%\bin\java.exe" goto NO_JAVA_HOME
if "%_JAVACMD%" == "" set _JAVACMD=%JAVA_HOME%\bin\java.exe
goto RUN_JAVA

:NO_JAVA_HOME
if "%_JAVACMD%" == "" set _JAVACMD=java.exe
echo.
echo Warning: JAVA_HOME environment variable is not set.
echo.

:RUN_JAVA

rem "Set Defaults."
set JAVA_ARGS=-Xmx1024M
set ACTIVEMQ_LOGGING_CONF="file:%ACTIVEMQ_HOME%\config\logging.properties"
set ACTIVEMQ_DATA_DIR="%ACTIVEMQ_HOME%\data"
set ACTIVEMQ_LOG_MANAGER=org.jboss.logmanager.LogManager

rem "Load Config"
if "%ACTIVEMQ_CONF%" == "" set ACTIVEMQ_DATA="%ACTIVEMQ_HOME%\bin\activemq.conf.bat"
if exist "%ACTIVEMQ_CONF%" (
   call "%ACTIVEMQ_CONF%" %*
) else (
   echo Config file not found "%ACTIVEMQ_CONF%"
)

rem "Create full JVM Args"
set JVM_ARGS=%JAVA_ARGS% -classpath %ACTIVEMQ_HOME%\lib\* -Dactivemq.home=%ACTIVEMQ_HOME% -Ddata.dir=%ACTIVEMQ_DATA_DIR% -Djava.util.logging.manager=%ACTIVEMQ_LOG_MANAGER% -Dlogging.configuration=%ACTIVEMQ_LOGGING_CONF% -Djava.library.path=%ACTIVEMQ_HOME%\lib\

rem "Set Debug & Cluster props"
if not "%DEBUG_ARGS%"=="" set JVM_ARGS=%JVM_ARGS% %DEBUG_ARGS%
if not "%ACTIVEMQ_CLUSTER_PROPS%"=="" set JVM_ARGS=%JVM_ARGS% %ACTIVEMQ_CLUSTER_PROPS%

"%_JAVACMD%" %JVM_ARGS% org.apache.activemq.cli.ActiveMQ %*

:END
endlocal
GOTO :EOF

:EOF
