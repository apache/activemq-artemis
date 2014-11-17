@echo off

setlocal

if NOT "%HORNETQ_HOME%"=="" goto CHECK_HORNETQ_HOME
PUSHD .
CD %~dp0..
set HORNETQ_HOME=%CD%
POPD

:CHECK_HORNETQ_HOME
if exist "%HORNETQ_HOME%\bin\hornetq.cmd" goto CHECK_JAVA

:NO_HOME
echo HORNETQ_HOME environment variable is set incorrectly. Please set HORNETQ_HOME.
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

if "%JVM_FLAGS%" == "" set JVM_FLAGS=-XX:+UseParallelGC -XX:+AggressiveOpts -XX:+UseFastAccessorMethods -Xms512M -Xmx1024M -Djava.naming.factory.initial=org.jnp.interfaces.NamingContextFactory -Djava.naming.factory.url.pkgs=org.jboss.naming:org.jnp.interfaces -Dhornetq.home=$HORNETQ_HOME -Ddata.dir=$HORNETQ_HOME/data -Djava.util.logging.manager=org.jboss.logmanager.LogManager -Dlogging.configuration="file:%HORNETQ_HOME%\config\logging.properties" -Djava.library.path="%HORNETQ_HOME%/bin/lib/linux-i686:%HORNETQ_HOME%/bin/lib/linux-x86_64"

if "x%HORNETQ_OPTS%" == "x" goto noHORNETQ_OPTS
  set JVM_FLAGS=%JVM_FLAGS% %HORNETQ_OPTS%
:noHORNETQ_OPTS

if "x%HORNETQ_DEBUG%" == "x" goto noDEBUG
  set JVM_FLAGS=%JVM_FLAGS% -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005
:noDEBUG

if "x%HORNETQ_PROFILE%" == "x" goto noPROFILE
  set JVM_FLAGS=-agentlib:yjpagent %JVM_FLAGS%
:noPROFILE

rem set JMX_OPTS=-Dcom.sun.management.jmxremote.port=1099 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false

set JVM_FLAGS=%JVM_FLAGS% %JMX_OPTS% -Dhornetq.home="%HORNETQ_HOME%" -classpath "%HORNETQ_HOME%\lib\*"

"%_JAVACMD%" %JVM_FLAGS% org.apache.activemq.cli.HornetQ %*

:END
endlocal
GOTO :EOF

:EOF
