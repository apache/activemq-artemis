@ echo off
setlocal ENABLEDELAYEDEXPANSION
set HORNETQ_HOME=..
IF "a%1"== "a" ( 
set CONFIG_DIR=%HORNETQ_HOME%\config\stand-alone\non-clustered
) ELSE (
SET CONFIG_DIR=%1
)
set CLASSPATH=%CONFIG_DIR%;%HORNETQ_HOME%\schemas\
REM you can use the following line if you want to run with different ports
REM set CLUSTER_PROPS="-Djnp.port=1099 -Djnp.rmiPort=1098 -Djnp.host=localhost -Dhornetq.remoting.netty.host=localhost -Dhornetq.remoting.netty.port=5445"
set JVM_ARGS=%CLUSTER_PROPS% -XX:+UseParallelGC  -XX:+AggressiveOpts -XX:+UseFastAccessorMethods -Xms512M -Xmx1024M -Dhornetq.config.dir=%CONFIG_DIR% -Djava.util.logging.manager=org.jboss.logmanager.LogManager -Djava.util.logging.config.file=%CONFIG_DIR%\logging.properties -Djava.library.path=.
REM export JVM_ARGS="-Xmx512M -Djava.util.logging.manager=org.jboss.logmanager.LogManager -Djava.util.logging.config.file=%CONFIG_DIR%\logging.properties -Dhornetq.config.dir=$CONFIG_DIR -Djava.library.path=. -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005"
for /R ..\lib %%A in (*.jar) do (
SET CLASSPATH=!CLASSPATH!;%%A
)
mkdir ..\logs
echo ***********************************************************************************
echo "java %JVM_ARGS% -classpath %CLASSPATH% org.hornetq.integration.bootstrap.HornetQBootstrapServer hornetq-beans.xml"
echo ***********************************************************************************
java %JVM_ARGS% -classpath "%CLASSPATH%" org.hornetq.integration.bootstrap.HornetQBootstrapServer hornetq-beans.xml
