@echo off

set "OVERRIDE_ANT_HOME=..\..\..\tools\ant"

if exist "..\..\..\src\bin\build.bat" (
   rem running from TRUNK
   call ..\..\..\src\bin\build.bat %*
) else (
   rem running from the distro
   call ..\..\..\bin\build.bat %*
)

set "OVERRIDE_ANT_HOME="
