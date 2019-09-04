REM a script which stops Hillview on a Windows machine

call detect-java.bat
..\apache-tomcat-9.0.4\bin\catalina.sh stop
set /p PID=<hillview-worker.pid
taskkill /F /PID %PID%
del hillview-worker.pid
