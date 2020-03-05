REM a script which starts Hillview on a Windows machine

set CATALINA_HOME=..\apache-tomcat-9.0.4
call detect-java.bat
start /B java -jar ..\platform\target\hillview-server-jar-with-dependencies.jar 127.0.0.1:3569
start /B ..\apache-tomcat-9.0.4\bin\catalina.bat run
start /B http://localhost:8080
