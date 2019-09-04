# This Powershell script installs the software needed to run Hillview on Windows

echo "Installing apache Tomcat web server"
if ( -not ( Test-Path .\apache-tomcat-9.0.4 )) {
   Invoke-WebRequest -OutFile tomcat.zip https://archive.apache.org/dist/tomcat/tomcat-9/v9.0.4/bin/apache-tomcat-9.0.4-windows-x64.zip
   Expand-Archive .\tomcat.zip -DestinationPath .
   rm apache-tomcat-9.0.4\webapps\ROOT -r -fo
   del tomcat.zip
}

$ARCHIVE="hillview-bin.zip"

echo "Installing Hillview"
Invoke-WebRequest -OutFile $ARCHIVE https://github.com/vmware/hillview/releases/download/v0.7-alpha/$ARCHIVE
Expand-Archive $ARCHIVE -DestinationPath .
mv web\target\web-1.0-SNAPSHOT.war apache-tomcat-9.0.4\webapps\ROOT.war
del $ARCHIVE
