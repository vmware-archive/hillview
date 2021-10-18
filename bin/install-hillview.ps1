# This Powershell script installs the software needed to run Hillview on Windows

TOMCATVERSION="9.0.4"
HILLVIEW_VERSION="0.9.2-beta"

echo "Installing apache Tomcat web server"
if ( -not ( Test-Path ".\apache-tomcat-$TOMCATVERSION" )) {
   Invoke-WebRequest -OutFile tomcat.zip "https://archive.apache.org/dist/tomcat/tomcat-9/v$TOMCATVERSION/bin/apache-tomcat-$TOMCATVERSION-windows-x64.zip"
   Expand-Archive .\tomcat.zip -DestinationPath .
   rm "apache-tomcat-$TOMCATVERSION\webapps\ROOT" -r -fo
   del tomcat.zip
}

$ARCHIVE="hillview-bin.zip"

echo "Installing Hillview"
Invoke-WebRequest -OutFile $ARCHIVE "https://github.com/vmware/hillview/releases/download/v$HILLVIEW_VERSION/$ARCHIVE"
Expand-Archive $ARCHIVE -DestinationPath .
mv web\target\web-1.0-SNAPSHOT.war "apache-tomcat-$TOMCATVERSION\webapps\ROOT.war"
del $ARCHIVE
