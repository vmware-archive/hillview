rem Script to detect Java installation path
rem Adapted from https://stackoverflow.com/questions/3930383/jre-installation-directory-in-windows

@ECHO on

SET KIT=JavaSoft\Java Runtime Environment
call:ReadRegValue VER "HKLM\Software\%KIT%" "CurrentVersion"
IF "%VER%" NEQ "" GOTO FoundJRE

SET KIT=Wow6432Node\JavaSoft\Java Runtime Environment
call:ReadRegValue VER "HKLM\Software\%KIT%" "CurrentVersion"
IF "%VER%" NEQ "" GOTO FoundJRE

SET KIT=JavaSoft\Java Development Kit
call:ReadRegValue VER "HKLM\Software\%KIT%" "CurrentVersion"
IF "%VER%" NEQ "" GOTO FoundJRE

SET KIT=Wow6432Node\JavaSoft\Java Development Kit
call:ReadRegValue VER "HKLM\Software\%KIT%" "CurrentVersion"
IF "%VER%" NEQ "" GOTO FoundJRE

ECHO Failed to find Java
GOTO :EOF

:FoundJRE
call:ReadRegValue JAVAPATH "HKLM\Software\%KIT%\%VER%" "JavaHome"
ECHO %JAVAPATH%
SET JAVA_HOME=%JAVAPATH%
GOTO :EOF

:ReadRegValue
SET key=%2%
SET name=%3%
SET "%~1="
SET reg=reg
IF DEFINED ProgramFiles(x86) (
  IF EXIST %WINDIR%\sysnative\reg.exe SET reg=%WINDIR%\sysnative\reg.exe
)
FOR /F "usebackq tokens=3* skip=1" %%A IN (`%reg% QUERY %key% /v %name% 2^>NUL`) DO SET "%~1=%%A %%B"
GOTO :EOF
