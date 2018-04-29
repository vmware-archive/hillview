#!/bin/bash
# Simple shell script which tries to force a Java process to execute GC

set -ex
PID=`jcmd | grep hillview-server | awk '{print $1}'`
jcmd $PID GC.run

