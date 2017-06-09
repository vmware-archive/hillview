#!/bin/bash

if [[ -v JAVA_HOME ]]
then
  #
  # Infer the directory names
  #
  LOCAL_SPARK_FOLDER=`basename spark*.tgz .tgz`
  LOCAL_HADOOP_FOLDER=`basename hadoop*.tar.gz .tar.gz`

  echo "Creating hadoop configuration files according to templates in puppet-manifests/."

  env FACTER_hillview_run_dir=$PWD \
   FACTER_hadoop_folder=$PWD/$LOCAL_HADOOP_FOLDER \
   FACTER_JAVA_HOME=$JAVA_HOME \
   puppet apply hillview_spark_cluster.pp

  $LOCAL_HADOOP_FOLDER/bin/hdfs namenode -format
else
  echo "JAVA_HOME environment variable is unset."
  echo "Please use 'export JAVA_HOME=<path-to-jdk>', preferably in your ~/.bashrc or ~/.zshrc file."
  exit
fi
