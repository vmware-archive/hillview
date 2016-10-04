#!/bin/bash

ssh-keygen -t rsa -b 4096 -P '' -f ~/.ssh/hdfs_localhost
cat ~/.ssh/hdfs_localhost.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
