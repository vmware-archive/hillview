#!/bin/bash
# load the ontime data into mysql

set -ex

year=2017
#../../bin/download-data.py ../../bin/config-aws.json "data/ontime/${year}*"
#mv ec2-*/* .
#rm -rf ec2-*

rm -f ontime.csv
for i in ${year}*.csv.gz; do
    zcat $i | tail -n +2 >>ontime.csv
done
sudo mv ontime.csv /var/lib/mysql-files
mysql -u root -p <loadontime.sql
# rm -f ${year}_*.csv.gz
