#!/bin/bash

set -x

# First seq is the year; legal values are currently 1988-2016
# Second seq is the month; legal values are 1-12
for year in `seq 2016 2016`; do
    for month in `seq 1 2`; do
        basename="On_Time_On_Time_Performance_${year}_${month}"
        if [ ! -f "$basename.csv" ]; then
            filename="$basename.zip"
            url="https://transtats.bts.gov/PREZIP/$filename"
            wget $url
            unzip -o $filename
            rm $filename
            rm -f readme.html
            gzip "$basename.csv"
        fi
    done
done
