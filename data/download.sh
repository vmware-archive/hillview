#!/bin/bash

# could start as early as 1988
for year in `seq 2015 2016`; do
    for month in `seq 1 12`; do
        basename="On_Time_On_Time_Performance_${year}_${month}"
        if [ ! -f "$basename.csv" ]; then
            filename="$basename.zip"
            url="https://transtats.bts.gov/PREZIP/$filename"
            wget $url
            unzip -o $filename
            rm $filename
        fi
    done
done
