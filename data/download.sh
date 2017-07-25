#!/bin/bash

# could start as early as 1988
for year in `seq 2016 2016`; do
    for month in `seq 1 2`; do
        basename="On_Time_On_Time_Performance_${year}_${month}"
        if [ ! -f "$basename.csv" ]; then
            filename="$basename.zip"
            url="https://transtats.bts.gov/PREZIP/$filename"
            wget $url
            unzip -o $filename
            rm $filename
            mv readme.html "On_Time_On_Time_Performance_readme.html"
        fi
    done
done
