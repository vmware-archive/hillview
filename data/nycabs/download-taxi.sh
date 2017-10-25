#!/bin/bash

# https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-01.csv
# could start as early as 2009
for year in `seq 2017 2017`; do
    for month in `seq 1 2`; do
        filename=`printf "yellow_tripdata_%02d-%02d.csv" ${year} ${month}`
        if [ ! -f "$filename" ]; then
            url="https://s3.amazonaws.com/nyc-tlc/trip+data/$filename"
            wget $url
        fi
    done
done
