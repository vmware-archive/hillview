#!/bin/bash

# https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-01.csv
# could start as early as 2009
for year in $(seq 2009 2018); do
    for month in $(seq 1 12); do
        filename=$(printf "yellow_tripdata_%02d-%02d.csv" ${year} ${month})
        if [ ! -f "${filename}.gz" ]; then
            url="https://s3.amazonaws.com/nyc-tlc/trip+data/$filename"
            wget $url
            gzip $filename &
        fi
    done
done
