#!/usr/bin/env python
# Downloads some of the OnTime flights database

import os
import subprocess

def execute_command(command):
    """Executes the specified command using a shell"""
    print(command)
    exitcode = subprocess.call(command, shell=True)
    if exitcode != 0:
        print("Exit code returned:", exitcode)
        exit(exitcode)

def download(startyear, startmonth, endyear, endmonth):
    months = (endyear - startyear) * 12 + endmonth - startmonth + 1;
    if months < 0:
        print("Illegal time range");
    for i in range(0, months):
        inMonths = startyear*12 + startmonth + i - 1
        year = inMonths / 12
        month = (inMonths % 12) + 1
        basename="On_Time_On_Time_Performance_" + str(year) + "_" + str(month)
        if not os.path.exists(basename + ".csv.gz"):
            filename=basename + ".zip"
            url="https://transtats.bts.gov/PREZIP/" + filename
            execute_command("wget -q " + url)
            execute_command("unzip -o " + filename)
            os.unlink(filename)
            os.unlink("readme.html")
            execute_command("gzip " + basename + ".csv")
        else:
            print("Already having " + basename)

def main():
    download(2016, 1, 2016, 2)

if __name__ == "__main__":
    main()
