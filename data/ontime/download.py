#!/usr/bin/env python
# Downloads some of the OnTime flights database
# pylint: disable=invalid-name,missing-docstring

import os
import subprocess

def execute_command(command):
    """Executes the specified command using a shell"""
    print(command)
    exitcode = subprocess.call(command, shell=True)
    if exitcode != 0:
        print("Exit code returned:", exitcode)
        exit(exitcode)

def canonical_name(year, month):
    return "On_Time_On_Time_Performance_" + str(year) + "_" + str(month)

def download(startyear, startmonth, endyear, endmonth):
    months = (endyear - startyear) * 12 + endmonth - startmonth + 1
    if months < 0:
        print("Illegal time range")
    for i in range(0, months):
        inMonths = startyear*12 + startmonth + i - 1
        year = inMonths / 12
        month = (inMonths % 12) + 1
        if year >= 2018:
            basename = "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_" + str(year) + "_" + str(month)
        else:
            # Unfortunately currently the website no longer has these files and it is not
            # clear what name they should have
            basename = "On_Time_On_Time_Performance_" + str(year) + "_" + str(month)
        canonical = canonical_name(year, month)
        if not os.path.exists(canonical + ".csv.gz"):
            filename = basename + ".zip"
            url = "https://transtats.bts.gov/PREZIP/" + filename
            execute_command("wget -q " + url)
            execute_command("unzip -o " + filename)
            os.unlink(filename)
            os.unlink("readme.html")
            actualname = basename.replace("1987_present", "(1987_present)")
            os.rename(actualname + ".csv", canonical + ".csv")
            execute_command("gzip " + canonical + ".csv")
        else:
            print("Already having " + basename)

def main():
    download(2018, 1, 2018, 2)

if __name__ == "__main__":
    main()
