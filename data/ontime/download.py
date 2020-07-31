#!/usr/bin/env python3
# Downloads some of the OnTime flights database
# pylint: disable=invalid-name,missing-docstring

import os
import subprocess
from sys import platform

wgetoptions = ""

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
        year = int(inMonths / 12)
        month = (inMonths % 12) + 1
        basename = "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_" \
                   + str(year) \
                   + "_" + str(month)
        canonical = canonical_name(year, month)
        if not os.path.exists(canonical + ".csv.gz"):
            filename = basename + ".zip"
            url = "https://transtats.bts.gov/PREZIP/" + filename
            global wgetoptions
            execute_command("wget " + wgetoptions + " -q " + url)
            execute_command("unzip -o " + filename)
            os.unlink(filename)
            os.unlink("readme.html")
            actualname = basename.replace("1987_present", "(1987_present)")
            os.rename(actualname + ".csv", canonical + ".csv")
            execute_command("gzip " + canonical + ".csv")
        else:
            print("Already having " + basename)

def main():
    # Newer versions of ubuntu have problems downloading the data
    # due to security settings, so we have to feed wget some additional
    # options.  Unfortunately these options do not exist on earlier versions.
    global wgetoptions
    if platform == "linux":
        import lsb_release
        info = lsb_release.get_lsb_information()
        if info['ID'] == "Ubuntu" and int(info["RELEASE"].split(".")[0]) > 18:
            wgetoptions = "--cipher 'DEFAULT:!DH'"
    download(2016, 1, 2016, 2)

if __name__ == "__main__":
    main()
