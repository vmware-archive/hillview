#!/usr/bin/env python3
# Downloads some of the OnTime flights database
# pylint: disable=invalid-name,missing-docstring

import os
import subprocess
from sys import platform
import urllib.request
import json


def execute_command(command):
    """Executes the specified command using a shell"""
    print(command)
    exitcode = subprocess.call(command, shell=True)
    if exitcode != 0:
        print("Exit code returned:", exitcode)
        exit(exitcode)


def canonical_name(year, month):
    return f"On_Time_On_Time_Performance_{year}_{month}"


def get_latest_wayback_machine_snapshot(url: str):
    # reference: https://archive.org/help/wayback_api.php
    wayback_api = f"https://archive.org/wayback/available?url={url}"
    response = json.loads(urllib.request.urlopen(wayback_api).read())
    return response["archived_snapshots"]["closest"]["url"]


def download(startyear, startmonth, endyear, endmonth):
    months = (endyear - startyear) * 12 + endmonth - startmonth + 1
    if months < 0:
        print("Illegal time range")
    for i in range(0, months):
        inMonths = startyear * 12 + startmonth + i - 1
        year = inMonths // 12
        month = inMonths % 12 + 1
        basename = (
            f"On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}"
        )
        canonical = canonical_name(year, month)
        if not os.path.exists(canonical + ".csv.gz"):
            filename = basename + ".zip"
            url = "https://transtats.bts.gov/PREZIP/" + filename

            # The ip address range used for github actions seem to have been
            # blocked by bts.gov so we use the following workaround: if GitHub
            # actions environment is detected then we download the latest
            # snapshot from internet archive instead.

            # Note that since these zip archives are behind a web form on the
            # bts.gov website so they are not automatically archived by wayback
            # machine. If a link hasn't been captured by wayback machine, go the
            # internet archive (https://archive.org/web/) and save it manually.
            if os.getenv("GITHUB_ACTIONS") == "true":
                wgetoptions = ""
                url = get_latest_wayback_machine_snapshot(url)
            else:
                wgetoptions = "--no-check-certificate"

            execute_command("wget " + wgetoptions + " -q " + url)
            execute_command("unzip -o " + filename)
            os.unlink(filename)
            os.unlink("readme.html")
            actualname = basename.replace("1987_present", "(1987_present)")
            os.rename(actualname + ".csv", canonical + ".csv")
            execute_command("gzip " + canonical + ".csv")
        else:
            print("Already having " + basename)


if __name__ == "__main__":
    download(2016, 1, 2016, 2)
