# knmi-temp-pusher

Better than scraping websites ;)

1. Get KNMI historical weather data daily, add to influxdb database
2. Get real-time KNMI weather hourly, add to influxdb database.

# Usage

    knmi2influxdb.py --time actual --station 260 --outuri "http://localhost:8086/write?db=smarthome&precision=s" --query "temperature outside_knmi{STN}={T:.1f} {DATETIME}"

    knmi2influxdb.py --time actual --station 260 --outuri out-file.csv

    knmi2influxdb.py --time historical --station 260 --outuri out-file.csv

## Data source

Script can get data in two methods:
1. Live from KNMI (10-min resolution, updated every 10min)
2. Historical from KNMI (1-hr resolution, updated daily, any timespan)

## Output data

The script can either:
1. Push data to influxdb directly (over HTTP API), or 
2. Store the influx queries in line format to a specified file.

# Background

## Getting live KNMI data

Get data from

    https://data.knmi.nl/datasets/Actuele10mindataKNMIstations/1

then parse using netCDF, insert into influxdb. Example:

    knmi2influxdb.py --time actual --station 260 --outuri "http://localhost:8086/write?db=smarthome&precision=s" --query "temperature outside_knmi{STN}={T:.1f} {DATETIME}"

## Getting historical KNMI data

One can get data via script from

    https://www.knmi.nl/kennis-en-datacentrum/achtergrond/data-ophalen-vanuit-een-script

e.g. 

    curl 'http://projects.knmi.nl/klimatologie/uurgegevens/getdata_uur.cgi' -d "start=$(date --date yesterday +'%Y%m%d')01&vars=ALL&stns=260" -o /tmp/knmidata-query.csv

or for a timerange in the past

    BEG=20180101
    END=20190911
    
    curl 'http://projects.knmi.nl/klimatologie/uurgegevens/getdata_uur.cgi' -d "start=${BEG}01&end=${END}24&vars=ALL&stns=260" -o knmidata-query.csv

or manually from

    http://projects.knmi.nl/klimatologie/uurgegevens/selectie.cgi

Using knmi2influxdb.py to fetch data and convert to lineformat for last 21 days:

    knmi2influxdb.py --time historical --histrange 21 --station 260 --outuri knmidata-influxformat.csv

or for a specific timerange:

    knmi2influxdb.py --time historical --histrange 20171101 20180101 --station 260 --query "temperature outside_knmi{STN}={T:.1f} {DATETIME}" --outuri knmidata-influxformat.csv
    knmi2influxdb.py --time historical --histrange 20160701 20180101 --station 260 --query "energyv2 irradiance_knmi{STN}={Q:.0f} {DATETIME} {DATETIME}" --outuri knmidata-influxformat.csv

### Insert into influxdb

Use curl to post datafile

    curl -i -XPOST "https://localhost:8086/write?db=smarthome&precision=s" --data-binary @/tmp/knmidata-influxformat.csv

# References

- https://www.knmi.nl/kennis-en-datacentrum/achtergrond/data-ophalen-vanuit-een-script
- http://projects.knmi.nl/klimatologie/uurgegevens/
- https://data.knmi.nl/download/Actuele10mindataKNMIstations/1