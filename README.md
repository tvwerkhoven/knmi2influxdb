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
1. Live from KNMI
2. From file (disabled now)

## Output data

The script can either:
1. Push data to influxdb directly (over HTTP API), or 
2. Store the influx queries in line format to a specified file.

# Background

## Getting live KNMI data

Get data from

    https://data.knmi.nl/datasets/Actuele10mindataKNMIstations/1

then parse using netCDF, insert into influxdb.

## Getting historical KNMI data

One can get data via script from

    https://www.knmi.nl/kennis-en-datacentrum/achtergrond/data-ophalen-vanuit-een-script

e.g. 

    curl 'http://projects.knmi.nl/klimatologie/uurgegevens/getdata_uur.cgi' -d "start=$(date --date yesterday +'%Y%m%d')01&vars=ALL&stns=260" -o /tmp/knmidata-query.csv

or manually from

    http://projects.knmi.nl/klimatologie/uurgegevens/selectie.cgi

## Convert to influx line format

Use knmi2influxdb.py, e.g. (does not work at the moment)

    python3 knmi2influxdb.py /tmp/knmidata-query.csv /tmp/knmidata-influxformat.csv --query "temperature outside_knmi{STN}={T:.1f} {date}"

## Insert into influxdb

Use curl to post datafile

    curl -i -XPOST "http://localhost:8086/write?db=smarthome&precision=s" --data-binary @/tmp/knmidata-influxformat.csv

## Get one time fix

Use this to get bulk data from KNMI and insert into influxdb. We split this in
multiple steps to allow debugging along the way.

	BEG=20180101
	END=20190911
	
	curl 'http://projects.knmi.nl/klimatologie/uurgegevens/getdata_uur.cgi' -d "start=${BEG}01&end=${END}24&vars=ALL&stns=260" -o /tmp/knmidata-query.csv
	
	python3 knmi2influxdb.py /tmp/knmidata-query.csv /tmp/knmidata-influxformat.csv --query 'temperaturev2 outside_knmi{STN}={T:.1f} {DATETIME}{NEWLINE}weatherv2 rain_duration_knmi{STN}={DR:.1f},rain_qty_knmi{STN}={RH:.1f},wind_speed_knmi{STN}={FF:.1f},wind_gust_knmi{STN}={FX:.1f},wind_dir_knmi{STN}={DD} {DATETIME}{NEWLINE}energyv2 irradiance_knmi{STN}={Q:.0f} {DATETIME}'
	
	curl -i -XPOST "http://localhost:8086/write?db=smarthome&precision=s" --data-binary @/tmp/knmidata-influxformat.csv

