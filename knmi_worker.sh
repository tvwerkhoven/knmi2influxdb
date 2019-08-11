#!/usr/bin/env bash

# Query data from last 5 days, if we have a daily failure this is corrected in a next run
curl 'http://projects.knmi.nl/klimatologie/uurgegevens/getdata_uur.cgi' -d "start=$(date --date '5 days ago' +'%Y%m%d')01&vars=ALL&stns=260" -o /tmp/knmidata-query.csv

python3 /home/pi/workers/knmi_temp/knmi2influxdb.py /tmp/knmidata-query.csv /tmp/knmidata-temp-influxformat.csv --query "temperaturev2 outside_knmi{STN}={T:.1f} {date}"
curl -i -XPOST "http://localhost:8086/write?db=smarthome&precision=s" --data-binary @/tmp/knmidata-temp-influxformat.csv

python3 /home/pi/workers/knmi_temp/knmi2influxdb.py /tmp/knmidata-query.csv /tmp/knmidata-weather-influxformat.csv --query "weatherv2 rain_duration_knmi{STN}={DR:.1f},rain_qty_knmi{STN}={RH:.1f},wind_speed_knmi{STN}={FF:.1f},wind_gust_knmi{STN}={FX:.1f},wind_dir_knmi{STN}={DD} {date}{NEWLINE}energyv2 irradiance_knmi{STN}={Q:.0f} {date}"
curl -i -XPOST "http://localhost:8086/write?db=smarthome&precision=s" --data-binary @/tmp/knmidata-weather-influxformat.csv