# knmi-temp-pusher

Get KNMI temp daily, add to database

# Data source

Via script from

    https://www.knmi.nl/kennis-en-datacentrum/achtergrond/data-ophalen-vanuit-een-script

or manually from

    http://projects.knmi.nl/klimatologie/uurgegevens/selectie.cgi

# Get data

e.g. 

    curl 'http://projects.knmi.nl/klimatologie/uurgegevens/getdata_uur.cgi' -d "start=$(date --date yesterday +'%Y%m%d')01&vars=ALL&stns=260" -o /tmp/knmidata-query.csv

# Convert format

Use knmi2influxdb.py, e.g.

    python3 knmi2influxdb.py /tmp/knmidata-query.csv /tmp/knmidata-influxformat.csv --query "temperaturev2 outside_knmi{STN}={T:.1f} {date}"

# Insert into influxdb

Use curl to post datafile

    curl -i -XPOST "http://localhost:8086/write?db=smarthome&precision=s" --data-binary @/tmp/knmidata-influxformat.csv

# Get one time fix

BEG=20180101
END=20190911
curl 'http://projects.knmi.nl/klimatologie/uurgegevens/getdata_uur.cgi' -d "start=${BEG}01&end=${END}24&vars=ALL&stns=260" -o /tmp/knmidata-query.csv
python3 knmi2influxdb.py /tmp/knmidata-query.csv /tmp/knmidata-influxformat.csv --query 'temperaturev2 outside_knmi{STN}={T:.1f} {DATETIME}{NEWLINE}weatherv2 rain_duration_knmi{STN}={DR:.1f},rain_qty_knmi{STN}={RH:.1f},wind_speed_knmi{STN}={FF:.1f},wind_gust_knmi{STN}={FX:.1f},wind_dir_knmi{STN}={DD} {DATETIME}{NEWLINE}energyv2 irradiance_knmi{STN}={Q:.0f} {DATETIME}'
curl -i -XPOST "http://localhost:8086/write?db=smarthome&precision=s" --data-binary @/tmp/knmidata-influxformat.csv

