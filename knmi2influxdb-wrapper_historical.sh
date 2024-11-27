#!/usr/bin/env bash
# 
# Wrapper for knmi2influxdb.py
#
# Get verified historical KNMI data daily around noon (but +-1hr), this is when data from previous day becomes available
# 0 12 * * * /home/tim/workers/knmi2influxdb/knmi_worker_historical.sh

# Secret settings - get from local dir https://stackoverflow.com/a/53122736 and https://stackoverflow.com/a/17744637
__dir="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source ${__dir}/../secrets.sh # Should contain INFLUX_USERNAME, INFLUX_PASSWORD, e.g. 'export KNMIAPIKEY="XYZ"'


# Call python3 directly because cron's env doesn't always work (somehow)
/usr/bin/python3 /home/tim/workers/knmi2influxdb/knmi2influxdb.py --time historical --station 260 --influxusername ${INFLUX_USERNAME} --influxpassword ${INFLUX_PASSWORD} --outuri "http://${INFLUX_HOST}:8086/write?db=smarthomev3&precision=s" --query 'temperaturev3,quantity=actual,source=knmi{STN},location=outside value={T:.2f} {DATETIME}{NEWLINE}weatherv3,quantity=rain,type=duration,source=knmi{STN} value={DR:.2f} {DATETIME}{NEWLINE}weatherv3,quantity=rain,type=quantity,source=knmi{STN} value={RH:.2f} {DATETIME}{NEWLINE}weatherv3,quantity=wind,type=speed,source=knmi{STN} value={FF:.2f} {DATETIME}{NEWLINE}weatherv3,quantity=wind,type=gust,source=knmi{STN} value={FX:.2f} {DATETIME}{NEWLINE}weatherv3,quantity=wind,type=direction,source=knmi{STN} value={DD} {DATETIME}{NEWLINE}energyv3,quantity=irradiance,type=production,source=knmi{STN} value={Q:.0f} {DATETIME}' 2>&1
