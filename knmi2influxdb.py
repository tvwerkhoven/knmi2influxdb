#!/usr/bin/env python3
#
# Convert KNMI hourly CSV data to influxdb line protocol.
# 
# Input can be either a KNMI data file, or the script can query data directly
# Output can be either a file, or a URI to an influxdb server to push data directly.
#

import requests
import csv
import argparse
import datetime

DEFAULTQUERY='test,src=outside_knmi{STN} wind={DD},windspeed={FF:.1f},temp={T:.1f},irrad={Q:.2f},rain={RH:.1f} {DATETIME}'
DEFAULTQUERY='temperaturev2 outside_knmi{STN}={T:.1f} {DATETIME}{NEWLINE}weatherv2 rain_duration_knmi{STN}={DR:.1f},rain_qty_knmi{STN}={RH:.1f},wind_speed_knmi{STN}={FF:.1f},wind_gust_knmi{STN}={FX:.1f},wind_dir_knmi{STN}={DD} {DATETIME}{NEWLINE}energyv2 irradiance_knmi{STN}={Q:.0f} {DATETIME}'
#DEFAULTQUERY='temperaturev2 outside_knmi{STN}={T:.1f} {DATETIME}'
KNMISTATION=260 # KNMI station for getting live data. See http://projects.knmi.nl/klimatologie/uurgegevens/

# Required for graceful None formatting, sometimes KNMI data has null entries, 
# but influxdb does not recognize this. We solve this by rendering None and 
# then removing those fields
# https://stackoverflow.com/questions/20248355/how-to-get-python-to-gracefully-format-none-and-non-existing-fields
import string
class PartialFormatter(string.Formatter):
    def __init__(self, missing='~~', bad_fmt='!!'):
        self.missing, self.bad_fmt=missing, bad_fmt

    def get_field(self, field_name, args, kwargs):
        # Handle a key not found
        try:
            val=super(PartialFormatter, self).get_field(field_name, args, kwargs)
            # Python 3, 'super().get_field(field_name, args, kwargs)' works
        except (KeyError, AttributeError):
            val=None,field_name 
        return val 

    def format_field(self, value, spec):
        # handle an invalid format
        if value==None: return self.missing
        try:
            return super(PartialFormatter, self).format_field(value, spec)
        except ValueError:
            if self.bad_fmt is not None: return self.bad_fmt   
            else: raise

def get_knmi_data(knmisource, knmistation=KNMISTATION):
	# If source is KNMI, get live data
	if (knmisource == 'KNMI'):
		# Get data from last 5 days by default
		start = datetime.datetime.now() - datetime.timedelta(days=21)
		
		knmiuri = 'http://projects.knmi.nl/klimatologie/uurgegevens/getdata_uur.cgi'
		knmiquery = "start=2018010101&vars=ALL&stns={}".format(start.strftime("%Y%m%d"), knmistation)

		# Query can take quite long, set long-ish timeout
		r = requests.post(knmiuri, data=knmiquery, timeout=30)

		# Return line-wise iterable for next stage
		return r.text.splitlines()
	else:
		with open(knmisource) as fdk:
			return fdk.readlines()

def convert_knmi(knmidata, query):
	start = False
	fieldpos = {}
	fieldval = {'NEWLINE':"\n"}
	fieldfunc = {
		'YYYYMMDD': lambda x: datetime.datetime(int(x[0:4]), int(x[4:6]), int(x[6:8]), tzinfo=datetime.timezone.utc),
		'HH': lambda x: int(x),
		'DD': lambda x: int(x),
		'FF': lambda x: int(x)/10,
		'FX': lambda x: int(x)/10,
		'T':  lambda x: int(x)/10,
		'SQ': lambda x: int(x)/10,
		'Q':  lambda x: int(x)*10000/3600, # Convert J/cm^2/hour to W/m^2, i.e. 10000cm^2/m^2 and 1/3600 hr/sec 
		'DR': lambda x: int(x)/10,
		'RH': lambda x: max(int(x),0)/10 # RH is values? From their doc: RH       = Uursom van de neerslag (in 0.1 mm) (-1 voor <0.05 mm); 
	}
	parsed_lines = []
	fmt=PartialFormatter()

	for r in knmidata:
		row = r.replace(' ','').split(',')
		# print(row)

		# Find start row (syntax should be like # STN,YYYYMMDD,   HH,   DD,   FH,   FF,   FX,    T,  T10,   TD,   SQ,    Q,   DR,   RH,    P,   VV,    N,    U,   WW,   IX,    M,    R,    S,    O,    Y)
		if (row[0][0] == "#" and len(row)>2 and "YYYYMMDD" in row[1] and not start):
			try:
				fieldpos['STN'] = 0
				fieldpos['YYYYMMDD'] = row.index("YYYYMMDD")
				fieldpos['HH'] = row.index("HH")
				fieldpos['DD'] = row.index("DD")
				fieldpos['FF'] = row.index("FF")
				fieldpos['FX'] = row.index("FX")
				fieldpos['T'] = row.index("T")
				fieldpos['SQ'] = row.index("SQ")
				fieldpos['Q'] = row.index("Q")
				fieldpos['DR'] = row.index("DR")
				fieldpos['RH'] = row.index("RH")
			except ValueError as e:
				quit("KNMI data file incompatible, could not find fields HH, DD or others: {}".format(e))
			start = True

		# After we found the start marker, parse data.
		elif (start and len(row) > 1):
			# valstn = row[0]
			# valyyyymmdd = row[1]
			for fname, pos in fieldpos.items():
				try:
					# Apply conversion function to each field's value,
					# if none specified, pass value as is.
					fieldval[fname] = fieldfunc.get(fname, lambda x: x)(row[pos])
				except:
					# print("Empty val for {}".format(fname))
					fieldval[fname] = None
				# print(fname, row[pos], fieldval[fname])

			# Construct datetime from date and hour fields
			# N.B. Although observations run from hour-1 to hour, (e.g. 
			# slot 1 runs from 00:00 to 01:00), most measurements are taken 
			# at the end of the slot, so we use that timestamp as given.
			# N.B. HH runs from 1-24, so we can't make a time directly 
			# (which runs from 0-23), so instead we add as timedelta
			# N.B. timestamp() only works in python3
			fieldval['DATETIME'] = int((fieldval['YYYYMMDD'] + datetime.timedelta(hours=fieldval['HH'])).timestamp())

			# Unpack dict to format query.
			# See https://github.com/influxdata/docs.influxdata.com/issues/717#issuecomment-249618099
			outline = fmt.format(query, **fieldval)
			# Influxdb does not recognize None or null as values, instead 
			# remove fields by filtering out all ~~ values.
			# First get field sets by splitting by space into three parts (https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/)
			outline_meas, outline_field, outline_time = outline.split(' ')
			# Replace None values
			outline_field = ','.join([w for w in outline_field.split(',') if not '~~' in w])
			# Store
			parsed_lines.append(" ".join([outline_meas, outline_field, outline_time]))

		# If we found nothing (during initialization), we continue to next 
		# line (added for clarity)
		else:
			continue

	return parsed_lines

def influxdb_output(outuri, influxdata):
	if (outuri[:4].lower() == 'http'):
		r = requests.post(outuri, data="\n".join(influxdata), timeout=5)
	else:
		# Store to file
		with open(outuri, 'w+') as fdo:
			fdo.write("\n".join(influxdata))


# Parse commandline arguments
parser = argparse.ArgumentParser(description="Convert KNMI data to influxdb line protocol. Optionally insert into database directly")
parser.add_argument("knmisource", help="KNMI hourly data source. Can be path or 'KNMI' to get live data.")
parser.add_argument("outuri", help="Output target, either influxdb server (if starts with http, e.g. http://localhost:8086/write?db=smarthome&precision=s), or filename (else)")
parser.add_argument("--query", help="Query template for influxdb line protocol, where {DATETIME}=UT date in seconds since epoch, {STN}=station, {T}=temp in C, {FF}=windspeed in m/s, {DD}=wind direction in deg, {Q}=irradiance in W/m^2, {RH}=precipitation in mm, {NEWLINE} is newline, e.g. 'weather,device=knmi temp={T} wind={DD}'", default=DEFAULTQUERY)
args = parser.parse_args()

# Run script
knmidata = get_knmi_data(args.knmisource)
influxdata = convert_knmi(knmidata, args.query)
influxdb_output(args.outuri, influxdata)