#!/usr/bin/env python3
#
# # About
# 
# Convert KNMI hourly CSV data to influxdb line protocol.
# 
# Input can be either a KNMI data file, or the script can query data directly
# Output can be either a file, or a URI to an influxdb server to push data directly.
#
# # Quick start
#
# /usr/bin/python3 knmi2influxdb.py --time actual --station 260
# 
# # References
#
# - https://www.knmi.nl/kennis-en-datacentrum/achtergrond/data-ophalen-vanuit-een-script
# - http://projects.knmi.nl/klimatologie/uurgegevens/
# - https://data.knmi.nl/download/Actuele10mindataKNMIstations/1
#
# 
# # KNMI stations
# id       name
# 210      Valkenburg
# 215      Voorschoten
# 225      IJmuiden
# 235      De Kooy
# 240      Schiphol
# 242      Vlieland
# 249      Berkhout
# 251      Hoorn (Terschelling)
# 257      Wijk aan Zee
# 258      Houtribdijk
# 260      De Bilt
# 265      Soesterberg
# 267      Stavoren
# 269      Lelystad
# 270      Leeuwarden
# 273      Marknesse
# 275      Deelen
# 277      Lauwersoog
# 278      Heino
# 279      Hoogeveen
# 280      Eelde
# 283      Hupsel
# 286      Nieuw Beerta
# 290      Twenthe
# 310      Vlissingen
# 319      Westdorpe
# 323      Wilhelminadorp
# 330      Hoek van Holland
# 340      Woensdrecht
# 344      Rotterdam
# 348      Cabauw
# 350      Gilze-Rijen
# 356      Herwijnen
# 370      Eindhoven
# 375      Volkel
# 377      Ell
# 380      Maastricht
# 391      Arcen 

import urllib.request
import requests
import csv
import argparse
import datetime
import netCDF4
import logging
import logging.handlers
import time

#DEFAULTQUERY='test,src=outside_knmi{STN} wind={DD},windspeed={FF:.1f},temp={T:.1f},irrad={Q:.2f},rain={RH:.1f} {DATETIME}'
#DEFAULTQUERY='temperaturev2 outside_knmi{STN}={T:.1f} {DATETIME}{NEWLINE}weatherv2 rain_duration_knmi{STN}={DR:.1f},rain_qty_knmi{STN}={RH:.1f},wind_speed_knmi{STN}={FF:.1f},wind_gust_knmi{STN}={FX:.1f},wind_dir_knmi{STN}={DD} {DATETIME}{NEWLINE}energyv2 irradiance_knmi{STN}={Q:.0f} {DATETIME}'
DEFAULTQUERY='temperaturev2 outside_knmi{STN}={T:.1f} {DATETIME}'
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
			else: 
				my_logger.exception("Exception occurred in format_field()")
				raise

def get_knmi_data_historical(knmistation=KNMISTATION, histrange=(21,)):
	my_logger.debug("get_knmi_data_historical(knmistation={}, histrange={})".format(knmistation, histrange))
	if len(histrange) == 1:
		histdays = int(histrange[0])
		# Get data from last 21 days by default
		histstart = datetime.datetime.now() - datetime.timedelta(days=histdays)
		knmiquery = "start={}01&vars=ALL&stns={}".format(histstart.strftime("%Y%m%d"), knmistation)
	elif len(histrange) == 2:
		histstart, histend = histrange
		# Try parsing histrange to see if formatting is OK (if strptime is happy, KNMI should be happy)
		try:
			a = (time.strptime(histstart,"%Y%m%d"))
			b = (time.strptime(histend,"%Y%m%d"))
		except:
			my_logger.exception("Exception occurred")
			raise ValueError("histrange formatting not OK, should be YYYYMMDD")
		knmiquery = "start={}01&end={}24&vars=ALL&stns={}".format(histstart, histend, knmistation)
	else:
		my_logger.exception("Exception occurred")
		raise ValueError("histrange should be either [days] or [start, end] and thus have 1 or 2 elements.")
	
	knmiuri = 'http://projects.knmi.nl/klimatologie/uurgegevens/getdata_uur.cgi'
	my_logger.info("get_knmi_data_historical(): getting query={}".format(knmiquery))

	# Query can take quite long, set long-ish timeout
	r = requests.post(knmiuri, data=knmiquery, timeout=30)

	# Return line-wise iterable for next stage
	return r.text.splitlines()

def get_knmi_data_actual(api_key, knmistation=KNMISTATION, query=DEFAULTQUERY):
	my_logger.debug("get_knmi_data_actual(knmistation={}, query={})".format(knmistation, query))
	# Get real-time data from now, store to disk (netCDF https support is limited)

	# Old approach (deprecated)
	# Latest:        https://data.knmi.nl/download/Actuele10mindataKNMIstations/1/noversion/2020/01/08/KMDS__OPER_P___10M_OBS_L2.nc
	# Specific time: https://data.knmi.nl/download/Actuele10mindataKNMIstations/1/noversion/2020/01/08/KMDS__OPER_P___10M_OBS_L2_1620.nc
	# https://stackoverflow.com/questions/22676/how-do-i-download-a-file-over-http-using-python/22776#22776

	# utcnow = datetime.datetime.utcnow()
	# URI = "https://data.knmi.nl/download/Actuele10mindataKNMIstations/1/noversion/{year}/{month:02}/{day:02}/KMDS__OPER_P___10M_OBS_L2.nc".format(year=utcnow.year, month=utcnow.month, day=utcnow.day)
	# urllib.request.urlretrieve(URI, "/tmp/KMDS__OPER_P___10M_OBS_L2.nc")

	# New approach 20200800: https://developer.dataplatform.knmi.nl/portal/example-scripts#list-10-files
	# Get latest file with data
	api_url = "https://api.dataplatform.knmi.nl/open-data"
	dataset_name = "Actuele10mindataKNMIstations"
	dataset_version = "2"

	# Get 10 files since one hour ago, which should show the latest 6 files. We take the last file of this, which should be the newest. Guaranteed to work if files are available max 1 hour later
	timestamp_now = datetime.datetime.utcnow()
	timestamp_one_hour_ago = timestamp_now - datetime.timedelta(hours=1)
	filename_one_hour_ago = f"KMDS__OPER_P___10M_OBS_L2_{timestamp_one_hour_ago.strftime('%Y%m%d%H%M')}.nc"
	list_files_response = requests.get(f"{api_url}/datasets/{dataset_name}/versions/{dataset_version}/files",
		headers={"Authorization": api_key},
		params={"maxKeys": 10, "startAfterFilename": filename_one_hour_ago})
	list_files = list_files_response.json()
	filename = list_files.get("files")[-1].get("filename")

	# Get latest file by constructing filename ourselves. Could fail if there's a delay before the files are available.
	# timestamp_now = datetime.datetime.utcnow()
	# timestamp_one_hour_ago = timestamp_now - timedelta(hours=1) - datetime.timedelta(minutes=timestamp_now.minute % 10)
	# filename = f"KMDS__OPER_P___10M_OBS_L2_{timestamp_one_hour_ago.strftime('%Y%m%d%H%M')}.nc"

	# Get data file
	endpoint = f"{api_url}/datasets/{dataset_name}/versions/{dataset_version}/files/{filename}/url"
	my_logger.debug(f"get_knmi_data_actual: getting {endpoint}")

	get_file_response = requests.get(endpoint, headers={"Authorization": api_key})
	download_url = get_file_response.json().get("temporaryDownloadUrl")
	dataset_file = requests.get(download_url,)
	urllib.request.urlretrieve(download_url, "/tmp/KMDS__OPER_P___10M_OBS_L2.nc")

	rootgrp = netCDF4.Dataset("/tmp/KMDS__OPER_P___10M_OBS_L2.nc", "r", format="NETCDF4")

	# Data file contains the following variables:
	# for k, v in rootgrp.variables.items():
	# 	try:
	# 		print ("{}, unit: {}, name: {}".format(k,v.units,v.long_name))
	# 	except:
	# 		print(k)
	# station
	# time, unit: seconds since 1950-01-01 00:00:00, name: time of measurement
	# stationname
	# lat, unit: degrees_north, name: station  latitude
	# lon, unit: degrees_east, name: station longitude
	# height, unit: m, name: Station height
	# dd, unit: degree, name: Wind Direction 10 Min Average
	# ff, unit: m s-1, name: Wind Speed at 10m 10 Min Average
	# gff, unit: m s-1, name: Wind Gust at 10m 10 Min Maximum
	# ta, unit: degrees Celsius, name: Air Temperature 1 Min Average
	# rh, unit: %, name: Relative Humidity 1 Min Average
	# pp, unit: hPa, name: Air Pressure at Sea Level 1 Min Average
	# zm, unit: m, name: Meteorological Optical Range 10 Min Average
	# D1H, unit: min, name: Rainfall Duration in last Hour
	# dr, unit: sec, name: Precipitation Duration (Rain Gauge) 10 Min Sum
	# hc, unit: ft, name: Cloud Base
	# hc1, unit: ft, name: Cloud Base First Layer
	# hc2, unit: ft, name: Cloud Base Second Layer
	# hc3, unit: ft, name: Cloud Base Third Layer
	# nc, unit: octa, name: Total cloud cover
	# nc1, unit: octa, name: Cloud Amount First Layer
	# nc2, unit: octa, name: Cloud Amount Second Layer
	# nc3, unit: octa, name: Cloud Amount Third Layer
	# pg, unit: mm/h, name: Precipitation Intensity (PWS) 10 Min Average
	# pr, unit: sec, name: Precipitation Duration (PWS) 10 Min Sum
	# qg, unit: W m-2, name: Global Solar Radiation 10 Min Average
	# R12H, unit: mm, name: Rainfall in last 12 Hours
	# R1H, unit: mm, name: Rainfall in last Hour
	# R24H, unit: mm, name: Rainfall in last 24 Hours
	# R6H, unit: mm, name: Rainfall in last 6 Hours
	# rg, unit: mm/h, name: Precipitation Intensity (Rain Gauge) 10 Min Average
	# ss, unit: min, name: Sunshine Duration
	# td, unit: degrees Celsius, name: Dew Point Temperature 1.5m 1 Min Average
	# tgn, unit: degrees Celsius, name: Grass Temperature 10cm 10 Min Minimum
	# Tgn12, unit: degrees Celsius, name: Grass Temperature Minimum last 12 Hours
	# Tgn14, unit: degrees Celsius, name: Grass Temperature Minimum last 14 Hours
	# Tgn6, unit: degrees Celsius, name: Grass Temperature Minimum last 6 Hours
	# tn, unit: degrees Celsius, name: Ambient Temperature 1.5m 10 Min Minimum
	# Tn12, unit: degrees Celsius, name: Air Temperature Minimum last 12 Hours
	# Tn14, unit: degrees Celsius, name: Air Temperature Minimum last 14 Hours
	# Tn6, unit: degrees Celsius, name: Air Temperature Minimum last 6 Hours
	# tx, unit: degrees Celsius, name: Ambient Temperature 1.5m 10 Min Maximum
	# Tx12, unit: degrees Celsius, name: Air Temperature Maximum last 12 Hours
	# Tx24, unit: degrees Celsius, name: Air Temperature Maximum last 24 Hours
	# Tx6, unit: degrees Celsius, name: Air Temperature Maximum last 6 Hours
	# ww, unit: code, name: wawa Weather Code
	# pwc, unit: code, name: Present Weather
	# ww-10, unit: code, name: wawa Weather Code for Previous 10 Min Interval
	# ts1, unit: Number, name: Number of Lightning Discharges at Station
	# ts2, unit: Number, name: Number of Lightning Discharges near Station
	# iso_dataset
	# product, unit: 1, name: ADAGUC Data Products Standard
	# projection

	# Get number of stations, then get station by station id.
	nstations = rootgrp.dimensions['station'].size
	stationid = [(rootgrp["/station"][i]) for i in range(nstations)].index("06"+str(knmistation))

	fieldval = {'NEWLINE':"\n"}
	# time units is: seconds since 1950-01-01 00:00:00
	naivetime = netCDF4.num2date(rootgrp["/time"][:], rootgrp["/time"].units)[0]
	# Cumbersome way to make into utc timestamp
	obstime = datetime.datetime(naivetime.year, naivetime.month, naivetime.day, naivetime.hour, naivetime.minute, tzinfo=datetime.timezone.utc)
	fieldval['DATETIME'] = int(obstime.timestamp())
	# tzinfo=datetime.timezone.utc
	fieldval['STN'] = knmistation
	fieldval['T'] = rootgrp["/ta"][stationid][0]
	fieldval['FF'] = rootgrp["/ff"][stationid][0]
	fieldval['FX'] = rootgrp["/gff"][stationid][0]
	fieldval['DD'] = rootgrp["/dd"][stationid][0]
	fieldval['Q'] = rootgrp["/qg"][stationid][0]
	# fieldval['SQ'] = rootgrp["/gq"][stationid]
	# fieldval['DR'] = rootgrp["/dr"][stationid][0]/600. # seconds to fraction
	fieldval['DR'] = rootgrp["/D1H"][stationid][0]/60. # minutes to fraction
	fieldval['RH'] = rootgrp["/R1H"][stationid][0]
	fieldval['P'] = rootgrp["/pp"][stationid][0]

	fmt = PartialFormatter()
	outline = fmt.format(query, **fieldval)

	# Return as array so we're compatible with convert_knmi() format (which 
	# returns multiple lines)
	return [outline]

def convert_knmi(knmidata, query):
	my_logger.debug("convert_knmi(knmidata, query={})".format(query))
	start = False
	fieldpos = {}
	fieldval = {'NEWLINE':"\n"}
	fieldfunc = {
		'YYYYMMDD': lambda x: datetime.datetime(int(x[0:4]), int(x[4:6]), int(x[6:8]), tzinfo=datetime.timezone.utc), ## time
		'HH': lambda x: int(x),
		'DD': lambda x: int(x),
		'FF': lambda x: int(x)/10,
		'FX': lambda x: int(x)/10,
		'T':  lambda x: int(x)/10,
		'SQ': lambda x: int(x)/10,
		'Q':  lambda x: int(x)*10000/3600, # Convert J/cm^2/hour to W/m^2, i.e. 10000cm^2/m^2 and 1/3600 hr/sec 
		'DR': lambda x: int(x)/10,
		'RH': lambda x: max(int(x),0)/10, # RH is values? From their doc: RH       = Uursom van de neerslag (in 0.1 mm) (-1 voor <0.05 mm); 
		'P':  lambda x: int(x)/10,
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
				fieldpos['P'] = row.index("P")
			except ValueError as e:
				my_logger.exception("KNMI data file incompatible, could not find fields HH, DD or others")
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
			# slot 1 runs from 00:00 to 01:00: 'Uurvak 05 loopt van 04.00 UT 
			# tot 5.00 UT'), most measurements are taken at the end of the
			# slot, so we use that timestamp as given, which is good enough.
			# N.B. HH runs from 1-24, so we can't make a time directly 
			# (which runs from 0-23) for hour 24, so instead we add as 
			# timedelta to allow for wrapping over days (e.g. 1->1, 2->2, 
			# but 24->0 next day)
			# N.B. timestamp() only works in python3
			fieldval['DATETIME'] = int((fieldval['YYYYMMDD'] + datetime.timedelta(hours=fieldval['HH'])).timestamp())

			# Unpack dict to format query to give influxdb line protocol "value=X"
			# See https://github.com/influxdata/docs.influxdata.com/issues/717#issuecomment-249618099
			outline = fmt.format(query, **fieldval)
			# Influxdb does not recognize None or null as values, instead 
			# remove fields by filtering out all ~~ values given by PartialFormatter().
			outline_fix = []
			for l in outline.split('\n'):
				# First get field sets by splitting by space into three parts (https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/)
				try:
					outline_meas, outline_field, outline_time = l.split(' ')
				except:
					my_logger.exception("Could not unpack line: {}".format(l))

				# Replace None values
				outline_field = ','.join([w for w in outline_field.split(',') if not '~~' in w])
				outline_fix.append(" ".join([outline_meas, outline_field, outline_time]))

			# Store
			parsed_lines.append("\n".join(outline_fix))

		# If we found nothing (during initialization), we continue to next 
		# line (added for clarity)
		else:
			continue

	return parsed_lines

def influxdb_output(outuri, influxdata):
	my_logger.debug("influxdb_output(outuri={}, influxdata)".format(outuri))
	if (outuri[:4].lower() == 'http'):
		r = requests.post(outuri, data="\n".join(influxdata), timeout=10)
		if r.status_code == 204:
			my_logger.debug("Query successfully handed to influxdb.")
		else:
			my_logger.error("Could not push to influxdb: {} - {}".format(r.status_code, r.content))
	else:
		# Store to file
		with open(outuri, 'w+') as fdo:
			fdo.write("\n".join(influxdata))

# Init logger, defaults to console
my_logger = logging.getLogger("MyLogger")
my_logger.setLevel(logging.DEBUG)

# create syslog handler which also shows filename in log
handler_syslog = logging.handlers.SysLogHandler(address = '/dev/log')
formatter = logging.Formatter('%(filename)s: %(message)s')
handler_syslog.setFormatter(formatter)
handler_syslog.setLevel(logging.INFO)
my_logger.addHandler(handler_syslog)

my_logger.debug("Init logging & parsing command line args.")

# Parse commandline arguments
parser = argparse.ArgumentParser(description="Convert KNMI data to influxdb line protocol. Optionally insert into database directly")
parser.add_argument("--time", choices=['actual', 'historical'], help="Get actual (default, updated in 10-min interval) or historical (hourly, updated daily) data. ", default='actual')
parser.add_argument("--histrange", help="Time range to get historical data for. Either days since now (if one parameter), or timerange in format of YYYYMMDD (if two parameters)", nargs="*", default=['21'])
parser.add_argument("--station", help="""KNMI station (default: de Bilt). Possible values:
	210: Valkenburg
	215: Voorschoten
	225: IJmuiden
	235: De Kooy
	240: Schiphol
	242: Vlieland
	249: Berkhout
	251: Hoorn (Terschelling)
	257: Wijk aan Zee
	258: Houtribdijk
	260: De Bilt
	265: Soesterberg
	267: Stavoren
	269: Lelystad
	270: Leeuwarden
	273: Marknesse
	275: Deelen
	277: Lauwersoog
	278: Heino
	279: Hoogeveen
	280: Eelde
	283: Hupsel
	286: Nieuw Beerta
	290: Twenthe
	310: Vlissingen
	319: Westdorpe
	323: Wilhelminadorp
	330: Hoek van Holland
	340: Woensdrecht
	344: Rotterdam
	348: Cabauw
	350: Gilze-Rijen
	356: Herwijnen
	370: Eindhoven
	375: Volkel
	377: Ell
	380: Maastricht
	391: Arcen""", default=260)
parser.add_argument("--api_key", help="KNMI opendata api key, required for actuals.")
parser.add_argument("--outuri", help="Output target, either influxdb server (if starts with http, e.g. http://localhost:8086/write?db=smarthome&precision=s), or filename (else)")
parser.add_argument("--query", help="Query template for influxdb line protocol, where {DATETIME}=UT date in seconds since epoch, {STN}=station, {T}=temp in C, {FF}=windspeed in m/s, {FX}=windgust in m/s, {DD}=wind direction in deg, {Q}=irradiance in W/m^2, {RH}=precipitation in mm, {NEWLINE} is newline, e.g. 'weather,device=knmi temp={T} wind={DD}'", default=DEFAULTQUERY)
args = parser.parse_args()

logging.debug("Got command line args:" + str(args))
influxdata=None
if (args.time == 'historical'):
	knmidata = get_knmi_data_historical(args.station, args.histrange)
	influxdata = convert_knmi(knmidata, args.query)
else:
	if (not args.api_key):
		logging.error("Need apikey for actual data query.")
	influxdata = get_knmi_data_actual(args.api_key, args.station, args.query)

if (args.outuri):
	influxdb_output(args.outuri, influxdata)
else:
	print (influxdata)
# Run for live data