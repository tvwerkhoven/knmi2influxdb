#/usr/bin/env python3
#
# Convert KNMI hourly CSV data to influxdb line protocol
#

import csv
import argparse
import datetime

#DEFAULTQUERY='temperaturev2 outside_knmi{STN} wind={DD} windspeed={FF:.1f} temp={T:.1f} irrad={Q:.2f} rain={RH:.1f} {date}'
DEFAULTQUERY='temperaturev2 outside_knmi{STN}={T:.1f} {date}'

def convert_knmi(knmifile, query, outfile):
	with open(knmifile) as fdk, open(outfile, 'w+') as fdo:
		spamreader = csv.reader(fdk, delimiter=',', skipinitialspace=True)
		start = False
		for row in spamreader:
			# After we found the start marker, parse data
			if (start and len(row) > 1):
				valstn = row[0]
				valyyyymmdd = row[1]
				valhh = int(row[hhpos])
				valdd = int(row[ddpos])
				valff = float(row[ffpos])/10
				valfx = float(row[fxpos])/10
				valt = float(row[tpos])/10
				valsq = float(row[sqpos])/10
				valq = int(float(row[qpos])*10000/3600) # Convert J/cm^2/hour to W/m^2, i.e. 10000cm^2/m^2 and 1/300 hr/sec 
				valdr = float(row[drpos])/10
				valrh = int(row[rhpos]) # -1 is stupid for low values? # RH       = Uursom van de neerslag (in 0.1 mm) (-1 voor <0.05 mm); 
				if valrh == -1:
					valrh = 0
				valrh = float(valrh)/10

				# Make date. Note that although observation runs from hour-1 to hour, most measurements are taken at the end of the slot, so we simply use the value as fiven
				# NB 24 does not exist, so we subtract one and add an hour. This works usually except if there is clock change in the time from 23 to 24 hours.
				if valhh == 24:
					thisdate = datetime.datetime(int(valyyyymmdd[0:4]), int(valyyyymmdd[4:6]), int(valyyyymmdd[6:8]), hour=valhh-1) + datetime.timedelta(hours=1)
				else:
					thisdate = datetime.datetime(int(valyyyymmdd[0:4]), int(valyyyymmdd[4:6]), int(valyyyymmdd[6:8]), hour=valhh)
				# N.B. timestamp() only works in python3
				thisdate_epoch = int(thisdate.timestamp())
				outline = query.format(STN=valstn, DD=valdd, FF=valff, FX=valfx, T=valt, SQ=valsq, Q=valq, DR=valdr, RH=valrh, date=thisdate_epoch, NEWLINE="\n") + "\n"
				fdo.write(outline)

			# Find start row (like # STN,YYYYMMDD,   HH,   DD,   FH,   FF,   FX,    T,  T10,   TD,   SQ,    Q,   DR,   RH,    P,   VV,    N,    U,   WW,   IX,    M,    R,    S,    O,    Y)
			elif row[0][0] == "#" and len(row)>2 and "YYYYMMDD" in row[1]:
				try:
					hhpos = row.index("HH")
					ddpos = row.index("DD")
					ffpos = row.index("FF")
					fxpos = row.index("FX")
					tpos = row.index("T")
					sqpos = row.index("SQ")
					qpos = row.index("Q")
					drpos = row.index("DR")
					rhpos = row.index("RH")
				except ValueError:
					quit("KNMI data file incompatible, could not find fields HH, DD or others.")
				start = True
				#print hhpos, ddpos, ffpos, tpos, qpos, rhpos
			
			# If we found nothing, we continue to next line (added for clarity)
			else:
				continue

# Parse commandline arguments
parser = argparse.ArgumentParser(description="Convert KNMI data to influxdb line protocol")
parser.add_argument("knmifile", help="KNMI hourly data file")
parser.add_argument("outfile", help="Output filename")
parser.add_argument("--query", help="Query template for influxdb line protocol, where {date}=UT date in seconds since epoch, {STN}=station, {T}=temp in C, {FF}=windspeed in m/s, {DD}=wind direction in deg, {Q}=irradiance in W/m^2, {RH}=precipitation in mm, e.g. 'weather,device=knmi temp={T} wind={DD}'", default=DEFAULTQUERY)
args = parser.parse_args()

convert_knmi(args.knmifile, args.query, args.outfile)
