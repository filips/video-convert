#!/usr/bin/python2.6

# Copyright (c) 2012 Filip Sandborg-Olsen <filipsandborg(at)gmail.com>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os, re, datetime, pprint, sys, fcntl
from metadata import writeMetadata

logDir = "/var/lib/awstats/"
webRoot = "/home/typothree/html/"

logSites = [
	"podcast.llab.dtu.dk"
]

fileTypes = ['m4v', 'mp4', 'mov']

logFiles = {}

if not logDir.endswith(os.path.sep):
    logDir += os.path.sep

def readLog(file):
	logFile = {}
	with open(file, 'r') as file:
		content = file.readlines()
		currentSec = ""
		keys = [
			"file",
			"hits",
			"hits_206",
			"bandwidth"
		]
		begin = re.compile("^BEGIN_DOWNLOADS (\d+)$")
		end = re.compile("^END_(\w+)$")
		reading = False
		for line in content:
			line = line[:-1]
			matchBegin = begin.search(line)
			matchEnd = end.search(line)
			if matchBegin:
				logFile = {}
				reading = True
			elif matchEnd and reading == True:
				break;
			elif reading == True:
				for key, el in enumerate(line.split(" ")):
					if key == 0:
						file = el
						logFile[file] = {}
					else:
						logFile[file][keys[key]] = el
	return logFile

pattern = re.compile("^awstats(\d{6})\.(" + "|".join(logSites)+ ")\.txt$")
for logFile in os.listdir(logDir):
	match = pattern.match(logFile)
	if match:
		siteStr = match.group(2)
		yearStr = match.group(1)[2:]
		monthStr = match.group(1)[:2]
		if not logFiles.get(siteStr):
			logFiles[siteStr] = {}
		if not logFiles[siteStr].get(yearStr):
			logFiles[siteStr][yearStr] = {}
		logFiles[siteStr][yearStr][monthStr] = readLog(logDir + logFile)

accumLog = {}

for site in logFiles:
	for year in logFiles[site]:
		for month in logFiles[site][year]:
			for file in logFiles[site][year][month]:
				existing = accumLog.get(file)
				if existing:
					for el in logFiles[site][year][month][file]:
						accumLog[file][el] = int(accumLog[file][el]) + int(logFiles[site][year][month][file][el])
				else:
					accumLog[file] = logFiles[site][year][month][file]

podcastInfo = {}

for file in accumLog:
	videofile = webRoot + file
	if os.path.isfile(videofile):
		tempMeta = {
			"bandwidth": accumLog[file]['bandwidth'],
			"hits": accumLog[file]['hits'],
			"hitspartial": accumLog[file]['hits_206']
		}
		nameParts = re.search('(.+)-(\w+)\.\w+$',videofile)
		if nameParts:
			basename = nameParts.group(1)
			version = nameParts.group(2)

			if not podcastInfo.get(basename):
				podcastInfo[basename] = {}

			podcastInfo[basename][version] = tempMeta

for basename in podcastInfo:
	counts = {
		"bandwidth": 0,
		"hits": 0,
		"hitspartial": 0
	}
	for version in podcastInfo[basename]:
		for value in podcastInfo[basename][version]:
			counts[str(value) + '-' + version] = podcastInfo[basename][version][value]
			counts[str(value)] += int(podcastInfo[basename][version][value])
	try:
		print("Writing metadata for " + basename)
		writeMetadata(basename + '-720p.mp4', counts)
	except IOError:
		print("Permission denied when writing metadata for " + basename)
