#!/usr/bin/python2.6
# -*- coding: utf-8 -*-

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

import os,re,time,subprocess,pprint,sys
import simplejson as json
import shutil
import datetime
import threading
import gdata.youtube
import gdata.youtube.service
from collections import deque
import smtplib
from termcolor import colored
import textwrap
import locale
import Image, ImageFont, ImageDraw, ImageChops
import fcntl

##########################################
########### CONSTANTS ####################
##########################################

#NO trailing slash on podcastPath!!
podcastPath = "/home/typothree/podcasts"
scriptDir = "/home/typothree/VideoParts/"
HandBrakeCLI = "/home/typothree/prefix/bin/HandBrakeCLI"

# YouTube parameters to be passed if metadata is missing
defaultYoutubeCategory = "Education"
defaultYoutubeKeywords = "Education"

# Default intro and outro video in case branding is specified, but no custom files are found
defaultIntro = "z:\\home\\typothree\\VideoParts\\intro.mov"
defaultOutro = "z:\\home\\typothree\\VideoParts\\outro.mov"

rawSuffix = "raw" # Used to be 720p

CPUS = 4
NICENESS = 15

# Number of simultaneous conversion threads
conversionThreads = 2

# List of possible video file types
fileTypes = ["mp4", "m4v", "mov"]


fullLog = open(scriptDir + "full.log", 'a')
infoLog = open(scriptDir + "info.log", 'a')

fileList = []
structure = {}
conversionList = []

# The drive letter in Wine correlating to linux "/"
wineDrive = "z"

# Video framerate. Should be configurable on a per-channel basis
fps = 25 

# Locale primarily used in date formats. Should be customized in metadata
locale.setlocale(locale.LC_ALL, "da_DK.UTF-8")

## INTERNAL ##

# Don't change
logging = True
pendingLog = []
mainThreadDone = False
conversionObjs = []

##########################################
########### STANDALONE METHODS ###########
##########################################

def scanStructure():
	_structure = {}
	_fileList = []
	log("Scanning " + podcastPath +  " for movie files...")
	for root, subFolders, files in os.walk(podcastPath):
		path = root[len(podcastPath):].split('/')
		lastStructure = _structure
		for part in path:
			if part not in lastStructure:
				lastStructure[part] = {"structure": {}, "config": ""}
			last = lastStructure[part]
			lastStructure = lastStructure[part]["structure"]
		for file in files:
			if file == "convertConfig.json":
				configFile = open(os.path.join(root,file),'r').read()
				try:
					config = json.loads(configFile)
				except ValueError:
					log("Error importing config file in '" + root + "'. Not doing *any* conversion beyond this path.", 'red')
					last['config'] = False
				else:
					last['config'] = config
			if time.time() - os.stat(os.path.join(root,file)).st_ctime < 10:
				log("Skipped " + file + ", too new", 'yellow')
			else:
				_fileList.append(os.path.join(root,file))
	return _fileList, _structure

def getRawFiles():
	rawFiles = {}
	patterns = {}
	patterns[rawSuffix] = re.compile("^.+\/([^\/]+)-"+rawSuffix+"(\d)?\.([^\.^-]+)$")
	for path in fileList:
		localConfig = getConfig(path)
		localSuffix = localConfig.get('rawSuffix')
		if localSuffix:
			if not patterns.get(localSuffix):
				patterns[localSuffix] =  re.compile("^.+\/([^\/]+)-"+localSuffix+"(\d)?\.([^\.^-]+)$")
			parts = patterns[localSuffix].search(path)
		else:
			parts = patterns[rawSuffix].search(path)
		if parts:
			if not parts.group(1) in rawFiles:
				rawFiles[parts.group(1)] = {}

			index = parts.group(2)
			if not index:
				index = 0

			rawFiles[parts.group(1)][int(index)] = parts.group(0)
	return rawFiles

def checkFiles():
	global structure, rawFiles, fileList, conversionQueue, filesAddedYoutube, filesAdded

	#print "Acquiring lock"
	#conversionQueueLock.acquire()
	#print "Acquired lock"
	fileList, structure = scanStructure()
	rawFiles = getRawFiles()

	pattern = re.compile("^.+\/([^\/]+)-([a-zA-Z0-9]+)\.([^\.^-]+)$")
	for file in fileList:
		parts = pattern.search(file)
		if parts:
			data = parts.group(0,1,2,3)
			basename,name,quality,ext = data
			if ext in fileTypes:
				localConfig = getConfig(file)
				localSuffix = localConfig.get('rawSuffix')
				if not localSuffix:
					localSuffix = rawSuffix
				if quality == localSuffix and file not in filesAdded:
					# filesAdded.append(file)
					#if not firstLoop:
					#	log("Found new raw file, " + file, 'green')
					config = getConfig(file)
					metadata = youtubeUpload.getMetadata(file)
					if not metadata:
						log("Missing metadata for file " + file, 'red')
						continue
					if metadata.get('quarantine') == "true":
						log(file+" is in quarantine!", "red")
						continue

					# Check if videos are to be uploaded to YouTube
					if config.get('youtubeUpload') == True and not metadata.get('enotelms:YouTubeUID') and not file in filesAddedYoutube:
						filesAddedYoutube.append(file)
						youtube = config.get('youtube');
						if youtube.get('uploadVersion'):
							version = youtube.get('uploadVersion')
						else:
							version = "720p"
						config['youtube']['uploadVersion'] = version
						if versionExists(file, localSuffix, version):
							username = youtube.get('username')
							password = youtube.get('password')
							developer_key = youtube.get('developerKey')
							playlist = youtube.get('playlist')
							if username and password and developer_key:
								filename = file.replace(localSuffix, version)
								youtubeUpload.addToQueue(filename, youtube.copy())
					# Check if videos are to be converted
					if config.get("convert") == True:
						for format in config.get('formats'):
							preset = config.get('presets').get(format)
							if preset:
								if not versionExists(file, localSuffix, preset.get('suffix')):
									if not jobQueued(file, preset.get('suffix')):
										log("Added " + file + " in version " + format + " to queue")
										conversionJob = {"path": data, "files": rawFiles[data[1]], "options": preset,"preset": format, "config": config, "rawSuffix": localSuffix, "priority": config['presets'][format].get('priority')}
										conversionQueueLock.acquire()
										conversionQueue.append(conversionJob)
										conversionQueueLock.release()
							else:
								log("Format '" + format + "' not found. Available ones are (" + ', '.join(format for format in config.get('presets')) + ")")
					# Generate thumbnails if missing
					thumbnail = re.sub("-" + localSuffix + "\.(" + "|".join(fileTypes) + ")", "-1.png", file)
					if not os.path.isfile(thumbnail):
						info = vidConv.videoInfo(file)
						try:
							videoConvert.executeCommand("ffmpeg -ss "+str(info['length']/3)+" -i '"+file+"' -vframes 1 -s 640x360 '"+thumbnail+"'")
						except executeException:
							log("Error generating thumbnail: " + thumbnail, 'red')
						else:
							log("Generated thumbnail: " + thumbnail , 'green')
	conversionQueue = sorted(conversionQueue, key=lambda k: k.get('priority'), reverse=True)

def log(string, color="white"):
	now = datetime.datetime.now()
	date = "[" + str(now)[:19] + "] "
	message = str(string)
	logstr = date + message
	logstr = colored(date, 'cyan') + colored(message, color)
	if logging:
		pendingLog.append({"message": logstr, "color": color})
		while len(pendingLog) > 0:
			nextEntry = pendingLog.pop(0)
			if nextEntry['color'] in ["green", "white"]:
				infoLog.write(nextEntry['message'] + "\n")
				infoLog.flush()
			fullLog.write(nextEntry['message'] + "\n")
			fullLog.flush()
	else:
		pendingLog.append(logstr)
	print colored(date, 'cyan'), colored(message, color)

# Find out if another instance of the script is running
def isRunning():
	pidfile="/tmp/convert.pid"
	try:
		pid = open(pidfile,'r').read().strip()
		if len(pid) == 0:
			raise IOError
		else:
			cmdline = open('/proc/' + str(pid) + '/cmdline','r').readline().strip("\0")
			if cmdline.endswith(sys.argv[0]):
				return True
			else:
				raise IOError
	except IOError:
		fp = open(pidfile, 'w')
		fp.write(str(os.getpid()))
		fp.close()
		return False

def anyVersionExists(file, rawSuffix):
	basename = re.split('-\w+\.\w+$',file)[0]
	for file in fileList:
		if re.match(basename + "-(^(!?"+rawSuffix+")\w+)\.("+"|".join(fileTypes)+")", file):
			return True
	return False
def versionExists(file, localSuffix, suffix):
	for filetype in fileTypes:
		filename = re.sub(localSuffix+".+$",suffix, file) + "." + filetype
		if filename in fileList:
			return True
		elif os.path.isfile(filename) and os.stat(filename).st_ctime >= 10:
			return True
	return False

def jobQueued(file, suffix):
	for job in conversionQueue:
		if job['path'][0] == file and job['preset'] == suffix:
			return True
	if file in currentlyProcessing():
		return True
	return False

# Get the complete config array for a file located at "file"
def getConfig(file):
	parts = file[len(podcastPath):].split('/')[:-1]
	config = {}
	parent = structure
	for i in parts:
		try:
			config = dict(config.items() + parent[i]['config'].items())
		except AttributeError:
			# Check if there are configuration errors
			if isinstance(parent[i]['config'], bool) and parent[i]['config'] == False:
				return {"convert": False}	
		parent = parent[i]['structure']
	return config

# Get a list of items missing from a list
def validateList(options, nonOptional):
	missing = []
	for opt in nonOptional:
		if not options.get(opt):
			missing.append(opt)
	return missing

# Delivery of error reports to a configured email adress
def sendErrorReport(filename, email):
	SERVER = "localhost"
	FROM = "noreply@podcast.llab.dtu.dk"
	TO = [email]

	SUBJECT = "Encoding failed!"

	TEXT = "Encoding failed for file: " + filename

	message = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s" % (FROM, ", ".join(TO), SUBJECT, TEXT)

	server = smtplib.SMTP(SERVER)
	server.sendmail(FROM, TO, message)
	server.quit()
	time.sleep(5)

def currentlyProcessing():
	converting = []
	for thread in conversionObjs:
		status = thread.currentlyConverting
		if status != False:
			converting.append(status)
	return converting
def acquireLock():
	start = time.time()
	conversionQueueLock.acquire()
	print "Acquire time: " + str(time.time() - start)

##########################################
########### CUSTOM EXCEPTIONS ############
##########################################

class metadataException(Exception):
	pass

class executeException(Exception):
	pass

##########################################
########### VIDEOCONVERT #################
##########################################

class videoConvert(threading.Thread):
	currentlyConverting = False
	def __init__(self):
		threading.Thread.__init__(self)
	def run(self):
		while not mainThreadDone or conversionQueue.__len__() > 0 or len(currentlyProcessing()) > 0:
			checkFiles()
			if conversionQueue.__len__() > 0:
				error = False
				with conversionQueueLock:
					if conversionQueue[0]['path'][0] not in currentlyProcessing() and not (anyVersionExists(conversionQueue[0]['path'][0], conversionQueue[0]['rawSuffix']) and len(currentlyProcessing()) > 0):
						log("Currently "+str(conversionQueue.__len__()) + " items queued for conversion.")
						element = conversionQueue.pop(0)
						self.currentlyConverting = element['path'][0]
					else:
						# Rotate conversion queue
						print "Waiting..."
						conversionQueue.append(conversionQueue.pop(0))
						time.sleep(2)
						continue
				youtubeUpload.writeMetadata(element['path'][0], {"conversion": "active"})
				resolutionError = False
				for key in element['files']:
					info = self.videoInfo(element['files'][key])
					if not info:
						error = True
						log("Error getting video info for " + element['files'][key], "red")
						break
					numStreams = info['videoStreams']
					width, height = info['width'], info['height']
					if (width, height) != (1280, 720):
						log("Videofile " + element['files'][key] + " has resolution " + str(width) + "x" + str(height) + " and was quarantined!", 'red')
						youtubeUpload.writeMetadata(element['path'][0], {"quarantine": "true"})
						resolutionError = True
				if not resolutionError and not error:
					if numStreams == 1:
						if self.handleConversionJob(element):

							youtubeUpload.writeMetadata(element['path'][0], {"conversion": False})

							# Adding job to youtubeUpload's queue. Should probably be handled by a watcher thread instead
							youtubeConfig = element['config'].get("youtube")
							destination = re.sub(element['rawSuffix'], element['options']['suffix'],element['path'][0])
							if youtubeConfig and element['config'].get('youtubeUpload') == True:
								if element.get("preset") == youtubeConfig.get("uploadVersion"):
									youtubeUpload.addToQueue(destination, youtubeConfig)
						else:
							error = True
							if element['config'].get("contactEmail"):
								sendErrorReport(element['path'][0], element['config'].get('contactEmail'))
					elif numStreams == False:
						log("Couldn't get video information for " + element['path'][0] + " , skipping!")
						error = True
					else:
						log("Video contains " + str(numStreams) + " videostrems, and was quarantined! ("+element['path'][0]+")", 'red')
						youtubeUpload.writeMetadata(element['path'][0], {"quarantine": "true"})
						error = True

				if error or resolutionError:
					youtubeUpload.writeMetadata(element['path'][0], {"conversion": "failed"})
			self.currentlyConverting = False
			time.sleep(0.5)
		log("Main thread exited, terminating videoConvert...")

	def generateIntroOverlay(self, title, course, date,file):
		import Image, ImageDraw, ImageFont

		width, height = 1280, 720
		titleOffsetX = 340

		img = Image.new("RGBA", (width, height), (0,0,0,0))
		draw = ImageDraw.Draw(img)
		titleFont = "/home/typothree/.fonts/NeoSansStd-Medium.otf"
		titleFontEl = ImageFont.truetype(titleFont, 40, encoding='unic')
		regularFont = "/home/typothree/.fonts/NeoSansStd-Regular.otf"

		titleSize = titleFontEl.getsize(title)

		maxWidth = width - titleOffsetX - 175

		titleSegments = []
		tempSegment = ""
		for word in title.split():
			if titleFontEl.getsize(tempSegment + word)[0] <= maxWidth:
				tempSegment = tempSegment + word + " "
			elif titleFontEl.getsize(word)[0] <= maxWidth:
				if len(tempSegment) > 0:
					titleSegments.append(tempSegment)
				tempSegment = word + " "
			else:
				if len(tempSegment) > 0:
					titleSegments.append(tempSegment)
				tempWord = ""
				for letter in word:
					if titleFontEl.getsize(tempWord + letter)[0] <= maxWidth:
						tempWord = tempWord + letter
					else:
						titleSegments.append(tempWord)
						tempWord = letter
				tempSegment = tempWord
		titleSegments.append(tempSegment)

		date = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M").strftime("%e. %B %Y")

		strings = []
		for key,part in enumerate(titleSegments):
			strings.append([(titleOffsetX, 430 + 45*key), part.upper(), (27,65,132,255), 40, titleFont])
		strings.append([(titleOffsetX, 400), course.upper(), (27,65,132,255), 24, regularFont])
		strings.append([(-20,-1), date, (204,204,204,255), 24, regularFont])

		self.drawText((width,height), strings).save(file)

	def generateOutroOverlays(self, producer, technician, lecturer, year, file):
		import Image, ImageDraw, ImageFont

		width, height = 1280, 720
		titleOffsetX = 340

		font = "/home/typothree/.fonts/NeoSansStd-Regular.otf"
		fontsize = 44
		textcolor = (164,164,164,255)

		copyright = u"\u00A9" + " "+year+" Danmarks Tekniske Universitet"
		self.drawText((width, height), [[(-50, -40), copyright, textcolor, fontsize, font]]).save(file + "1.png")

		strings = []

		ypos = 200
		if(lecturer):
			text = u"Forelæser: " + lecturer
			strings.append([(70, ypos), text, textcolor, fontsize-2, font])
			ypos += fontsize
		if(technician):
			text = "Teknik: " + technician
			strings.append([(70, ypos), text, textcolor, fontsize-2, font])
			ypos += fontsize
		if(producer):
			text = "Producer: " + producer
			strings.append([(70, ypos), text, textcolor, fontsize-2, font])
			ypos += fontsize

		self.drawText((width, height), strings).save(file + "2.png")

	def drawText(self, imsize, strings):
		# Inspired by
		# http://nedbatchelder.com/blog/200801/truly_transparent_text_with_pil.html

		alpha = Image.new("L", imsize, "black")
		img = Image.new("RGBA", imsize, (0,0,0,0))
		
		for pos, text, color, size, font in strings:
			imtext = Image.new("L", imsize, 0)
			draw = ImageDraw.Draw(imtext)
			font = ImageFont.truetype(font, size, encoding='unic')
			(offset_x, offset_y) = pos
			if offset_x < 0:
				offset_x = imsize[0] - font.getsize(text)[0] + pos[0]
			if offset_y < 0:
				pass
				offset_y = imsize[1] - font.getsize(text)[1] + pos[1]

			draw.text((offset_x, offset_y), text, font=font, fill="white")

			alpha = ImageChops.lighter(alpha, imtext)
			solidcolor = Image.new("RGBA", imsize, color)
			immask = Image.eval(imtext, lambda p: 255 * (int(p != 0)))
			img = Image.composite(solidcolor, img, immask)

		img.putalpha(alpha)

		return img
	@staticmethod
	def executeCommand(cmd,niceness=False):
		if niceness:
			cmd = 'nice -n '+str(NICENESS)+' sh -c "' + cmd.replace('"','\'') + '"'
		process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		#stdout, stderr = process.communicate()
		stdout = ''
		while True:
			output = process.stdout.read(1)
			if output == '' and process.poll() != None:
				break
			if output != '':
				if len(sys.argv) > 1 and sys.argv[1] == '-v':
					sys.stdout.write(output)
				sys.stdout.flush()
				stdout += output

		if process.returncode > 0:
			raise executeException({"returncode": process.returncode, "cmd": cmd})
		return stdout

	# Routine to get basic information about a video file
	def videoInfo(self, file):
		try:
			numStreams = int(self.executeCommand("ffprobe \""+file+"\" 2>&1 | awk '/Stream .+ Video/{print $0}' | wc -l"))
			metaData = self.executeCommand("exiftool \""+file+"\"")
			info = {}
			for line in metaData.splitlines():
				key, value = [x.strip() for x in line.split(" : ")]
				info[key] = value
			
			length = info['Duration'].split(":")
			secs = int(length[0]) * 3600 + int(length[1]) * 60 + int(length[2])

			videoinfo = {
				"videoStreams": numStreams,
				"height": int(info['Image Height']),
				"width": int(info['Image Width']),
				"length": secs
			}
		except executeException:
			return False
		except ValueError as e:
			return False
		else:
			return videoinfo

	def winPath(self, unix_path):
		return wineDrive + ":" + unix_path.replace("/","\\")

	# Write avisynth script
	def writeAvisynth(self,options):
		path = self.winPath(options['path'][0])
		metadata = youtubeUpload.getMetadata(options['path'][0], unicode=True)
		if metadata == False:
			raise metadataException
		title = metadata.get('title')
		course_id = options['config'].get('course_id')
		pubdate = metadata.get('pubDate')
		dirname = os.path.dirname(options['path'][0]) + "/"

		producer = metadata.get('producer')
		technician = metadata.get('technician')
		lecturer = metadata.get('lecturer')
		year = datetime.datetime.strptime(pubdate, "%Y-%m-%d %H:%M").strftime("%Y")
		if not producer:
			producer = u"LearningLab DTU / Kasper Skårhøj"

		self.generateIntroOverlay(title, course_id, pubdate, scriptDir+"Konverterede/" + options['path'][1] + '-introOverlay.png')
		self.generateOutroOverlays(producer, technician , lecturer, year, scriptDir+"Konverterede/" + options['path'][1] + '-outroOverlay')
		startOffset = metadata.get('startOffset')
		endOffset   = metadata.get('endOffset')

		if options['config'].get('branding') == True:
			branding = True
			inout    = True
		else:
			branding = False
			inout    = False

		if os.path.isfile(dirname + "intro.mov"):
			intro = self.winPath(dirname + "intro.mov")
		else:
			intro = defaultIntro
		if os.path.isfile(dirname + "outro.mov"):
			outro = self.winPath(dirname + "outro.mov")
		else:
			outro = defaultOutro

		if title and course_id and pubdate:
			template = open(scriptDir + "convert/avisynth.avs", 'r').read().decode('utf-8')
			videoList = ""
			for i in range(len(options['files'])):

				try:
					soff = startOffset[i]
				except (IndexError, TypeError) as e:
					soff = 0
				try:
					eoff = endOffset[i]
				except (IndexError, TypeError):
					eoff = 0

				videoList += "addVideoClip(\"" + self.winPath(options['files'][i]) + "\","+str(soff)+","+str(eoff)+")"
				if i != len(options['files'])-1:
					videoList += " ++ "
			template = template.format(
				intro = intro, 
				video = path, 
				outro = outro, 
				title=title, 
				course=course_id, 
				date=pubdate, 
				introOutro=inout, 
				brandClips=branding, 
				videoList=videoList, 
				fps=fps,
				introoverlay=self.winPath(scriptDir+"Konverterede/" + options['path'][1] + '-introOverlay.png'),
				outrooverlay1=self.winPath(scriptDir+"Konverterede/" + options['path'][1] + '-outroOverlay1.png'),
				outrooverlay2=self.winPath(scriptDir+"Konverterede/" + options['path'][1] + '-outroOverlay2.png')
				)
			script = open(scriptDir+"Konverterede/" + options['path'][1] + "-"+ options['options']['suffix'] + '.avs', 'w')
			script.write(template.encode('latin-1', 'ignore'))
			script.close
			return template
		else:
			raise metadataException({"type": "missingMetadata"})

	def handleConversionJob(self,conversionJob):
		rawFiles = conversionJob['files']
		options = conversionJob['options']

		missingOptions = validateList(options, ["width", "height", "quality", "suffix", "audiobitrate"])
		if missingOptions.__len__() > 0:
			log("Missing options: " + ", ".join(missingOptions) + " for file " + rawFiles[0])
			return False

		conversionJob['outputFile'] = scriptDir+"Konverterede/" + conversionJob['path'][1] + "-"+ conversionJob['options']['suffix']
		outputFile = conversionJob['outputFile']
		finalDestination = re.sub(conversionJob['rawSuffix']+"\..+", conversionJob['options']['suffix'] + ".mp4",conversionJob['path'][0])
		if os.path.isfile(finalDestination):
			log("File " + finalDestination + " already exists!")
			return False
		success = False
		convertLog = ""
		outputFile = outputFile + ".mp4"
		if os.path.isfile(outputFile):
			log("Removed outputFile prior to encoding ...")
			os.remove(outputFile)

		try:
			if not conversionJob['config'].get('branding') == True and len(rawFiles) == 1:
				log("HandBrake conversion of " + rawFiles[0] + " to " + conversionJob['preset'])
				convertLog = self.handbrakeConversion(conversionJob)
			else:
				# BUG: Fallthrough if branding is disabled and more than one raw file is found!
				log("Avisynth conversion of " + rawFiles[0] + " to " + conversionJob['preset'])
				log("Job consists of " + str(len(rawFiles)) + " raw files")
				convertLog = self.avisynthConversion(conversionJob)
		except metadataException as e:
			log("Missing metadata for file " + rawFiles[0], 'red')
			return False
		except executeException as e:
			print "error"
			print e
			log("Encoding of " + outputFile + " failed!", 'red')
		except Exception as e:
			print e
		else:
			if os.path.isfile(outputFile):
				log("Encoding of " + outputFile + " succeded!", 'green')
				shutil.move(outputFile, finalDestination)
				success = True
			else:
				log("Encoding of " + outputFile + " failed!", 'red')

		if convertLog:
			fp = open(outputFile.replace(".mp4",".log"), "w")
			fp.write(convertLog)
			fp.close()

		if success == True:
			return True
		else:
			return False

	# Conversion using avisynth, utilizing branding and intro/outro videos
	def avisynthConversion(self, job):
		options = job['options']
		inputFile = job['path'][0]
		outputFile = job['outputFile'] + '.mp4'
		audioFile = job['outputFile'] + '.wav'
		videoFile = job['outputFile'] + '.264'
		avsScript = job['outputFile'] + '.avs'

		self.writeAvisynth(job)
		log = ""
		try:
			log += self.executeCommand("wine avs2pipe audio \"" + avsScript + "\" > \"" + audioFile + "\"", niceness=True)
			#log += self.executeCommand("wine avs2yuv \""+ avsScript +"\" - | x264 --fps "+str(fps)+" --stdin y4m --output \""+videoFile+"\" --bframes 0 -q "+str(options['quality'])+" --video-filter resize:"+str(options['width'])+","+str(options['height'])+" -")
			log += self.executeCommand("wine avs2yuv \""+ avsScript +"\" - | x264 --fps "+str(fps)+" --stdin y4m --output \""+videoFile+"\" --bframes 0 -q "+str(options['quality'])+" --video-filter resize:"+str(options['width'])+","+str(options['height'])+" -", niceness=True)
			log += self.executeCommand("yes | ffmpeg -r "+str(fps)+" -i \""+videoFile+"\" -i \""+audioFile+"\" -vcodec copy -strict -2 \""+outputFile+"\"", niceness=True)
		except Exception:
			raise
		finally:
			if os.path.isfile(audioFile):
				os.remove(audioFile)
			if os.path.isfile(videoFile):
				os.remove(videoFile)
		return log

	# Legacy conversion using Handbrake. As of now faster and perhaps more reliable than avisynth.
	def handbrakeConversion(self, job):
		options = job['options']
		handBrakeArgs = "-e x264 -q " + str(options['quality']) + " -B " + str(options['audiobitrate']) + " -w " + str(options['width']) + " -l " + str(options['height'])	
		cmd = HandBrakeCLI + " --cpu " + str(CPUS) + " " + handBrakeArgs + " -r "+str(fps)+" -i '" + job['path'][0] + "' -o '" + job['outputFile'] + ".mp4'"
		return self.executeCommand(cmd, niceness=True)


##########################################
########### YOUTUBEUPLOAD ################
##########################################

class youtubeUpload (threading.Thread):
	queue = deque()
	def run(self):
		while not (mainThreadDone and not conversionObjs[0].isAlive()) or self.queue.__len__() > 0:
			if self.queue.__len__() > 0:
				element = self.queue.popleft()
				metadata = self.getMetadata(element['filename'])
				if metadata.get("enotelms:YouTubeUID"):
					log("Video is already on YouTube: " + element['filename'], 'yellow')
					continue
				else:
					try:
						self.yt_service = self.authenticate(element['username'],element['password'],element['developerKey'])
					except gdata.service.Error as e:
						log("ERROR: (Possibly?) no video channel created on YouTube account: " + element['username'], "red")
					else:
						video_id = self.uploadFromMetaData(element, metadata)
						if video_id != False:
							self.writeMetadata(element['filename'],{"enotelms:YouTubeUID": video_id})
						else:
							log("Youtube upload failed!", 'red')
			time.sleep(0.5)
		log("Main thread exited, terminating youtubeUpload...")

	# Upload video processing metadata
	def uploadFromMetaData(self, preferences, metadata):
		log('Uploading "' + preferences['filename'] + "'")
		playlist = preferences.get('playlist')
		if preferences.get('private') == True:
			private = True
		else:
			private = False
		missing = validateList(metadata, ["title", "description"])
		if missing.__len__() > 0:
			log("Missing options: " + ", ".join(missing) + " for file " + preferences['filename'], 'red')
			return False
		if not metadata.get('itunes:keywords'):
			metadata['itunes:keywords'] = defaultYoutubeKeywords
			log("WARNING: No keywords specified for file: " + preferences['filename'] + "!", 'yellow')

		options = {
			"title": metadata['title'],
			"description": metadata['description'],
			"keywords": metadata['itunes:keywords'],
			"private": private,
			"path": preferences['filename'],
			"category": preferences.get('category')
		}
		try:
			video_id = self.uploadVideo(options)
		except gdata.youtube.service.YouTubeError:
			return False
		else:
			if playlist:
				self.addToPlaylist(video_id, playlist)
			return video_id

	@staticmethod
	def addToQueue(filename, options):
		options['filename'] = filename
		youtubeUpload.queue.append(options)

	def authenticate(self, username, password, developer_key):
		yt_service = gdata.youtube.service.YouTubeService()
		yt_service.ssl = True
		yt_service.developer_key = developer_key
		yt_service.client_id = 'Podcast uploader'
		yt_service.email = username
		yt_service.password = password
		yt_service.ProgrammaticLogin()

		return yt_service

	@staticmethod
	def getMetadata(file, unicode=False):
		metafile = re.split('-\w+\.\w+$',file)[0] + ".txt"
		if os.path.isfile(metafile):
			with open(metafile, 'r') as fp:
				fcntl.flock(fp, fcntl.LOCK_SH)
				lines = fp.readlines()
				fp.close()
			metadata = {}
			for line in lines:
				if unicode==True:
					line = line.decode('utf-8')
				match = re.search('^\s*([^#^\s]\S+)\s*=\s*([^\[^\s]\S.*\S|\S|\S\S)\s*$', line)
				if match:
					submatch = re.search('^{(.+)}$', match.group(2))
					if submatch:
						metadata[match.group(1)] = submatch.group(1).split(',')
					else:
						metadata[match.group(1)] = match.group(2)
			return metadata
		else:
			return False
	
	@staticmethod
	def writeMetadata(file, data):
		metafile = re.split('-\w+\.\w+$',file)[0] + ".txt"
		if os.path.isfile(metafile):
			with open(metafile, 'r') as f:
				fcntl.flock(f, fcntl.LOCK_SH)
				lines = f.readlines()
				for key, line in enumerate(lines):
					match = re.search('^\s*(' + "|".join(data.keys()) + ')\s*=', line)
					if match:
						if data[match.group(1)] == False:
							lines.pop(key)
						else:
							lines[key] = match.group(1) + " = " + str(data[match.group(1)]) + "\n"
						data.pop(match.group(1))
					elif line[-1] != '\n':
						lines[key] = line + '\n'
				for idx in data:
					if data[idx] != False:
						lines.append(idx+" = " + str(data[idx]) + "\n")
				f.close()
			with open(metafile, 'w') as f:
				fcntl.flock(f, fcntl.LOCK_EX)
				f.writelines(lines)
				f.close()

	def retrievePlaylists(self):
		playlist_feed = self.yt_service.GetYouTubePlaylistFeed(username='default')
		playlists = {}
		for item in playlist_feed.entry:
			playlists[item.title.text] = item.feed_link[0].href
		return playlists
	def addPlaylist(self, playlist):
		new_playlist = self.yt_service.AddPlaylist(playlist,'')
		if isinstance(new_playlist, gdata.youtube.YouTubePlaylistEntry):
			log('Added playlist "' + playlist + '"', 'green')
			return new_playlist
	def addToPlaylist(self, video_id, playlist):
		playlists = self.retrievePlaylists()
		if playlist in playlists:
			url = playlists[playlist]
		else:
			new_playlist = self.addPlaylist(playlist)
			url = new_playlist.feed_link[0].href

		entry = self.yt_service.AddPlaylistVideoEntryToPlaylist(url, video_id)
		if isinstance(entry, gdata.youtube.YouTubePlaylistVideoEntry):
			log("Video added to playlist '" + playlist + "'", 'green')
		else:
			log("Video NOT added to playlist", 'red')
		pass
	def uploadVideo(self, options):
		if options['private'] == True:
			private = gdata.media.Private()
		else:
			private = None
		if options.get('category'):
			category = options.get('category')
		else:
			category = defaultYoutubeCategory
		media_group = gdata.media.Group(
			title = gdata.media.Title(text=options['title']),
			description = gdata.media.Description(description_type='plain', text=options['description']),
			keywords = gdata.media.Keywords(text=options['keywords']),
			category = [gdata.media.Category(
				text=category,
				scheme='http://gdata.youtube.com/schemas/2007/categories.cat',
				label=category
				)],
			player = None,
			private = private
			)
		video_entry = gdata.youtube.YouTubeVideoEntry(media=media_group)
		video_file_location = options['path']
		try:
			new_entry = self.yt_service.InsertVideoEntry(video_entry, video_file_location)
		except gdata.youtube.service.YouTubeError as e:
			log(e.message)
			raise
		else:
			return new_entry.id.text.split('/')[-1]

##########################################
########### Main thread ##################
##########################################

if isRunning():
	log("Another instance is already running. Exiting.", 'red')
	sys.exit(0)

log("Convert script launched")
log("Launching youtube processing thread..")

conversionQueueLock = threading.Lock()
conversionQueue = []
filesAdded = []
filesAddedYoutube = []

youtube = youtubeUpload()
log("Launching "+str(conversionThreads)+" video processing threads..")

for i in range(conversionThreads):
 	vidConv = videoConvert()
 	vidConv.start()
 	conversionObjs.append(vidConv)

youtube.start()

# Initial population of conversionQueue
checkFiles()

mainThreadDone = True	
