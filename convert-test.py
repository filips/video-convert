#!/usr/bin/python2.6
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

# List of possible video file types
fileTypes = ["mp4", "m4v", "mov"]


logFile = open(scriptDir + "convert.log", 'a')

fileList = []
structure = {}
conversionList = []

# The drive letter in Wine correlating to linux "/"
wineDrive = "z"

# Video framerate. Should be configurable on a per-channel basis
fps = 25 

# Don't change
logging = True
pendingLog = []
mainThreadDone = False

##########################################
########### STANDALONE METHODS ###########
##########################################

def log(string, color="white"):
	now = datetime.datetime.now()
	date = "[" + str(now)[:19] + "] "
	message = str(string)
	logstr = date + message
	if logging:
		pendingLog.append(logstr)
		while len(pendingLog) > 0:
			logFile.write(pendingLog.pop(0) + "\n")
			logFile.flush()
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

def versionExists(file, suffix):
	for filetype in fileTypes:
		filename = re.sub(rawSuffix+".+$",suffix, file) + "." + filetype
		if filename in fileList:
			return True
		elif os.path.isfile(filename) and os.stat(filename).st_ctime >= 10:
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
	queue = deque()
	def run(self):
		while not mainThreadDone or self.queue.__len__() > 0:
			if self.queue.__len__() > 0:
				log("Currently "+str(self.queue.__len__()) + " items queued for conversion.")
				element = self.queue.popleft()
				numStreams = self.videoInfo(element['path'][0])
				error = False
				if numStreams == 1:
					if self.handleConversionJob(element):

						# Adding job to youtubeUpload's queue. Should probably be handled by a watcher thread instead
						youtubeConfig = element['config'].get("youtube")
						destination = re.sub(rawSuffix, element['options']['suffix'],element['path'][0])
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
			time.sleep(0.5)
		log("Main thread exited, terminating videoConvert...")

	def generateIntroOverlay(self, title, course, date,file):
		import Image, ImageDraw, ImageFont

		width, height = 1280, 720
		titleOffsetX = 340

		img = Image.new("RGBA", (width, height), (0,0,0,0))
		draw = ImageDraw.Draw(img)
		titleFont = ImageFont.truetype("/home/typothree/.fonts/NeoSansStd-Medium.otf", 40, encoding='unic')
		courseFont = ImageFont.truetype("/home/typothree/.fonts/NeoSansStd-Regular.otf", 24, encoding='unic')
		dateFont = ImageFont.truetype("/home/typothree/.fonts/NeoSansStd-Regular.otf", 24, encoding='unic')

		title = "BLAHBLAHBLAHBLAHBLAHBLAHBLAHBLAHBLAHBLAHBLAHBLAHBLAHBLAHBLAHBLAHBLAHBLAHBLAHBLAH Blah blha blablabl blab alb ab bal"
		dateSize = dateFont.getsize(date)
		titleSize = titleFont.getsize(title)

		maxWidth = width - titleOffsetX - 175

		titleSegments = []
		tempSegment = ""
		for word in title.split():
			if titleFont.getsize(tempSegment + word)[0] <= maxWidth:
				tempSegment = tempSegment + word + " "
			elif titleFont.getsize(word)[0] <= maxWidth:
				if len(tempSegment) > 0:
					titleSegments.append(tempSegment)
				tempSegment = word + " "
			else:
				if len(tempSegment) > 0:
					titleSegments.append(tempSegment)
				tempWord = ""
				for letter in word:
					if titleFont.getsize(tempWord + letter)[0] <= maxWidth:
						tempWord = tempWord + letter
					else:
						titleSegments.append(tempWord)
						tempWord = letter
				tempSegment = tempWord
		titleSegments.append(tempSegment)

		for key,part in enumerate(titleSegments):
			draw.text((titleOffsetX, 430 + 45*key), part.upper(), font=titleFont, fill=(27,65,132,255))
		draw.text((titleOffsetX, 400), course.upper(), font=courseFont, fill=(27,65,132,255))
		draw.text((width-dateSize[0] - 20, height-dateSize[1] - 20), date, font=dateFont, fill=(204,204,204,255))
		img.save(file)

	@staticmethod
	def executeCommand(cmd):
		process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
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
		except executeException:
			return False
		except ValueError:
			return False
		else:
			return numStreams

	def winPath(self, unix_path):
		return wineDrive + ":" + unix_path.replace("/","\\")

	# Write avisynth script
	def writeAvisynth(self,options):
		path = self.winPath(options['path'][0])
		metadata = youtubeUpload.getMetadata(options['path'][0])
		if metadata == False:
			raise metadataException
		title = metadata.get('title')
		course_id = options['config'].get('course_id')
		pubdate = metadata.get('pubDate')
		dirname = os.path.dirname(options['path'][0]) + "/"

		self.generateIntroOverlay(title, course_id, pubdate, scriptDir+"Konverterede/" + options['path'][1] + '-introOverlay.png')

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
			template = open(scriptDir + "convert/avisynth.avs", 'r').read()
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
				introoverlay=self.winPath(scriptDir+"Konverterede/" + options['path'][1] + '-introOverlay.png')
				)

			script = open(scriptDir+"Konverterede/" + options['path'][1] + "-"+ options['options']['suffix'] + '.avs', 'w')
			script.write(template.decode('utf-8').encode('latin-1'))
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
		finalDestination = re.sub(rawSuffix+"\..+", conversionJob['options']['suffix'] + ".mp4",conversionJob['path'][0])
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
				log("HandBrake conversion of " + rawFiles[0] + "...")
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
			log += self.executeCommand("wine avs2pipe audio \"" + avsScript + "\" > \"" + audioFile + "\"")
			#log += self.executeCommand("wine avs2yuv \""+ avsScript +"\" - | x264 --fps "+str(fps)+" --stdin y4m --output \""+videoFile+"\" --bframes 0 -q "+str(options['quality'])+" --video-filter resize:"+str(options['width'])+","+str(options['height'])+" -")
			log += self.executeCommand("wine avs2yuv \""+ avsScript +"\" - | x264 --fps "+str(fps)+" --stdin y4m --output \""+videoFile+"\" --bframes 0 -q "+str(options['quality'])+" --video-filter resize:"+str(options['width'])+","+str(options['height'])+" -")
			log += self.executeCommand("yes | ffmpeg -r "+str(fps)+" -i \""+videoFile+"\" -i \""+audioFile+"\" -vcodec copy -strict -2 \""+outputFile+"\"")
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
		cmd = "nice -n " + str(NICENESS) + " " + HandBrakeCLI + " --cpu " + str(CPUS) + " " + handBrakeArgs + " -r "+str(fps)+" -i '" + job['path'][0] + "' -o '" + job['outputFile'] + ".mp4'"
		return self.executeCommand(cmd)


##########################################
########### YOUTUBEUPLOAD ################
##########################################

class youtubeUpload (threading.Thread):
	queue = deque()
	def run(self):
		while not (mainThreadDone and not vidConv.isAlive()) or self.queue.__len__() > 0:
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
	def getMetadata(file):
		metafile = re.split('-\w+\.\w+$',file)[0] + ".txt"
		if os.path.isfile(metafile):
			with open(metafile, 'r') as fp:
				lines = fp.readlines()
				fp.close()
			metadata = {}
			for line in lines:
				match = re.search('^\s*([^#^\s]\S+)\s*=\s*([^\[^\s]\S.*\S|\S)\s*$', line)
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
	def writeMetadata(file,data):
		metafile = re.split('-\w+\.\w+$',file)[0] + ".txt"
		if os.path.isfile(metafile):
			f = open(metafile, 'a')
			for idx in data:
				f.write("\n" + idx+" = "+data[idx]+"\n")
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

youtube = youtubeUpload()
log("Launching video processing thread..")
vidConv = videoConvert()

youtube.start()
vidConv.start()

log("Scanning " + podcastPath +  " for movie files...")
for root, subFolders, files in os.walk(podcastPath):
	path = root[len(podcastPath):].split('/')
	lastStructure = structure
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
			fileList.append(os.path.join(root,file))

rawFiles = {}
pattern = re.compile("^.+\/([^\/]+)-"+rawSuffix+"(\d)?\.([^\.^-]+)$")
for path in fileList:
	parts = pattern.search(path)
	if parts:
		if not parts.group(1) in rawFiles:
			rawFiles[parts.group(1)] = {}

		index = parts.group(2)
		if not index:
			index = 0

		rawFiles[parts.group(1)][int(index)] = parts.group(0)

pattern = re.compile("^.+\/([^\/]+)-([a-zA-Z0-9]+)\.([^\.^-]+)$")
for file in fileList:
	parts = pattern.search(file)
	if parts:
		data = parts.group(0,1,2,3)
		basename,name,quality,ext = data
		if ext in fileTypes:
			if quality == rawSuffix:
				config = getConfig(file)
				metadata = youtubeUpload.getMetadata(file)
				if not metadata:
					log("Missing metadata for file " + file, 'red')
					continue
				if metadata.get('quarantine') == "true":
					log(file+" is in quarantine!", "red")
					continue

				# Check if videos are to be uploaded to YouTube
				if config.get('youtubeUpload') == True and not metadata.get('enotelms:YouTubeUID'):
					youtube = config.get('youtube');
					if youtube.get('uploadVersion'):
						version = youtube.get('uploadVersion')
					else:
						version = "720p"
					config['youtube']['uploadVersion'] = version
					if versionExists(file, version):
						username = youtube.get('username')
						password = youtube.get('password')
						developer_key = youtube.get('developerKey')
						playlist = youtube.get('playlist')
						if username and password and developer_key:
							filename = file.replace(rawSuffix, version)
							youtubeUpload.addToQueue(filename, youtube.copy())
				# Check if videos are to be converted
				if config.get("convert") == True:
					for format in config.get('formats'):
						preset = config.get('presets').get(format)
						if preset:

							if not versionExists(file, preset.get('suffix')):
								conversionJob = {"path": data, "files": rawFiles[data[1]], "options": preset,"preset": format, "config": config}
								videoConvert.queue.append(conversionJob)
						else:
							log("Format '" + format + "' not found. Available ones are (" + ', '.join(format for format in config.get('presets')) + ")")
				# Generate thumbnails if missing
				thumbnail = re.sub("-" + rawSuffix + "\.(" + "|".join(fileTypes) + ")", "-1.png", file)
				if not os.path.isfile(thumbnail):
					try:
						videoConvert.executeCommand("ffmpeg -ss 0.5 -i '"+file+"' -vframes 1 -s 640x360 '"+thumbnail+"'")
					except executeException:
						log("Error generating thumbnail: " + thumbnail, 'red')
					else:
						log("Generated thumbnail: " + thumbnail, 'green')

mainThreadDone = True	
