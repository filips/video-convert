#!/usr/bin/python2.6
import os,re,time,subprocess,pprint,sys
import simplejson as json
import shutil
import datetime
import threading
import gdata.youtube
import gdata.youtube.service
from collections import deque

##########################################
########### CONSTANTS ####################
##########################################

podcastPath = "/home/typothree/podcasts/TEST"
scriptDir = "/home/typothree/VideoParts/"
HandBrakeCLI = "/home/typothree/prefix/bin/HandBrakeCLI"

defaultYoutubeCategory = "Education"
defaultYoutubeKeywords = "Education"

rawSuffix = "raw" # Used to be 720p

CPUS = 4
NICENESS = 15

# List of file types.
fileTypes = ["mp4", "m4v", "mov"]

logFile = open(scriptDir + "convert.log", 'a')

fileList = []
structure = {}
conversionList = []

# Don't change
logging = True
pendingLog = []
mainThreadDone = False

##########################################
########### STANDALONE METHODS ###########
##########################################

def log(string):
	now = datetime.datetime.now()
	logstr = "[" + str(now)[:19] + "] " + str(string)
	if logging:
		pendingLog.append(logstr)
	else:
		pendingLog.append(logstr)
	print logstr

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
		if re.sub(rawSuffix+".+$",suffix, file) + "." + filetype in fileList:
			return True
	return False

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

def validateList(options, nonOptional):
	missing = []
	for opt in nonOptional:
		if not options.get(opt):
			missing.append(opt)
	return missing

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
				if self.handleConversionJob(element):

					# Adding job to youtubeUpload's queue. Should probably be handled by a watcher thread instead
					youtubeConfig = element['config'].get("youtube")
					destination = re.sub(rawSuffix, element['options']['suffix'],element['path'][0])
					if youtubeConfig:
						if element.get("preset") == youtubeConfig.get("uploadVersion"):
							youtubeUpload.addToQueue(destination, youtubeConfig)
			time.sleep(0.5)
		print "Main thread exited, terminating videoConvert..."
	def executeCommand(self, cmd):
		process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
		stdout, stderr = process.communicate()
		if process.returncode > 0:
			raise executeException({"returncode": process.returncode, "cmd": cmd})
		return stdout

	# Routine to get basic information about a video file
	def videoInfo(self):
		pass
	
	def writeAvisynth(self,options):
		path = 'z:' + options['path'][0].replace("/","\\")
		metadata = youtubeUpload.getMetadata(options['path'][0])

		title = metadata.get('title')
		course_id = options['config'].get('course_id')
		pubdate = metadata.get('pubDate')

		if title and course_id and pubdate:
			template = open(scriptDir + "avisynth.avs", 'r').read()
			template = template.format(intro = "z:\\home\\typothree\\VideoParts\\intro.mp4", video = path, outro = "z:\\home\\typothree\\VideoParts\\outro.mp4", title=title, course=course_id, date=pubdate)
			script = open(scriptDir+"Konverterede/" + options['path'][1] + "-"+ options['options']['suffix'] + '.avs', 'w')
			script.write(template.decode('utf-8').encode('latin-1'))
			script.close
			return template
		else:
			raise metadataException({"type": "missingMetadata"})

	def handleConversionJob(self,conversionJob):
		rawFile = conversionJob['path'][0]
		options = conversionJob['options']

		missingOptions = validateList(options, ["width", "height", "quality", "suffix", "audiobitrate"])
		if missingOptions.__len__() > 0:
			log("Missing options: " + ", ".join(missingOptions) + " for file " + rawFile)
			return False

		conversionJob['outputFile'] = scriptDir+"Konverterede/" + conversionJob['path'][1] + "-"+ conversionJob['options']['suffix']
		outputFile = conversionJob['outputFile']
		finalDestination = re.sub(rawSuffix, conversionJob['options']['suffix'],conversionJob['path'][0])
		if os.path.isfile(finalDestination):
			log("File " + finalDestination + " already exists!")
			return False

		convertLog = ""
		outputFile = outputFile + ".mp4"
		if os.path.isfile(outputFile):
			log("Removed outputFile prior to encoding ...")
			os.remove(outputFile)

		try:
			if not conversionJob['config'].get('branding') == True:
				log("HandBrake conversion of " + rawFile + "...")
				convertLog = self.handbrakeConversion(conversionJob)
			else:
				log("Avisynth conversion of " + rawFile + " to " + conversionJob['preset'])
				convertLog = self.avisynthConversion(conversionJob)
		except metadataException as e:
			log("Missing metadata for file " + rawFile)
			return
		except executeException as e:
			print "error"
			print e
			log("Encoding of " + outputFile + " failed!")
		except Exception as e:
			print e
		else:
			if os.path.isfile(outputFile):
				log("Encoding of " + outputFile + " succeded!")
				shutil.move(outputFile, finalDestination)
				return True
			else:
				log("Encoding of " + outputFile + " failed!")


		fp = open(outputFile.replace(".mp4",".log"), "w")
		fp.write(convertLog)
		fp.close()

		


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
			log += self.executeCommand("wine avs2pipe audio " + avsScript + " > " + audioFile)
			log += self.executeCommand("wine avs2yuv "+ avsScript +" - | x264 --fps 25 --stdin y4m --output "+videoFile+" --bframes 0 -q "+str(options['quality'])+" --video-filter resize:"+str(options['width'])+","+str(options['height'])+" -")
			log += self.executeCommand("yes | ffmpeg -r 25 -i "+videoFile+" -i "+audioFile+" -vcodec copy -strict -2 "+outputFile)
		except Exception:
			raise
		finally:
			os.remove(audioFile)
			os.remove(videoFile)
		return log
	def handbrakeConversion(self, job):
		options = job['options']
		handBrakeArgs = "-e x264 -q " + str(options['quality']) + " -B " + str(options['audiobitrate']) + " -w " + str(options['width']) + " -l " + str(options['height'])	
		cmd = "nice -n " + str(NICENESS) + " " + HandBrakeCLI + " --cpu " + str(CPUS) + " " + handBrakeArgs + " -r 25 -i '" + job['path'][0] + "' -o '" + job['outputFile'] + ".mp4'"
		return self.executeCommand(cmd)


##########################################
########### YOUTUBEUPLOAD ################
##########################################

class youtubeUpload (threading.Thread):
	queue = deque()
	def run(self):
		while not mainThreadDone or self.queue.__len__() > 0:
			if self.queue.__len__() > 0:
				element = self.queue.popleft()
				metadata = self.getMetadata(element['filename'])
				if metadata.get("enotelms:YouTubeUID"):
					log("Video is already on YouTube: " + element['filename'])
					continue
				else:
					self.yt_service = self.authenticate(element['username'],element['password'],element['developerKey'])
					video_id = self.uploadFromMetaData(element, metadata)
					if video_id != False:
						self.writeMetadata(element['filename'],{"enotelms:YouTubeUID": video_id})
					else:
						log("Youtube upload failed!")
			time.sleep(0.5)
		log("Main thread exited, terminating youtubeUpload...")

	def uploadFromMetaData(self, preferences, metadata):
		log('Uploading "' + preferences['filename'] + "'")
		playlist = preferences.get('playlist')
		if preferences.get('private') == True:
			private = True
		else:
			private = False
		missing = validateList(metadata, ["title", "description"])
		if missing.__len__() > 0:
			log("Missing options: " + ", ".join(missing) + " for file " + preferences['filename'])
			return False
		if not metadata.get('keywords'):
			metadata['keywords'] = defaultYoutubeKeywords
			log("WARNING: No keywords specified for file: " + preferences['filename'] + "!")

		options = {
			"title": metadata['title'],
			"description": metadata['description'],
			"keywords": metadata['keywords'],
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
					metadata[match.group(1)] = match.group(2)
			return metadata
		else:
			return False
	def writeMetadata(self,file,data):
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
			log('Added playlist "' + playlist + '"')
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
			log("Video added to playlist '" + playlist + "'")
		else:
			log("Video NOT added to playlist")
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
	log("Another instance is already running. Exiting.")
	sys.exit(0)

log("Convert script launched")
log("Launching youtube processing thread..")

youtubeUpload().start()
log("Launching video processing thread..")
videoConvert().start()

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
				log("Error importing config file in '" + root + "'. Not doing *any* conversion beyond this path.")
				last['config'] = False
			else:
				last['config'] = config
		fileList.append(os.path.join(root,file))

for file in fileList:
	pattern = re.compile("^.+\/([^\/]+)-([a-zA-Z0-9]+)\.([^\.^-]+)$")
	parts = pattern.search(file)
	if parts:
		data = parts.group(0,1,2,3)
		basename,name,quality,ext = data
		if ext in fileTypes:
			if time.time() - os.stat(file).st_ctime < 20:
				log("Skipping " + os.path.basename(file) + ", too new")
			else:
				if quality == rawSuffix:
					config = getConfig(file)
					if config.get('youtubeUpload') == True:
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
					if config.get("convert") == True:
						for format in config.get('formats'):
							preset = config.get('presets').get(format)
							if preset:
								if not versionExists(file, preset.get('suffix')):
									conversionJob = {"path": data, "options": preset,"preset": format, "config": config}
									videoConvert.queue.append(conversionJob)
							else:
								log("Format '" + format + "' not found. Available ones are (" + ', '.join(format for format in config.get('presets')) + ")")

#mainThreadDone = True	