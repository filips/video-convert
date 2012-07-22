#!/usr/bin/python2.6
import os,re,time,subprocess,pprint,sys
import simplejson as json
import shutil
import datetime
import threading
import gdata.youtube
import gdata.youtube.service
from collections import deque

podcastPath = "/home/typothree/podcasts/TEST"
scriptDir = "/home/typothree/VideoParts/"
HandBrakeCLI = "/home/typothree/prefix/bin/HandBrakeCLI"
avisynthLauncher = "/home/typothree/VideoParts/convert/convert"

rawSuffix = "raw" # Used to be 720p

CPUS = 4
NICENESS = 15

# List of file types. For now ignoring .mov, as they in our current setup is unable to convert
fileTypes = ["mp4", "m4v", "mov"]

# Truncate the logfile to 1000 lines
#os.system("tail -n 1000 "+scriptDir+"convert.log > "+scriptDir+"convert.log.tmp; mv "+scriptDir+"convert.log.tmp "+scriptDir+"convert.log")

logFile = open(scriptDir + "convert.log", 'a')

fileList = []
structure = {}
conversionList = []

# Don't change
logging = False
pendingLog = []
mainThreadDone = False

def log(string):
	now = datetime.datetime.now()
	logstr = "[" + str(now)[:19] + "] " + str(string)
	if logging:
		pendingLog.append(logstr)
		# while len(pendingLog) > 0:
		# 	#logFile.write(pendingLog.pop(0) + "\n")
		# 	#logFile.flush()
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

def writeAvisynth(options):
	path = 'z:' + options['path'][0].replace("/","\\")
	metadata = youtubeUpload.getMetadata(options['path'][0])
	template = open(scriptDir + "avisynth.avs", 'r').read()
	template = template.format(intro = "z:\\home\\typothree\\VideoParts\\intro.mp4", video = path, outro = "z:\\home\\typothree\\VideoParts\\outro.mp4", title=metadata['title'], course="01234 Test", date="7. maaned")
	script = open(options['path'][0].replace(options['path'][3],"avs"), 'w')
	script.write(template)
	script.close
	return template

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

def handBrakeArgs(options):
	return "-e x264 -q " + str(options['quality']) + " -B " + str(options['audiobitrate']) + " -w " + str(options['width']) + " -l " + str(options['height'])

if isRunning():
	log("Another instance is already running. Exiting.")
	sys.exit(0)

##########################################
########### VIDEOCONVERT #################
##########################################

class videoConvert(threading.Thread):
	queue = deque()
	def run(self):
		while not mainThreadDone or self.queue.__len__() > 0:
			if self.queue.__len__() > 0:
				element = self.queue.popleft()
				self.handleConversionJob(element)
			time.sleep(0.5)
		print "Main thread exited, terminating videoConvert..."
	def executeCommand(self, cmd):
		cmdparts = cmd.split(" ")
		cmdparts = cmd
		process = subprocess.Popen(cmdparts, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
		stdout, stderr = process.communicate()
		if process.returncode > 0:
			raise Exception({"returncode": process.returncode})
		return stdout

	# Routine to get basic information about a video file
	def videoInfo(self):
		pass

	def handleConversionJob(self,conversionJob):
		rawFile = conversionJob['path'][0]
		options = conversionJob['options']

		conversionJob['outputFile'] = scriptDir+"Konverterede/" + conversionJob['path'][1] + "-"+ conversionJob['options']['suffix']
		outputFile = conversionJob['outputFile']
		finalDestination = re.sub(rawSuffix, conversionJob['options']['suffix'],conversionJob['path'][0])

		if os.path.isfile(outputFile):
			log("Removed outputFile prior to encoding ...")
			os.remove(outputFile)

		if options.get('branding') == True:
			log("HandBrake conversion of " + rawFile + "...")
			convertLog = self.handbrakeConversion(conversionJob)
		else:
			log("Avisynth conversion of " + rawFile + "...")
			convertLog = self.avisynthConversion(conversionJob)
		
		fp = open(outputFile + ".log", "w")
		fp.write(convertLog)
		fp.close()

		outputFile = outputFile + ".mp4"
		if os.path.isfile(outputFile):
			log("Encoding of " + outputFile + " succeded!")
			shutil.move(outputFile, finalDestination)
		else:
			log("Encoding of " + outputFile + " failed!")

	def avisynthConversion(self, job):
		options = job['options']
		avsScript = job['path'][0].replace(job['path'][3],"avs")
		inputFile = job['path'][0]
		outputFile = job['outputFile'] + '.mp4'
		audioFile = job['outputFile'] + '.wav'
		videoFile = job['outputFile'] + '.264'

		writeAvisynth(job)
		log = ""
		log += self.executeCommand("wine avs2pipe audio " + avsScript + " > " + audioFile)
		log += self.executeCommand("wine avs2yuv "+ avsScript +" - | x264 --fps 25 --stdin y4m --output "+videoFile+" --bframes 0 -q "+str(options['quality'])+" --video-filter resize:"+str(options['width'])+","+str(options['height'])+" -")
		log += self.executeCommand("yes | ffmpeg -r 25 -i "+videoFile+" -i "+audioFile+" -vcodec copy -strict -2 "+outputFile)

		os.remove(audioFile)
		os.remove(videoFile)

		return log
	def handbrakeConversion(self, job):	
		cmd = "nice -n " + str(NICENESS) + " " + HandBrakeCLI + " --cpu " + str(CPUS) + " " + handBrakeArgs(options) + " -r 25 -i '" + job['path'][0] + "' -o '" + job['outputFile'] + "'"
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
				metadata = self.getMetadata(element['file'])
				if metadata.get("enotelms:YouTubeUID"):
					print "Video is already on YouTube"
					continue
				else:
					self.yt_service = self.authenticate(element['username'],element['password'],element['developer_key'])
					video_id = self.uploadFromMetaData(element['file'], metadata)
					self.writeMetadata(element['file'],{"enotelms:YouTubeUID": video_id})
			time.sleep(0.5)
		print "Main thread exited, terminating youtubeUpload..."
		#self.uploadFromMetaData(self.path)
	def uploadFromMetaData(self, path, metadata):
		print 'Uploading "' + path + "'"
		options = {
			"title": metadata['title'],
			"description": metadata['description'],
			"keywords": metadata['keywords'],
			"private": True,
			"path": path
		}
		try:
			video_id = self.uploadVideo(options)
		except gdata.youtube.service.YouTubeError:
			pass
		else:
			self.addToPlaylist(video_id, "Testvideoer!!!!!")
			return video_id
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
				f.write(idx+" = "+data[idx]+"\n")
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
			print 'Added playlist "' + playlist + '"'
	def addToPlaylist(self, video_id, playlist):
		playlists = self.retrievePlaylists()
		if playlist in playlists:
			pass
		else:
			self.addPlaylist(playlist)
			playlists = self.retrievePlaylists()

		entry = self.yt_service.AddPlaylistVideoEntryToPlaylist(playlists[playlist], video_id)
		if isinstance(entry, gdata.youtube.YouTubePlaylistVideoEntry):
			print "Video added to playlist '" + playlist + "'"
		else:
			print "Video NOT added to playlist"
		pass
	def uploadVideo(self, options):
		media_group = gdata.media.Group(
			title = gdata.media.Title(text=options['title']),
			description = gdata.media.Description(description_type='plain', text=options['description']),
			keywords = gdata.media.Keywords(text=options['keywords']),
			category = [gdata.media.Category(
				text='Education',
				scheme='http://gdata.youtube.com/schemas/2007/categories.cat',
				label='Education'
				)],
			player = None,
			private = gdata.media.Private()
			)
		video_entry = gdata.youtube.YouTubeVideoEntry(media=media_group)
		video_file_location = options['path']
		try:
			new_entry = self.yt_service.InsertVideoEntry(video_entry, video_file_location)
		except gdata.youtube.service.YouTubeError as e:
			print e.message
			raise
		else:
			return new_entry.id.text.split('/')[-1]

# END youtubeUpload


##########################################
########### Main thread ##################
##########################################

log("Convert script launched")
log("Launching youtube processing thread..")

#youtubeUpload().start()
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

						if versionExists(file, version):
							username = youtube.get('username')
							password = youtube.get('password')
							developer_key = youtube.get('developerKey')
							if username and password and developer_key:
								youtubeUpload.queue.append({"username": username, "password": password, "developer_key": developer_key, "file": file.replace(rawSuffix, version)})
					if config.get("convert") == True:
						for format in config.get('formats'):
							preset = config.get('presets').get(format)
							if preset:
								if not versionExists(file, preset.get('suffix')):
									conversionJob = {"path": data, "options": preset,"preset": format}
									#conversionList.append(conversionJob)
									videoConvert.queue.append(conversionJob)
							else:
								log("Format '" + format + "' not found. Available ones are (" + ', '.join(format for format in config.get('presets')) + ")")

#pp = pprint.PrettyPrinter(indent=2)
#pp.pprint(structure)

if conversionList.__len__() > 0:
	logging = True

log(str(conversionList.__len__()) + " items queued for conversion.")
for job in conversionList:
	writeAvisynth(job)
	finalDestination = re.sub(rawSuffix, job['options']['suffix'],job['path'][0])
	outputFile = scriptDir+"Konverterede/" + job['path'][1] + "-"+ job['options']['suffix']
	if os.path.isfile(outputFile):
		log("Removed outputFile prior to encoding ...")
		os.remove(outputFile)
	log("Converting '" + job['path'][0] + "' to '" + job['preset'] + "'")
	#cmd = "nice -n " + str(NICENESS) + " " + HandBrakeCLI + " --cpu " + str(CPUS) + " " + handBrakeArgs(job['options']) + " -r 25 -i '" + job['path'][0] + "' -o '" + outputFile + "'"
	cmd = "nice -n " + str(NICENESS) + " " + avisynthLauncher + " " + job['path'][0].replace(job['path'][3],"avs") + " " + job['options']['quality'] + " " + outputFile + " " + str(job['options']['width']) + " " + str(job['options']['height'])
	log(cmd)
	process = subprocess.Popen(cmd, shell=True)
	process.wait()
	print
	#fp = open(scriptDir + "Konverterede/" + job['path'][1] + "-"+ job['options']['suffix'] + ".log", "w")
	#fp.write("".join(process.stderr.readlines()))
	#fp.close()
	outputFile = outputFile + ".mp4"
	if os.path.isfile(outputFile):
		log("Encoding of " + outputFile + " succeded!")
		shutil.move(outputFile, finalDestination)
	else:
		log("Encoding of " + outputFile + " failed!")

log("Conversion done. exiting!")	

mainThreadDone = True	