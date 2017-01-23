#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2014 Filip Sandborg-Olsen <filipsandborg@gmail.com>

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
import datetime
import fcntl
import locale
import os, re, time, subprocess, sys
import shutil
import smtplib
import threading
from collections import deque
import traceback

from PIL import Image, ImageFont, ImageDraw, ImageChops
import json
from termcolor import colored

from metadata import getMetadata, writeMetadata
import getopt

from contextlib import contextmanager

# Google Data API for YouTube Integration
from apiclient.discovery import build
from apiclient.errors import HttpError
from apiclient.http import MediaFileUpload
from oauth2client.client import flow_from_clientsecrets, GoogleCredentials
import httplib2, http.client

httplib2.RETRIES = 1

##########################################
########### CONSTANTS ####################
##########################################

settingsFile = os.path.join(os.path.dirname(os.path.realpath(__file__)), "settings.json")

settings = False
missing = False

if os.path.isfile(settingsFile):
    try:
        settings = json.loads(open(settingsFile).read())
        requiredFields = [
            "fontDir",
            "scriptDir",
            "podcastPath",
            "nametagPath",
            "defaultYoutubeCategory",
            "defaultYoutubeKeywords",
            "defaultIntro",
            "defaultOutro",
            "defaultLogo",
            "queueTextFile",
            "youtubeAuth",
            "youtubeSecrets"
        ]
        for field in requiredFields:
            if not field in settings:
                print("Missing option '" + field + "' in settings.json")
                missing = True
    except:
        settings = None

if settings is False:
    print("No settings.json file found!")
    sys.exit(1)
elif settings is None:
    print("Invalid JSON in settings.json")
    sys.exit(1)
elif missing:
    sys.exit(1)

rawSuffix = "raw" # Used to be 720p

CPUS = 4
NICENESS = 15

# Number of simultaneous conversion threads
conversionThreads = 4

# List of possible video file types
fileTypes = ["mp4", "m4v", "mov"]


fullLog = open(settings.get("scriptDir") + "full.log", 'a')
infoLog = open(settings.get("scriptDir") + "info.log", 'a')

fileList = []
structure = {}
conversionList = []

# Video framerate. Should be configurable on a per-channel basis
fps = 25

# Minimum interval between folder scans
scanInterval = 60

# Locale primarily used in date formats. Should be customized in metadata
locale.setlocale(locale.LC_ALL, "da_DK.UTF-8")

## INTERNAL ##

opts, args = getopt.getopt(sys.argv[1:],'',["dry-run"])

dryrun = False
for opt, arg in opts:
    if opt == "--dry-run":
        dryrun = True


# Don't change
logging = True
pendingLog = []
mainThreadDone = False
convertObjLock = threading.Lock()
conversionObjs = []
lastScanned = 0

localeLock = threading.Lock()

quarantinedFiles = []


# Localization

localizedStrings = {
    'copyright': {"Danish": "Danmarks Tekniske Universitet", "English": "Technical University of Denmark"},
    'presenter': {"Danish": "Forelæser", "English": "Presenter"},
    'technician': {"Danish": "Teknik", "English": "Technician"},
    'producer': {"Danish": "Producer"}
}
##########################################
########### STANDALONE METHODS ###########
##########################################

@contextmanager
def setLocale(name):
    """Setting the unix locale"""
    with localeLock:
        oldLocale = locale.setlocale(locale.LC_ALL)
        try:
            yield locale.setlocale(locale.LC_ALL, name)
        finally:
            locale.setlocale(locale.LC_ALL, oldLocale)

# Returning localized strings will return empty strings if string could not be found.
def getLocalizedString(string, language):
    if string in localizedStrings:
        default = localizedStrings[string].get('Danish', '')
        return localizedStrings[string].get(language, default)
    else:
        return ''

def saveQueueText():
    """This method saves the queue.html file, which can then be served by a web server"""
    
    accum = ""
    accum += """
<html>
    <head>
        <title>Conversion queue</title>
        <style>
            table {
                margin: auto;
                margin-top: 100px;
                border-spacing:0;
                border-collapse:collapse;
                width: 900px;
            }
            table tr th {
                text-align: left;
            }
            table tr.header {
                background-color: rgb(153,0,0);
                color: #fff;
            }
            table tr.header th {
                padding:5px;
                padding-right: 40px;
            }
            table td {
                padding: 5px;
            }
            table tr.odd {
                background-color: #ddd;
            }
            body {
                font-family: palatino;
                font-size: 11px;
            }
        </style>
        <meta http-equiv="refresh" content="5">
    </head>
    <body>
        <div style="text-align: center; padding: 10px; font-size: 15px">Last activity change: """+str(datetime.datetime.now())+"""
        <p><a href="" onClick='xmlhttp = new XMLHttpRequest(); xmlhttp.open("GET", "setRefresh.php", true); xmlhttp.send(); return false;'>Force update of list</a></p></div>
        <table>
            <tr class="header">
                <th>#</th>
                <th>Name</th>
                <th>Version</th>
                <th>Priority</th>
                <th>Date</th>
            </tr>
    """
    with conversionQueueLock:
        for key, el in enumerate(conversionQueue):
            accum += """
            <tr class="data {oddity}">
                <td>{id}</td>
                <td>{path}</td>
                <td>{version}</td>
                <td>{priority}</td>
                <td>{date}</td>
            </tr>           
            """.format(id=str(key+1), version=el['preset'], date=el['metadata']['pubDate'].replace(" ", " "), priority=str(el['priority']), path=el['path'][0], oddity='odd' if key%2==1 else 'even')
    accum += """
        </table>
    </body>
</html>
    """
    if settings.get('queueTextFile'):
        with open(settings.get("queueTextFile"), 'w') as f:
            f.write(accum)

def saveQuarantineText():
    """This method saves the quarantine.html file"""
    accum = ""
    accum += """
<html>
    <head>
        <title>Quarantine list</title>
        <style>
            table {
                margin: auto;
                margin-top: 100px;
                border-spacing:0;
                border-collapse:collapse;
                width: 900px;
            }
            table tr th {
                text-align: left;
            }
            table tr.header {
                background-color: rgb(153,0,0);
                color: #fff;
            }
            table tr.header th {
                padding:5px;
                padding-right: 40px;
            }
            table td {
                padding: 5px;
            }
            table tr.odd {
                background-color: #ddd;
            }
            body {
                font-family: palatino;
                font-size: 11px;
            }
        </style>
        <meta http-equiv="refresh" content="5">
    </head>
    <body>
        <div style="text-align: center; padding: 10px; font-size: 15px">Last activity change: """+str(datetime.datetime.now())+"""
        <p><a href="" onClick='xmlhttp = new XMLHttpRequest(); xmlhttp.open("GET", "setRefresh.php", true); xmlhttp.send(); return false;'>Force update of list</a></p></div>
        </div>
        <table>
            <tr class="header">
                <th>Name</th>
                <th>Reason</th>
            </tr>
    """
    i = 0
    for el in quarantinedFiles:
        i += 1
        accum += """
        <tr class="data {oddity}">
            <td>{name}</td>
            <td>{reason}</td>
        </tr>           
        """.format(name=el.get('name'), reason=el.get('reason'), oddity='odd' if i%2==1 else 'even')
    accum += """
        </table>
    </body>
</html>
    """
    if settings.get('quarantineTextFile'):
        with open(settings.get("quarantineTextFile"), 'w') as f:
            f.write(accum)

def scanStructure():
    """Scans the podcast path for files"""

    _structure = {}
    _fileList = []
    log("Scanning " + settings.get('podcastPath') +  " for movie files...")
    for root, subFolders, files in os.walk(settings.get('podcastPath')):
        path = root[len(settings.get('podcastPath')):].split('/')
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
            try:
                if time.time() - os.stat(os.path.join(root,file)).st_ctime < 10:
                    log("Skipped " + file + ", too new", 'yellow')
                else:
                    _fileList.append(os.path.join(root,file))
            except:
                log("File " + file + " disappeared", 'yellow')
    return _fileList, _structure

def addToQueue(job):
    """Adds a job object to the conversion queue"""
    index = -1
    for key, element in enumerate(conversionQueue):
        if job['priority'] > element['priority']:
            index = key
            break
        elif job['priority'] == element['priority']:
            if int(job['pubDate']) <= int(element['pubDate']):
                index = key
                break
    if index == -1:
        conversionQueue.append(job)
    else:
        conversionQueue.insert(index, job)

def printQueue():
    """Prints the conversion queue to the terminal output"""
    log("### CURRENT QUEUE ###", 'green')
    for key, el in enumerate(conversionQueue):
        log(str(key+1) + "\t" + el['preset'] + "\t" + el['metadata']['pubDate'] + "\t" + str(el['priority']) + "\t" + el['path'][0], 'green')

def getRawFiles():
    """Fetches all the -raw files from the list generated using """
    startTime = time.time()
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
    log("getRawFiles took %f seconds" % (time.time() - startTime))
    return rawFiles

def checkFiles(force=False):
    """Updates the queue with the newest files"""
    global structure, rawFiles, fileList, conversionQueue, filesAddedYoutube, filesAdded, lastScanned,conversionObjs, quarantinedFiles

    quarantinedFiles = []

    if time.time() - lastScanned < scanInterval and not force:
        return False
    lastScanned = time.time()

    fileList, structure = scanStructure()
    log("scanStructure took %f seconds" % (time.time() - lastScanned))

    rawFiles = getRawFiles()

    illegalChars = "/?<>\*|”"

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
                    config = getConfig(file)
                    metadata = getMetadata(file)
                    if not metadata:
                        log("Missing metadata for file " + file, 'red')
                        continue
                    if metadata.get('quarantine'):
                        try:
                            quarantineTime = int(metadata.get('quarantine'))
                        except:
                            quarantineTime = 0

                        if time.time() - quarantineTime < 43200:
                            log(file+" is in quarantine!", "red")
                            quarantinedFiles.append({'name': file, 'reason': metadata.get('quarantineReason')})
                            continue
                        else:
                            log('Removing file '+ file + ' from quarantine', 'yellow')
                            removeQuarantine(file)

                    if any(e in os.path.basename(file) for e in illegalChars):
                        log("ILLEGAL CHARS IN " + file, "red")
                        continue

                    # Check if videos are to be uploaded to YouTube
                    if config.get('youtubeUpload') == True and not metadata.get('enotelms:YouTubeUID') and not file in filesAddedYoutube:
                        filesAddedYoutube.append(file)
                        youtube = config.get('youtube');
                        if youtube:
                            if (youtube.get('manualSelection') and metadata.get('youtubeUpload',"") == "true") or not youtube.get('manualSelection'):
                                if youtube.get('uploadVersion'):
                                    version = youtube.get('uploadVersion')
                                else:
                                    version = "720p"
                                config['youtube']['uploadVersion'] = version
                                if versionExists(file, localSuffix, version):
                                    alias = youtube.get('alias')

                                    playlist = youtube.get('playlist')
                                    if alias:
                                        filename = file.replace(localSuffix, version)

                                        for type in fileTypes:
                                            tFile = filename[:-3] + type
                                            if os.path.isfile(tFile):
                                                filename = tFile
                                                break;
                                        youtubeUpload.addToQueue(filename, youtube.copy())
                                    else:
                                        log('ERROR: No YouTube alias specified for file "{}"'.format(file))
                    # Check if videos are to be converted
                    if config.get("convert") == True:
                        reconverting = metadata.get('reconvert') == "true"
                        for format in config.get('formats'):
                            preset = config.get('presets').get(format)
                            if preset:
                                if not preset.get('fps'):
                                    preset['fps'] = fps
                                if not versionExists(file, localSuffix, preset.get('suffix')) or reconverting:
                                    if reconverting:
                                        log("Reconverting file " + file, 'yellow')
                                    with conversionQueueLock:
                                        if not jobQueued(file, preset.get('suffix')):
                                            if not metadata.get('pubDate'):
                                                log("Publishing date not set for file " + file, 'red')
                                            else:
                                                log("Added " + file + " in version " + format + " to queue")
                                                try:
                                                    timeValue = datetime.datetime.strptime(metadata.get('pubDate'), "%Y-%m-%d %H:%M").strftime("%s")
                                                except ValueError:
                                                    log("Invalid time format in " + file, 'red')
                                                else:
                                                    priority = int(config['presets'][format].get('priority'))
                                                    # Reduce priority for reconversions.
                                                    if reconverting:
                                                        priority -= 1000
                                                    
                                                    conversionJob = {"path": data, "files": rawFiles[data[1]], "options": preset,"preset": format, "config": config, "rawSuffix": localSuffix, "priority": priority, "metadata": metadata, "pubDate": timeValue, "extension": ext}
                                                    addToQueue(conversionJob)      
                            else:
                                log("Format '" + format + "' not found. Available ones are (" + ', '.join(format for format in config.get('presets')) + ")")
                        if metadata.get('reconvert'):
                            writeMetadata(data[0], {"reconvert": False})
                    # Generate thumbnails if missing
                    thumbnail = re.sub("-" + localSuffix + "\.(" + "|".join(fileTypes) + ")", "-1.png", file)
                    if not os.path.isfile(thumbnail):
                        info = videoConvert.videoInfo(file)

                        if not info or not info.get('length'):
                            thumbnailTime = 1 # Maybe a bit naive
                        else:
                            thumbnailTime = info.get('length')/3

                        try:
                            videoConvert.executeCommand("ffmpeg -ss "+str(thumbnailTime)+" -i '"+file+"' -vframes 1 -s 640x360 '"+thumbnail+"'", includeStderr=True)
                        except executeException:
                            log("Error generating thumbnail: " + thumbnail, 'magenta')
                        else:
                            log("Generated thumbnail: " + thumbnail , 'green')
    saveQueueText()
    saveQuarantineText()

def log(string, color="white"):
    """This method writes a string to the log file, and prints it to stdout"""
    now = datetime.datetime.now()
    date = "[" + str(now)[:19] + "] "
    message = str(string)
    logstr = date + message
    logstr = colored(date, 'cyan') + colored(message, color)
    if logging:
        pendingLog.append({"message": logstr, "color": color})
        try:
            while True:
                nextEntry = pendingLog.pop(0)
                if nextEntry['color'] in ["green", "white"]:
                    infoLog.write(nextEntry['message'] + "\n")
                    infoLog.flush()
                fullLog.write(nextEntry['message'] + "\n")
                fullLog.flush()
        except IndexError:
            pass
    else:
        pendingLog.append(logstr)
    print(colored(date, 'cyan'), colored(message, color))

# Find out if another instance of the script is running
def isRunning():
    pidfile="/tmp/convert.pid"
    try:
        pid = open(pidfile,'r').read().strip()
        if len(pid) == 0:
            raise IOError
        else:
            cmdline = open('/proc/' + str(pid) + '/cmdline','r').readline().strip("\0")
            if sys.argv[0] in cmdline:
                return True
            else:
                raise IOError
    except IOError:
        fp = open(pidfile, 'w')
        fp.write(str(os.getpid()))
        fp.close()
        return False

def anyVersionExists(file, rawSuffix):
    # Use system resources better. If conversions are too slow, lower conversionThreads parameter
    return False

    basename = re.split('-\w+\.\w+$',file)[0]
    for fileName in fileList:
        # Temporary workaround for faulty regex.
        if fileName[:len(basename)] == basename and fileName[len(basename)+1:-4] != rawSuffix and fileName[-3:] in fileTypes and not fileName.endswith('.new.m4v'):
            return True
        #if re.match(basename + "-(^(!?"+rawSuffix+")\w+)\.("+"|".join(fileTypes)+")", fileName):
        #   return True
    return False

def versionExists(file, localSuffix, suffix):
    """Returns true if the given file already exists in the given version"""
    for filetype in fileTypes:
        filename = re.sub(localSuffix+".+$",suffix, file) + "." + filetype
        try:
            if filename in fileList:
                return True
            elif os.path.isfile(filename) and os.stat(filename).st_ctime >= 10:
                return True
        except:
            log(traceback.format_exc(),'red')
    return False

def jobQueued(file, suffix):
    """Returns True if the file is already in the processing queue"""
    for job in conversionQueue:
        if job['path'][0] == file and job['options'].get('suffix') == suffix:
            return True
    if file in currentlyProcessing():
        return True
    return False

# Get the complete config array for a file located at "file"
def getConfig(file):
    parts = file[len(settings.get('podcastPath')):].split('/')[:-1]
    config = {}
    parent = structure
    for i in parts:
        try:
            config = dict(list(config.items()) + list(parent[i]['config'].items()))
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
    """Returns a dict of currently active conversion jobs"""
    converting = []
    for thread in conversionObjs:
        status = thread.currentlyConverting
        if status != False:
            converting.append(status)
    return converting


def setQuarantine(file, reason=False):
    """Set quarantine status on the file"""
    writeMetadata(file, {"quarantine": int(time.time()), "conversion": "failed", "quarantineReason": reason})

def removeQuarantine(file):
    """Remove quarantine status from file"""
    writeMetadata(file, {"quarantine": False, "quarantineReason": False})

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
    """This object handles a single conversion job. It """
    
    currentlyConverting = False

    # The drive letter in Wine correlating to linux "/"
    wineDrive = "z"
    
    def __init__(self, conversionJob):
        threading.Thread.__init__(self)
        self.job = conversionJob
        self.currentlyConverting = self.job['path'][0]
    

    def run(self):
        try:
            error = False
            writeMetadata(self.job['path'][0], {"conversion": "active"})

            self.job['duration'] = 0.0

            startOffset = self.job['metadata'].get('startOffset')
            endOffset   = self.job['metadata'].get('endOffset')
            
            self.job['originalDuration'] = []
            self.job['streams'] = []

            # Loop through all the files in the job (e.g. raw1, raw2,)
            for key in self.job['files']:
                info = self.videoInfo(element['files'][key])
                if not info: # Could not fetch video info. Perhaps corrupt file?
                    error = True
                    log("Error getting video info for " + self.job['files'][key], "red")
                    setQuarantine(self.job['path'][0], "Could not get video info")
                    break

                # Get start/end offsets . Default to 0.0s for both fields
                try:
                    soff = float(startOffset[key])
                except (IndexError, TypeError, ValueError) as e:
                    soff = 0.0
                try:
                    eoff = float(endOffset[key])
                except (IndexError, TypeError, ValueError):
                    eoff = 0.0
                

                self.job['originalDuration'].append(float(info['length']))

                self.job['duration'] += float(info['length']) - float(soff) - float(eoff)
                numStreams = info['videoStreams']
                width, height = info['width'], info['height']

                # Perform various checks. Fails if resolution is not 720p, or the file contains no streams
                if (width, height) != (1280, 720):
                    log("Videofile " + self.job['files'][key] + " has resolution " + str(width) + "x" + str(height) + " and was quarantined!", 'red')
                    setQuarantine(self.job['path'][0], "Video file has resolution %dx%d" % (width,height))
                    error = True
                if info['streams']['audio'] is None or info['streams']['video'] is None:
                    log("Couldn't find audio/video streams in file " + self.job['files'][key], 'red')
                    error = True
                    setQuarantine(self.job['path'][0], "Missing video/audio stream in file")
                else:
                    self.job['streams'].append(info['streams'])
            
            if not error: # Everything seems OK
                try: # Attempt the conversion
                    self.handleConversionJob(self.job)
                except Exception as e: # Conversion failed
                    error = True
                    setQuarantine(self.job['path'][0], str(e))
                    log(traceback.format_exc(), 'red')
                else: # Conversion was successful
                    writeMetadata(element['path'][0], {"conversion": False})
                    # Adding job to youtubeUpload's queue. Should probably be handled by a watcher thread instead
                    youtubeConfig = self.job['config'].get("youtube")
                    destination = re.sub(self.job['rawSuffix'], self.job['options']['suffix'],self.job['path'][0])

                    metadata = getMetadata(element['path'][0])
                    if youtubeConfig and self.job['config'].get('youtubeUpload') == True:
                          if (youtubeConfig.get('manualSelection') and metadata.get('youtubeUpload',"") == "true") or not youtubeConfig.get('manualSelection'):
                            if self.job.get("preset") == youtubeConfig.get("uploadVersion"):
                                destination = destination[:-3] + "mov"
                                youtubeUpload.addToQueue(destination, youtubeConfig)
            
            if error:
                # Send a mail to contactEmail as conversion failed
                if self.job['config'].get("contactEmail"):
                    sendErrorReport(self.job['path'][0], self.job['config'].get('contactEmail'))
            self.currentlyConverting = False
        except:
            log(traceback.format_exc(), 'red')

    def cleanup(self):
        writeMetadata(self.job['path'][0], {"conversion": False})
    
    def generateLTOverlay(self, title, subtitle, file):
        """Generate lower third overlay png file"""
        width, height = 875, 115

        font = os.path.join(settings.get('fontDir'), "NeoSansStd-Regular.otf")
        mediumFont = os.path.join(settings.get('fontDir'), "NeoSansStd-Medium.otf")

        textcolor = (27,65,132,255)
        strings = []
        strings.append([(10, 15), title, textcolor, 40, font])
        strings.append([(10, 65), subtitle, textcolor, 32, mediumFont])

        self.drawText((width, height), strings).save(file)

    def generateIntroOverlay(self, title, course, date, file, color, language):
        """Generate intro overlay"""

        width, height = 1280, 720
        titleOffsetX = 340

        img = Image.new("RGBA", (width, height), (0,0,0,0))
        draw = ImageDraw.Draw(img)
        titleFont = os.path.join(settings.get('fontDir'), "NeoSansStd-Medium.otf")
        titleFontEl = ImageFont.truetype(titleFont, 40, encoding='unic')
        regularFont = os.path.join(settings.get('fontDir'), "NeoSansStd-Regular.otf")

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

        videoLocale = "da_DK.UTF-8"
        if language.lower() == "english":
            videoLocale = "en_US.UTF-8"

        with setLocale(videoLocale):
            date = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M").strftime("%e. %B %Y")

        strings = []
        for key,part in enumerate(titleSegments):
            strings.append([(titleOffsetX, 430 + 45*key), part.upper(), color, 40, titleFont])
        strings.append([(titleOffsetX, 400), course.upper(), color, 24, regularFont])
        strings.append([(-20,-10), date, (204,204,204,255), 24, regularFont])

        self.drawText((width,height), strings).save(file)

    def generateOutroOverlays(self, producer, technician, lecturer, year, nodtubranding, file, language, license):
        """Generate the two outro overlays"""

        width, height = 1280, 720
        titleOffsetX = 340

        font = os.path.join(settings.get('fontDir'), "NeoSansStd-Regular.otf")
        fontsize = 44
        textcolor = (164,164,164,255)

        licenseFile = os.path.join(settings.get('scriptDir'), 'by-nc-nd.png')
        licenseImg = None
        
        if(os.path.isfile(licenseFile)) and license and license.lower() == "creative commons":
            licenseImg = Image.open(licenseFile)

        if nodtubranding:
            copyright = ""
        else:
            copyright = "\u00A9" + " "+year+" " + getLocalizedString('copyright', language)
        
        overlay1 = self.drawText((width, height), [[(-50, -40), copyright, textcolor, fontsize, font]])
        if licenseImg:
            overlay1.paste(licenseImg, (50, 625))
        overlay1.save(file + "1.png")

        strings = []

        ypos = 200
        if(lecturer):
            text = "%s: %s" % (getLocalizedString('presenter', language), lecturer)
            strings.append([(70, ypos), text, textcolor, fontsize-2, font])
            ypos += fontsize
        if(technician):
            text = "%s: %s" % (getLocalizedString('technician', language), technician)
            strings.append([(70, ypos), text, textcolor, fontsize-2, font])
            ypos += fontsize
        if(producer):
            text = "%s: %s" % (getLocalizedString('producer', language), producer)
            strings.append([(70, ypos), text, textcolor, fontsize-2, font])
            ypos += fontsize

        self.drawText((width, height), strings).save(file + "2.png")

    def drawText(self, imsize, strings):
        """Draw text on image"""
        # Inspired by
        # http://nedbatchelder.com/blog/200801/truly_transparent_text_with_pil.html

        alpha = Image.new("L", imsize, "black")
        img = Image.new("RGBA", imsize, (0,0,0,0))
        
        for pos, text, color, size, font in strings:
            imtext = Image.new("L", imsize, 0)
            text = str(text)

            draw = ImageDraw.Draw(imtext)
            fontObj = ImageFont.truetype(font, size=size)
            
            (offset_x, offset_y) = pos
            
            if offset_x < 0:
                offset_x = imsize[0] - draw.textsize(text, fontObj)[0] + pos[0]
            if offset_y < 0:
                pass
                offset_y = imsize[1] - draw.textsize(text, fontObj)[1] + pos[1]

            draw.text((offset_x, offset_y), text, font=fontObj, fill=255)
            #draw.flush()

            alpha = ImageChops.lighter(alpha, imtext)
            solidcolor = Image.new("RGBA", imsize, color)
            immask = Image.eval(imtext, lambda p: 255 * (int(p != 0)))
            img = Image.composite(solidcolor, img, immask)

        img.putalpha(alpha)

        return img
    
    @staticmethod
    def executeCommand(cmd,niceness=False, includeStderr=False):
        """Wrapper method to execute shell command. executeException raised on error"""
        if niceness:
            cmd = 'nice -n '+str(NICENESS)+' sh -c "' + cmd.replace('"','\'') + '"'
        if includeStderr:
            process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        else:
            process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=None)
        #stdout, stderr = process.communicate()
        stdout = b''
        while True:
            output = process.stdout.read(1)
            if output == b'' and process.poll() != None:
                break
            if output != b'':
                if len(sys.argv) > 1 and sys.argv[1] == '-v':
                    sys.stdout.write(output)
                sys.stdout.flush()
                stdout += output
        if process.returncode > 0:
            raise executeException({"returncode": process.returncode, "cmd": cmd})
        return stdout.decode('utf-8')

    # Routine to get basic information about a video file
    @staticmethod
    def videoInfo(file):
        try:
            infoDict = videoConvert.executeCommand("ffprobe \""+file+"\" -print_format json -show_format -show_streams 2>/dev/null")
            info = json.loads(infoDict)

            videoStreams = 0
            width, height = 0,0

            streams = {"video": None, "audio": None}
            
            audioStreams = []

            for stream in info.get("streams"):
                if stream.get("codec_type") == "video":
                    videoStreams += 1
                    if (stream.get("width"),stream.get("height")) == (1280,720):
                        width = stream.get("width")
                        height = stream.get("height")
                        streams['video'] = stream.get("index")
                elif stream.get("codec_type") == "audio":
                    audioStreams.append(stream.get("index"))
            
            if len(audioStreams) == 1:
                streams['audio'] = [audioStreams[0]]
            elif len(audioStreams) > 1:
                streams['audio'] = audioStreams[0:2]

            videoinfo = {
                "videoStreams": videoStreams,
                "height": int(height),
                "width": int(width),
                "length": float(info.get('format').get("duration")),
                "streams": streams
            }
        except executeException:
            return False
        except ValueError as e:
            return False
        except KeyError: # NO LARGE FILE SUPPORT IN EXIFTOOL!!!!!!
            return False 
        else:
            return videoinfo
    
    def winPath(self,unix_path):
        """Convert unix path to windows (wine) path"""
        return videoConvert.wineDrive + ":" + unix_path.replace("/","\\")


    def getCorrectedTime(self, time, remSecs):
        """Correct timestamps for removed sections in the video file"""
        newTime = time
        for remSec in remSecs:
            if time >= remSec['end']:
                newTime -= remSec['end'] - remSec['start']
            elif time > remSec['start']:
                return None
        return newTime

    def writeAvisynth(self,options):
        """Write the avisynth script for the conversion job"""
        path = self.winPath(options['path'][0])

        metadata = getMetadata(options['path'][0])
        if metadata == False:
            raise metadataException("No metadata file found!!")

        title = metadata.get('title').replace('\x0D', '')
        
        # Get course_id from .txt metadata if present, and otherwise from convertConfig
        course_id = metadata.get('course_id', options['config'].get('course_id', ' '))

        pubdate = metadata.get('pubDate')
        dirname = os.path.dirname(options['path'][0]) + "/"

        producer = metadata.get('producer')
        technician = metadata.get('technician')
        lecturer = metadata.get('performers')
        year = datetime.datetime.strptime(pubdate, "%Y-%m-%d %H:%M").strftime("%Y")

        language = metadata.get('language', 'Danish')
        license = metadata.get('license')

        if not license:
            license = "Creative Commons"

        nodtubranding = options['config'].get('nodtubranding')
        if not producer and not nodtubranding:
            producer = "LearningLab DTU"

        startOffset = metadata.get('startOffset')
        endOffset   = metadata.get('endOffset')

        removeSectionCmd = ""
        lowerThirdsCmd = ""
        
        # Many of the advanced editing features are only available with a single raw file present.
        if len(options['files']) == 1:
            removeSections = []
            duration = float(options['originalDuration'][0])

            removeSection = metadata.get('removeSection')
            if removeSection: 
                if len(removeSection)%2 == 0:
                    for n in range(len(removeSection)/2):
                        removeSections.append({"start": float(removeSection[(n-1)*2]), "end": float(removeSection[(n-1)*2+1])})
                else:
                    raise metadataException("Syntax error in removeSection")

            # In/out times for the first video file.
            try:
                inTime = float(startOffset[0])
            except (IndexError, TypeError, ValueError) as e:
                inTime = 0.0
            try:
                outTime = float(endOffset[0])
            except (IndexError, TypeError, ValueError):
                outTime = 0.0

            # Interpret start/end offset as removed sections for simplicity.
            removeSections.append({"start": 0.0, "end": inTime})
            removeSections.append({"start": duration - outTime, "end": duration})
            removeSections.sort(key=lambda x: x['start'])

            newRemoveSections = []

            # Logic for removing the sections desired
            for remSec in enumerate(removeSections):
                if remSec[1]['start'] < 0.0:
                    remSec[1]['start'] = 0.0
                if remSec[1]['end'] > duration:
                    remSec[1]['end'] = duration
                if remSec[1]['start'] > duration or remSec[1]['end'] < 0.0 or remSec[1]['start']==remSec[1]['end'] or remSec[1]['end'] == 0.0 or remSec[1] in newRemoveSections:
                    continue
                
                if len(newRemoveSections) == 0:
                    newRemoveSections.append(remSec[1])
                else:
                    if remSec[1]['start'] <= newRemoveSections[-1]['end']:
                        newRemoveSections[-1]['end'] = remSec[1]['end']
                    else:
                        newRemoveSections.append(remSec[1])

            for remSec in reversed(newRemoveSections):
                    removeSectionCmd += 'content = removeSection(content, '+str(remSec['start'])+', '+str(remSec['end'])+')\n'

            # Add lower thirds
            lowerThirdsData = metadata.get('lowerThirds')
            if lowerThirdsData:
                if len(lowerThirdsData)%3 == 0:
                    for n in range(len(lowerThirdsData)/3):
                        image = settings.get('scriptDir')+"Konverterede/" + options['path'][1] + '-lowerThird-'+str(n)+'.png';
                        self.generateLTOverlay(lowerThirdsData[(n-1)*3+1].replace(chr(9634),','), lowerThirdsData[(n-1)*3+2].replace(chr(9634),','), image)
                        newTime = self.getCorrectedTime(float(lowerThirdsData[(n-1)*3]), newRemoveSections)
                        if newTime != None:
                            lowerThirdsCmd += 'content = addLowerThird(content, '+str(newTime)+', "'+self.winPath(image)+'")\n'
                        else: 
                            log("Ignoring lowerthird at " + str(lowerThirdsData[(n-1)*3]) + "s : Invalid time!", 'blue')
                else:
                    raise metadataException("Syntax error in lowerThird declaration")



        if options['config'].get('branding') == True:
            enableLogo = True
            enableIntro = True
            enableOutro = True
        else:
            enableLogo = False
            enableIntro = False
            enableOutro = False

        setInt = options['config'].get('enableIntro')
        setOut = options['config'].get('enableOutro')
        setLogo = options['config'].get('enableLogo')

        if setInt != None:
            if setInt:
                enableIntro = True
            else:
                enableIntro = False

        if setOut != None:
            if setOut:
                enableOutro = True
            else:
                enableOutro = False

        if setLogo != None:
            if setLogo:
                enableLogo = True
            else:
                enableLogo = False

        if metadata.get('H264ProRecorderDynamicRangeCorrection') == "true":
            log("Applying H264 Recorder specific dynamic range correction to " + options['path'][0])
            H264Correction = True
        else:
            H264Correction = False

        # Set font color for the overlays
        c = options['config'].get('titleColor')
        if c and len(c) == 7:
            c = options['config'].get('titleColor')
            r = int(c[1:3],16)
            g = int(c[3:5], 16)
            b = int(c[5:7], 16)
            titleColor = (r,g,b, 255)
        else:
            titleColor = (27,65,132,255)

        # Determine the location of the intro/outro template movies. Defaults to defaultIntro/defaultOutro
        if os.path.isfile(dirname + "intro.mov"):
            intro = self.winPath(dirname + "intro.mov")
        else:
            intro = settings.get('defaultIntro')
        if os.path.isfile(dirname + "outro.mov"):
            outro = self.winPath(dirname + "outro.mov")
        else:
            outro = settings.get('defaultOutro')
        
        logo = metadata.get('logo')

        if logo:
            if logo == "false":
                logoOverlay = ""
                enableLogo = False
            elif os.path.isfile(dirname + "logo" + logo + ".png"):
                logoOverlay = self.winPath(dirname + "logo" + logo + ".png")
        else:
            logoOverlay = settings.get('defaultLogo')

        # If everything is allright, continue
        if title and course_id and pubdate:
            self.generateIntroOverlay(title, course_id, pubdate, settings.get('scriptDir')+"Konverterede/" + options['path'][1] + '-introOverlay.png', titleColor, language)
            self.generateOutroOverlays(producer, technician , lecturer, year, nodtubranding, settings.get('scriptDir')+"Konverterede/" + options['path'][1] + '-outroOverlay', language, license)
            template = open(settings.get('scriptDir') + "video-convert/avisynth.avs", 'r').read()
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

                if len(options['files']) > 1:
                    videoList += "addVideoClip(\"" + self.winPath(options['files'][i]) + "\","+str(soff)+","+str(eoff)+")"
                else:
                     videoList += "addVideoClip(\"" + self.winPath(options['files'][i]) + "\", 0.0, 0.0)"
                if i != len(options['files'])-1:
                    videoList += " ++ "

            # Fill the avisynth.avs template file with the actual parameters
            template = template.format(
                intro = intro, 
                video = path, 
                outro = outro, 
                title=title, 
                course=course_id, 
                date=pubdate,
                enableIntro=enableIntro,
                enableOutro=enableOutro,
                addLogo=enableLogo, 
                videoList=videoList, 
                correctH264Levels=H264Correction,
                fps=int(options['options']['fps']),
                introoverlay=self.winPath(settings.get('scriptDir')+"Konverterede/" + options['path'][1] + '-introOverlay.png'),
                outrooverlay1=self.winPath(settings.get('scriptDir')+"Konverterede/" + options['path'][1] + '-outroOverlay1.png'),
                outrooverlay2=self.winPath(settings.get('scriptDir')+"Konverterede/" + options['path'][1] + '-outroOverlay2.png'),
                logoOverlay=logoOverlay,
                lowerThirdList=lowerThirdsCmd,
                removeSection=removeSectionCmd,
                nametag=settings.get('nametagPath')
                )
            script = open(settings.get('scriptDir')+"Konverterede/" + options['path'][1] + "-"+ options['options']['suffix'] + '.avs', 'wb')
            script.write(template.encode('latin-1', 'ignore'))
            script.close
            return template
        else:
            raise metadataException("Either title, course_id or pubdate missing")

    def handleConversionJob(self,conversionJob):
        """Runs the conversion job"""
        rawFiles = conversionJob['files']
        options = conversionJob['options']

        missingOptions = validateList(options, ["width", "height", "quality", "suffix", "audiobitrate", "fps"])
        if missingOptions.__len__() > 0:
            raise metadataException("Missing options: " + ", ".join(missingOptions) + " for file " + rawFiles[0])

        conversionJob['outputFile'] = settings.get('scriptDir')+"Konverterede/" + conversionJob['path'][1] + "-"+ conversionJob['options']['suffix']
        outputFile = conversionJob['outputFile']
        finalDestination = re.sub(conversionJob['rawSuffix']+"\..+", conversionJob['options']['suffix'] + "." + conversionJob['extension'],conversionJob['path'][0])
        if os.path.isfile(finalDestination) and not conversionJob['metadata'].get('reconvert') == "true":
            raise metadataException("File " + finalDestination + " already exists!")
        
        success = False
        convertLog = ""
        outputFile = outputFile + "." + conversionJob['extension']
        if os.path.isfile(outputFile):
            log("Removed outputFile prior to encoding ...")
            os.remove(outputFile)

        try:
            log("Avisynth conversion of " + rawFiles[0] + " to " + conversionJob['preset'])
            log("Job consists of " + str(len(rawFiles)) + " raw files")
            convertLog = self.avisynthConversion(conversionJob)
        except metadataException as e:
            raise metadataException("Error '"+str(e)+"' for file " + rawFiles[0], 'red')
        except executeException as e:
            print("error")
            print(e)
            log("Encoding of " + outputFile + " failed!", 'red')
            raise
        else:
            if os.path.isfile(outputFile):
                log("Encoding of " + outputFile + " succeded!", 'green')
                shutil.move(outputFile, finalDestination)
                success = True
            else:
                log("Encoding of " + outputFile + " failed (no output file)!", 'red')

        if convertLog:
            fp = open(outputFile.replace("." + conversionJob['extension'],".log"), "w")
            fp.write(convertLog)
            fp.close()

        if not success:
            raise Exception("Internal error in handleConversionJob")
    
    def writeFFmetadata(self,options):
        """Write an ffmeta file with chapter information. This info is incorporated into the converted videos"""
        path = self.winPath(options['path'][0])
        metadata = getMetadata(options['path'][0])

        totalDuration = options['duration']

        ffmeta  = ";FFMETADATA1\n"
        ffmeta += "title=" + metadata.get('title') + "\n\n"
        ffmeta += "artist=LearningLab DTU\n"

        pattern = re.compile("(\d{2}:\d{2}) - ([^;]+);")
        description = metadata.get('description')
        if description:
            mat = pattern.findall(description);
            for key,value in enumerate(mat):
                timeparts = value[0].split(":")
                try:
                    nexttime = mat[key+1]
                except IndexError:
                    nextseconds = options['duration']
                else:
                    nexttimeparts = nexttime[0].split(":")
                    nextseconds = int(nexttimeparts[0]) * 60 + int(nexttimeparts[1])

                seconds = int(timeparts[0]) * 60 + int(timeparts[1])

                if nextseconds > totalDuration or seconds > nextseconds:
                    continue
                
                ffmeta += "[CHAPTER]\n"
                ffmeta += "TIMEBASE=1/1000\n"
                ffmeta += "START=" + str(seconds*1000) + "\n"
                ffmeta += "END=" + str(nextseconds * 1000) + "\n" 
                ffmeta += "title=" + value[1] + "\n"

            script = open(settings.get('scriptDir')+"Konverterede/" + options['path'][1] + "-"+ options['options']['suffix'] + '.ffmeta', 'wb')
            script.write(ffmeta.encode('utf-8'))
            script.close
            return True
        else:
            return False

    def avisynthConversion(self, job):
        """Conversion using avisynth, utilizing branding and intro/outro videos"""
        
        options = job['options']
        streams = job['streams']
        inputFile = job['path'][0]
        outputFile = job['outputFile'] + "." + job['extension']
        audioFile = job['outputFile'] + '.wav'
        videoFile = job['outputFile'] + '.264'
        avsScript = job['outputFile'] + '.avs'
        execLog = ""
        log('Repacking file with ffmpeg, just to be sure.. ('+outputFile+')', 'blue')
        newlist = {}
        index = 0
        for key in job['files']:
            file = job['files'][key]
            stream = streams[index]
            if len(stream['audio']) == 1:
                execLog += self.executeCommand("ffmpeg -y -i \"" + file + "\" -map 0:"+str(stream['video'])+" -map 0:"+str(stream['audio'][0])+" -c:a copy -c:v copy \"" + file + ".new." + job['extension'] + "\"", includeStderr=True)
            else:
                execLog += self.executeCommand("ffmpeg -y -i \"" + file + "\" -map 0:"+str(stream['video'])+"  -filter_complex '[0:"+str(stream['audio'][0])+"][0:"+str(stream['audio'][1])+"]amerge[aout]' -map '[aout]' -c:a pcm_s24le -c:v copy \"" + file + ".new." + job['extension'] + "\"", includeStderr=True)
            newlist[key] = file + ".new." + job['extension']
            index += 1
        
        job['files'] = newlist

        self.writeAvisynth(job)
        
        if self.writeFFmetadata(job):
            ffmetaFile = job['outputFile'] + '.ffmeta'
        else:
            ffmetaFile = None
        

        # Run all the conversion steps.
        # 1. Audio processing
        # 2. Video processing
        # 3. Muxing with ffmpeg
        try:
            log('Running avs2pipemod audio.. ('+outputFile+')', 'blue')
            execLog += self.executeCommand("wine avs2pipemod -wav \"" + avsScript + "\" > \"" + audioFile + "\"", niceness=True, includeStderr=True)
            log('Running avs2pipemod video.. ('+outputFile+')', 'blue')
            execLog += self.executeCommand("wine avs2pipemod -y4mp \""+ avsScript +"\" | x264 --fps "+str(int(options['fps']))+" --threads 2 --stdin y4m --output \""+videoFile+"\" --bframes 0 -q "+str(options['quality'])+" --video-filter resize:"+str(options['width'])+","+str(options['height'])+" -", niceness=True, includeStderr=True)
            log('Muxing with ffmpeg.. ('+outputFile+')', 'blue')
            if ffmetaFile:
                execLog += self.executeCommand("ffmpeg -y -r "+str(int(options['fps']))+" -i \""+videoFile+"\" -i \""+audioFile+"\" -i \"" +ffmetaFile+ "\" -map_metadata 2 -vcodec copy -strict -2 \""+outputFile+"\" ", niceness=True, includeStderr=True)
            else:
                execLog += self.executeCommand("ffmpeg -y -r "+str(int(options['fps']))+" -i \""+videoFile+"\" -i \""+audioFile+"\" -vcodec copy -strict -2 \""+outputFile+"\" ", niceness=True, includeStderr=True)
        except Exception:
            raise
        finally:
            log('Cleaning up.. ('+outputFile+')', 'blue')
            if os.path.isfile(audioFile):
                os.remove(audioFile)
            if os.path.isfile(videoFile):
                os.remove(videoFile)
            for file in newlist:
                if os.path.isfile(newlist[file]):
                    os.remove(newlist[file])
        
        return execLog

    # Legacy conversion using Handbrake. As of now faster and perhaps more reliable than avisynth.
    def handbrakeConversion(self, job):
        log("Handbrake not supported! QUARANTINED", 'red')
        setQuarantine(job['path'][0])
        return "HANDBRAKE NOT INSTALLED!"

        options = job['options']
        handBrakeArgs = "-e x264 -q " + str(options['quality']) + " -B " + str(options['audiobitrate']) + " -w " + str(options['width']) + " -l " + str(options['height'])  
        cmd = HandBrakeCLI + " --cpu " + str(CPUS) + " " + handBrakeArgs + " -r "+str(fps)+" -i '" + job['path'][0] + "' -o '" + job['outputFile'] + "."+job['extension']+"'"
        return self.executeCommand(cmd, niceness=True, includeStderr=True)


##########################################
########### YOUTUBEUPLOAD ################
##########################################

class youtubeUpload (threading.Thread):
    """Handles youtube uploads"""
    queue = deque()
    def run(self):
        while not mainThreadDone or self.queue.__len__() > 0:
            if self.queue.__len__() > 0:
                element = self.queue.popleft()
                metadata = getMetadata(element['filename'])
                log("Attempting YouTube upload of '%s'" % element['filename'], 'green')
                if metadata.get("enotelms:YouTubeUID"):
                    log("Video is already on YouTube: " + element['filename'], 'yellow')
                    continue
                else:
                    try:
                        credentials = self.getCredentials(element['alias'])
                        if not credentials:
                            log("ERROR: No YouTube credentials found for '{}'".format(element['alias']), 'red')
                            continue
                        self.youtube = self.authenticate(element['alias'])
                    except:
                        log(traceback.format_exc(),'red')
                    else:
                        video_id = self.uploadFromMetaData(element, metadata)
                        if video_id != False:
                            writeMetadata(element['filename'],{"enotelms:YouTubeUID": video_id})
                        else:
                            log("Youtube upload failed!", 'red')
            time.sleep(0.5)
        log("Main thread exited, terminating youtubeUpload...")

    def getCredentials(self, username):
        users = json.loads(open(settings.get('youtubeAuth'), 'r').read())
        userData = users.get(username)
        if userData and userData.get('credentials'):
            return GoogleCredentials.from_json(userData.get('credentials'))

        return False

    # Upload video processing metadata
    def uploadFromMetaData(self, preferences, metadata):
        log('Uploading "' + preferences['filename'] + '"')
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
            metadata['itunes:keywords'] = settings.get('defaultYoutubeKeywords')
            log("WARNING: No keywords specified for file: " + preferences['filename'] + "!", 'yellow')

        key = ""
        for i in metadata['itunes:keywords'].split(" "):
            if len(i) >= 2:
                key += i + ", "

        metadata['description'] = metadata['description'].replace("<", "←")
        metadata['description'] = metadata['description'].replace(">", "→")


        
        options = {
            "title": metadata['title'][:100],
            "description": metadata['description'],
            "keywords": key[:-2],
            "private": private,
            "path": preferences['filename'],
            "category": preferences.get('category')
        }

        try:
            video_id = self.uploadVideo(options)
        except:
            log(traceback.format_exc(), 'red')
            return False
        else:
            if playlist:
                try:
                    self.addToPlaylist(video_id, playlist)
                except:
                    log(traceback.format_exc(),'red')
                    log("Some error occured adding the video to youtube playlist..", 'red')
                    #return False
            return video_id

    @staticmethod
    def addToQueue(filename, options):
        options['filename'] = filename
        youtubeUpload.queue.append(options)

    def authenticate(self, username):
        YOUTUBE_SCOPE = "https://www.googleapis.com/auth/youtube"

        credentials = self.getCredentials(username)
        if not credentials:
            return False

        flow = flow_from_clientsecrets(settings.get("youtubeSecrets"), scope=YOUTUBE_SCOPE)
        return build("youtube", "v3", http=credentials.authorize(httplib2.Http()))

    def retrievePlaylists(self):
        playlists = {}
        pageToken = None
        response = None

        while True:
            fetchedAll = True
            try:
                response = self.youtube.playlists().list(part='snippet', maxResults=50, pageToken=pageToken, mine=True).execute()
                pageToken = response.get('nextPageToken')
                if pageToken:
                    fetchedAll = False
                if len(playlists) == response['pageInfo']['totalResults']:
                    break
            except:
                log(traceback.format_exc(),'red')
                break
            else:
                for item in response.get('items'):
                    title = item.get('snippet').get('localized').get('title')
                    playlists[title] = item.get('id')

        return playlists
    
    def addPlaylist(self, playlist):
        try:
            res = self.youtube.playlists().insert(
              part="snippet,status",
              body=dict(
                snippet=dict(
                  title=playlist
                ),
                status=dict(
                    privacyStatus='public'
                )
              )
            ).execute()
        except:
            log(traceback.format_exc(),'red')
            return False
        else:
            return res['id']

    def uploadVideo(self, options):

        # Retriable exceptions and status codes from youtube example code

        # Always retry when these exceptions are raised.
        RETRIABLE_EXCEPTIONS = (httplib2.HttpLib2Error, IOError, http.client.NotConnected,
  http.client.IncompleteRead, http.client.ImproperConnectionState,
  http.client.CannotSendRequest, http.client.CannotSendHeader,
  http.client.ResponseNotReady, http.client.BadStatusLine)

        # Always retry when an apiclient.errors.HttpError with one of these status
        # codes is raised.
        RETRIABLE_STATUS_CODES = [500, 502, 503, 504]

        body = dict(
            snippet = dict(
                title = options['title'],
                description = options['description'],
                tags = None,
                categoryId = 27 # Education, hardcoded for now
            ),
            status = dict(
                privacyStatus = 'private' if options['private'] else 'public'
            )
        )

        part = ",".join(body.keys())

        try:
            request = self.youtube.videos().insert(part=part, body=body, media_body=MediaFileUpload(options['path'], chunksize=1024*1024*100, resumable=True))
            response = None
            retries = 0
            while response is None:
                try:
                    status, response = request.next_chunk()
                    if response is not None:
                        if 'id' in response:
                            return response['id']
                        else:
                            return False
                except HttpError as e:
                    if e.resp.status in RETRIABLE_STATUS_CODES:
                        log("Got HTTP Error %d: %s" % (e.resp.status, e.content), 'yellow')
                        retries += 1
                    else:
                        raise
                except RETRIABLE_EXCEPTIONS as e:
                    log("Retriable exception %s" % e, 'yellow')
                    retries += 1

                if retries > 10:
                    log("ERROR: Retried 10 times uploading {}, bailing".format(options['file']), 'red')
                    return False
        except:
            log(traceback.format_exc(),'red')
            return False

    def addToPlaylist(self, video_id, playlist):
        playlists = self.retrievePlaylists()
        if playlist in playlists:
            playlist_id = playlists[playlist]
        else:
            playlist_id = self.addPlaylist(playlist)
    
        if video_id and playlist_id:
            try:
                res = self.youtube.playlistItems().insert(
                    part='snippet', 
                    body=dict(
                        snippet=dict(
                            resourceId=dict(
                                kind='youtube#video',
                                videoId=video_id),
                            playlistId=playlist_id,
                        )
                    )
                ).execute()
            except Exception as e:
                log(traceback.format_exc(),'red')
                print(e.content)
                log("Video NOT added to playlist", 'red')
            else:
                log("Video added to playlist '" + playlist + "'", 'green')


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

checkFiles()

youtube = youtubeUpload()
youtube.start()

lastCount = 0

# This loop runs as long as there are conversion jobs
while 1:
    try:
        if conversionObjs.__len__() == 0 and conversionQueue.__len__() == 0:
            log("No more work to do - exiting main loop")
            break
        with convertObjLock:
            for thread in conversionObjs:
                if not thread.is_alive():
                    thread.cleanup()
                    conversionObjs.remove(thread)
            
            forceUpdateFile = os.path.join(settings.get('podcastPath'), 'CHECK_FILES')
            if conversionObjs.__len__() != lastCount:
                checkFiles(force=True)
            elif os.path.isfile(forceUpdateFile):
                updateState = ""
                with open(forceUpdateFile, 'r+') as f:
                    updateState = f.read();
                    f.seek(0)
                    f.truncate()
                    f.write("FALSE")
                if updateState == "TRUE":
                    log("Forced update of files from website..", 'blue')
                    checkFiles(force=True)
            
            with conversionQueueLock:
                if conversionObjs.__len__() != conversionThreads and conversionQueue.__len__() > 0:
                            if conversionQueue[0]['path'][0] not in currentlyProcessing() and not (anyVersionExists(conversionQueue[0]['path'][0], conversionQueue[0]['rawSuffix']) and conversionObjs.__len__() > 0):
                                log("Currently "+str(conversionQueue.__len__()) + " items queued for conversion.")
                                printQueue()
                                element = conversionQueue.pop(0)

                                log("Created videoConvert object.")
                                vidConv = videoConvert(element)
                                conversionObjs.append(vidConv)
                                vidConv.start()
        lastCount = conversionObjs.__len__()
        time.sleep(1)
    except:
        log(traceback.format_exc(),'red')

mainThreadDone = True   
