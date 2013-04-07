#!/usr/bin/python2.6
# -*- coding: utf-8 -*-

# Copyright (c) 2013 Filip Sandborg-Olsen <filipsandborg@gmail.com>

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

import re, os, fcntl

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
			parts = [x.strip() for x in line.split('=', 1)]
			if len(parts) == 2 and not parts[1].startswith('['):
				submatch = re.search('^{(.+)}$', parts[1])
				if submatch:
					metadata[parts[0]] = submatch.group(1).split(',')
				else:
					metadata[parts[0]] = parts[1]
		return metadata
	else:
		return False

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
		try:
			with open(metafile, 'w') as f:
				fcntl.flock(f, fcntl.LOCK_EX)
				f.writelines(lines)
				f.close()
		except IOError:
			print("Couldn't write metadata")
			return False
		else:
			return True