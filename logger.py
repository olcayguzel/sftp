import os
import enum
import datetime
import sys
from const import LOG_PATTERN, LOG_FOLDER_PATH, MAX_LOG_FILE_SIZE

class LogTypes(enum.Enum):
	INFO = 1
	DEBUG = 2
	ERROR = 3
	FATAL = 4

class OutputTargets(enum.Enum):
	FILE = 0,
	STDOUTPUT = 1
	BOTH = 2

class Logger:
	def __init__(self):
		self.__pwd = os.getcwd()
		self.__filename = "filecollector.log"
		self.__output_folder = LOG_FOLDER_PATH
		self.__outputTo:OutputTargets = OutputTargets.BOTH # 0 -> FILE		1-> STD OUTPUT

	def changeoutputfolder(self, folder):
		self.__output_folder = folder

	def changeoutput(self, target:OutputTargets):
		if target == OutputTargets.STDOUTPUT:
			self.__outputTo = OutputTargets.STDOUTPUT.value
		elif target == OutputTargets.FILE:
			self.__outputTo = OutputTargets.FILE.value
		elif target == OutputTargets.BOTH:
			self.__outputTo = OutputTargets.BOTH.value
		else:
			self.write(LogTypes.ERROR, "Unsupported output target")

	def __createfolder(self, folder):
		try:
			if os.path.exists(os.path.join(self.__pwd, folder)) == False:
				os.mkdir(os.path.join(self.__pwd, folder))
		except Exception as ex:
			print(ex)

	def write(self, logtype:LogTypes, message, host = ""):
		self.__createfolder(self.__output_folder)
		now = datetime.datetime.now()
		logtime = now.strftime("%Y-%m-%d %H:%M:%S")
		logmessage = LOG_PATTERN.replace("[TIME]", logtime)
		logmessage = logmessage.replace("[HOST]", host)
		logmessage = logmessage.replace("[TYPE]", logtype.name)
		logmessage = logmessage.replace("[MESSAGE]", str(message))

		if self.__outputTo == OutputTargets.FILE:
			self.__writeToFile(logmessage)
		elif self.__outputTo == OutputTargets.STDOUTPUT:
			if sys.stderr.buffer.writable():
				sys.stderr.buffer.write(bytes(logmessage.encode("utf-8")))
				sys.stderr.buffer.flush()
		elif self.__outputTo == OutputTargets.BOTH:
			self.__writeToFile(logmessage)
			if sys.stderr.buffer.writable():
				sys.stderr.buffer.write(bytes(logmessage.encode("utf-8")))
				sys.stderr.buffer.flush()

	def __checklogfile(self):
		path = os.path.join(self.__pwd, self.__output_folder, self.__filename)
		if os.path.exists(path):
			stat = os.stat(path)
			size = 0
			if stat is not None:
				size = stat.st_size
				size = size / 1024 / 2024
			if size >= MAX_LOG_FILE_SIZE:
				now = datetime.datetime.now()
				os.rename(path, os.path.join(self.__pwd, self.__output_folder, now.strftime("%Y%m%d_%H%M%S") + ".log"))

	def __writeToFile(self, message:str):
		fd = None
		try:
			self.__checklogfile()
			fd = open(os.path.join(self.__pwd, self.__output_folder, self.__filename), "a")
			if fd.writable():
				fd.write(message)
				fd.flush()
		except Exception as ex:
			print(ex)
		finally:
			if fd is not None and fd.closed == False:
				fd.close()
