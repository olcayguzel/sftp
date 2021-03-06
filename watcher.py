import datetime
import os
import threading
import time

class Watcher:
	def __init__(self):
		self.__folder:str = os.getcwd()
		self.__onnewhandler = None
		self.__worker:threading.Thread = None
		self.__running:bool = False
		self.__lastchecktime:datetime = time.time_ns()

	@property
	def running(self) -> bool:
		return self.running

	@property
	def folder(self) -> str:
		return self.__folder

	@folder.setter
	def folder(self, value:str):
		self.__folder = value

	@property
	def onnew(self) -> callable(str):
		return self.__onnewhandler

	@onnew.setter
	def onnew(self, func:callable(str)):
		self.__onnewhandler = func

	def __start(self):
		if not os.path.exists(self.__folder):
			self.__running = False

		while self.__running:
			if self.__onnewhandler is None or not callable(self.__onnewhandler):
				continue
			checktime = self.__lastchecktime
			with os.scandir(self.__folder) as files:
				for file in files:
					stats = file.stat(follow_symlinks = False)
					if stats.st_ctime_ns > checktime:
						if stats.st_ctime_ns > self.__lastchecktime:
							self.__lastchecktime = stats.st_ctime_ns
						self.__onnewhandler(file.name)
			time.sleep(1)

	def start(self):
		if not self.__running:
			try:
				self.__worker = threading.Thread(target = self.__start)
				self.__running = True
				self.__worker.start()
			except Exception as ex:
				print(ex)
				self.__running = False

	def stop(self):
		self.__running = False
		self.__worker = None