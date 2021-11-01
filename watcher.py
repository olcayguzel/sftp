import datetime
import os
import time
from threading import Thread
from pathlib import Path
from configuration import Config

class Watcher:
	def __init__(self):
		self.__folder:str = os.getcwd()
		self.__onnewhandler = None
		self.__worker:Thread
		self.__running:bool = False
		self.__lastchecktime:datetime = time.time()
		self.__config = None

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
	def config(self) -> str:
		return self.__config

	@config.setter
	def config(self, value:Config):
		self.__config = value

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
			for file in Path(self.__config.InputFolder).glob(self.__config.InputFilePattern):
					stats = os.stat(os.path.join(self.__config.InputFolder, file.name))
					if stats.st_ctime_ns > checktime:
						if stats.st_ctime_ns > self.__lastchecktime:
							self.__lastchecktime = stats.st_ctime_ns
						self.__onnewhandler(os.path.join(self.__config.InputFolder, file.name))
			time.sleep(1)

	def start(self):
		if not self.__running:
			try:
                self.__worker = Thread(target=self.__start, daemon=True)
                self.__running = True
                self.__worker.start()
            except Exception as ex:
                print(ex)
                self.__running = False

    def stop(self):
        self.__running = False
        self.__worker = None

    def join(self):
        if self.__worker is not None and self.__running:
            self.__worker.join()
