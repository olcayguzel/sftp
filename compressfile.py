import gzip

class CompressFile:
	def __init__(self):
		self.__filename:str = ""
		self.__outputfilename:str = ""
		self.__zip:gzip.GzipFile = None
		self.__level:int = 5
	@property
	def filename(self):
		return  self.__filename

	@filename.setter
	def filename(self, value:str):
		self.__filename = value

	@property
	def outputfilename(self):
		return  self.__outputfilename

	@property
	def level(self):
		return  self.__level

	@level.setter
	def level(self, value:int):
		self.__level = value

	def compress(self):
		fd = None
		try:
			fd = open(self.__filename, "rb")
			self.__outputfilename = f"{self.__filename}.gzip"
			self.__zip = gzip.GzipFile(filename= self.__outputfilename, mode = "wb", compresslevel = self.__level)
			self.__zip.write(fd.read())
		finally:
			if fd is not None:
				fd.close()
			if self.__zip is not None:
				self.__zip.close()
				self.__zip = None