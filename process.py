from subprocess import Popen, PIPE
from threading import Thread
class Process:
	def __init__(self):
		self.__path:str = ""
		self.__arguments:str = ""
		self.__process = None
        self.__pid: int = 0
        self.__workeroutput = None
        self.__workererror = None
        self.__outputhandler: callable(str) = None
        self.__errorhandler: callable(str) = None

    @property
    def path(self):
        return self.__path

    @property
    def exitcode(self) -> int:
        code = -1
        if self.__process is not None:
            code = self.__process.returncode
        return code

    @path.setter
    def path(self, value: str):
        self.__path = value

    @property
    def arguments(self):
        return self.__arguments

    @arguments.setter
	def arguments(self, value:str):
		self.__arguments = value

	@property
	def onerror(self):
		return self.__errorhandler

	@onerror.setter
	def onerror(self, value:callable(str)):
		self.__errorhandler = value

	@property
	def onoutput(self):
		return self.__outputhandler

	@onoutput.setter
	def onoutput(self, value: callable(str)):
		self.__outputhandler = value

	def __watchoutput(self):
		if self.__process is not None:
			if self.__process.stdout is not None and not self.__process.stdout.closed:
				if self.__process.stdout.readable():
					for line in iter(self.__process.stdout):
						if self.__outputhandler is not None:
                            print("Output:", line.strip())
                            self.__outputhandler(line.strip())

	def __watcherror(self):
		if self.__process is not None:
			while self.__process.stderr is not None and not self.__process.stderr.closed:
				if self.__process.stderr.readable():
					line = self.__process.stderr.readline()
					if self.__errorhandler is not None:
                        print("Output:", line.strip())
                        self.__errorhandler(line)
					if self.__process.poll():
						break

	def __execute(self, program, arguments):
        print(program, arguments)
		self.__workeroutput = Thread(target = self.__watchoutput, daemon= True)
        self.__workererror = Thread(target=self.__workererror, daemon=True)
        self.__process = Popen(executable=program, args=arguments, stdout=PIPE, stderr=PIPE, universal_newlines=True,
                               shell=True)
		self.__workeroutput.start()
		self.__workererror.start()
		self.__pid = self.__process.pid

	def kill(self):
		if self.__pid > 0:
			self.__process.kill()

	def start(self, restart:bool = True):
		if restart:
			self.kill()
		self.__execute(self.__path, self.__arguments)

	def wait(self, timeout:int = None):
		if self.__process is not None:
			self.__process.wait(timeout)

