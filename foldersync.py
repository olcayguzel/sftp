import os
import time
from configuration import Config
from watcher import Watcher
from compressfile import CompressFile
from logger import Logger, LogTypes
from const import DAT_FILE_NAME


class FolderSync:
    def __init__(self):
        self.__log: Logger = Logger()
        self.__config: Config = Config()
        self.__watch: Watcher == Watcher()
        self.__lastprocessedfiledate = 0
        self.__lastprocessedfilename = ""

    def __getlastprocessedfiles(self):
        date: 0
        name: ""
        fd = None
        result = dict()
        try:
            if os.path.exists(DAT_FILE_NAME):
                fd = open(DAT_FILE_NAME)
                if fd.readable():
                    for line in fd.readlines():
                        if len(line) > 0:
                            data = line.split("|")
                            print(data)
                            host = str(data[0])
                            folder = str(data[1])
                            name = str(data[2])
                            date = str(data[3])
                            if host not in result:
                                result[host] = dict()
                            if folder not in result[host]:
                                result[host][folder] = (name, date)
                else:
                    self.__log.write(LogTypes.DEBUG,
                                     "Could not detect last processed file. All files in folder will be enqueue to processed")
            else:
                self.__log.write(LogTypes.INFO,
                                 "Could not detect last processed file(s). All files in folder will be enqueue to processed")
        except Exception as ex:
            self.__log.write(LogTypes.ERROR, ex)
        finally:
            if fd is not None:
                fd.close()
        return result

    def __getnewfiles(self):
        cdrfiles = dict()
        with os.scandir(self.__config.InputFolder) as files:
            for file in files:
                stats = file.stat(follow_symlinks=False)
                if stats.st_ctime_ns > self.__lastprocessedfiledate and file.name.__eq__(
                        self.__lastprocessedfilename) == False:
                    cdrfiles[file.name] = stats.st_ctime_ns
        cdrfiles = list(sorted(cdrfiles.items(), key=lambda t: t[1]))
        for file in cdrfiles:
            self.__sendfile(os.path.join(self.__config.InputFolder, file[0]))
            time.sleep(10)

    def __compressfile(self, file):
        compress = CompressFile()
        compress.filename = file
        compress.level = 9
        compress.compress()
        return compress.outputfilename

    def __sendfile(self, file):
        if len(self.__config.RemoteHosts) > 0:
            for host in self.__config.RemoteHosts:
                if host.Compression:
                    file = self.__compressfile(file)
                host.addtoqueue(file)

    def __watch(self):
        self.__watch.folder = self.__config.InputFolder
        self.__watch.onnew = self.__sendfile
        self.__watch.start()

    def __setlastprocessedfiles(self):
        filenames = self.__getlastprocessedfiles()
        last_date = -1
        file_name = ""
        if len(filenames) > 0:
            for host in self.__config.RemoteHosts:
                if host.Address in filenames:
                    if host.RemotePath in filenames[host.Address]:
                        name, date = filenames[host.Address][host.RemotePath]
                        host.setlastprocessedfile = (name, date)
                        if last_date == -1 or last_date > date:
                            last_date = date
                            file_name = name
            if last_date > 0:
                self.__lastprocessedfiledate = last_date
                self.__lastprocessedfilename = file_name

    def initialize(self):
        self.__config.load(filename="./config.json")
        self.__config.log = self.__log
        if self.__config.validate():
            self.__setlastprocessedfiles()
            self.__getnewfiles()
            self.__watch()


if __name__ == '__main__':
    syncer = FolderSync()
    syncer.initialize()
