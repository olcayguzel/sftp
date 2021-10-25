import os
import time
from configuration import Config
from watcher import Watcher
from process import Process
from compressfile import CompressFile

config = Config()
watcher = Watcher()

def getnewfiles():
    cdrfiles = dict()
    date = 0
    name = ""
    with os.scandir(config.InputFolder) as files:
        for file in files:
            stats = file.stat(follow_symlinks=False)
            if stats.st_ctime_ns > date and file.name.__eq__(name) == False:
                cdrfiles[file.name] = stats.st_ctime_ns
    cdrfiles = list(sorted(cdrfiles.items(), key=lambda t: t[1]))
    for file in cdrfiles:
        sendfile(os.path.join(config.InputFolder, file[0]))
        time.sleep(10)

def compressfile(file):
    compress = CompressFile()
    compress.filename = file
    compress.level = 9
    compress.compress()
    return compress.outputfilename


def sendfile(file):
    if len(config.RemoteHosts) > 0:
        for host in config.RemoteHosts:
            if host.Compression:
                file = compressfile(file)
            host.addtoqueue(file)

def printnewfile(file:callable(str)):
    sendfile(file)

def watch():
    watcher.folder = config.InputFolder
    watcher.onnew = printnewfile
    watcher.start()

if __name__ == '__main__':
    config.load("./config.json")
    getnewfiles()
    watch()
    #process()
    #p.kill()
    #start()
    #watch()
    #time.sleep(15)
    #w.stop()
    #time.sleep(10)
    #w.start()
