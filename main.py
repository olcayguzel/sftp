import os
import time
from configuration import Config
import watcher

config = Config()
w = watcher.Watcher()

def start():
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
"""
def getlastreadfiledate():
    global lastprocessedtime
    fd = None
    date = 0
    name = ""
    try:
        fd = open(const.DAT_FILE_NAME)
        if fd.readable():
            content = fd.readline()
            if len(content) > 1:
                data = content.split("|")
                date = int(data[0])
                name = data[1]
                log.write(LogTypes.DEBUG, f"Last processed file is: {name}")
                lastprocessedtime = date
            else:
                log.write(LogTypes.DEBUG, "Could not detect last processed file. All files in folder will be enqueue to processed")
    except Exception as ex:
        log.write(LogTypes.ERROR, ex)
    finally:
        if fd is not None:
            fd.close()
    return (date, name)

def checkexistingfiles():
    try:
        global queue
        global config
        global log
        date, name = getlastreadfiledate()
        cdrfiles = dict()
        with os.scandir(config.InputFolder) as files:
            for file in files:
                if file.name.endswith('.cdr'):
                    stats = file.stat(follow_symlinks=False)
                    if stats.st_ctime_ns > date and file.name.__eq__(name) == False:
                        cdrfiles[file.name] = stats.st_ctime_ns
        cdrfiles = list(sorted(cdrfiles.items(), key=lambda t: t[1]))
        for file in cdrfiles:
            queue.put(os.path.join(config.InputFolder, file[0]))
        filecount = len(cdrfiles)
        if filecount > 0:
            log.write(LogTypes.DEBUG, f"{filecount} new file(s) detected which is created after last running time. All of them added to queue to process")
        else:
            log.write(LogTypes.DEBUG, f"There is no new files which pending to process at: {config.InputFolder}")
    except Exception as ex:
        log.write(LogTypes.ERROR, ex)
"""

def sendfile(file):
    if len(config.RemoteHosts) > 0:
        for host in config.RemoteHosts:
            host.addtoqueue(file)
            #host.send(file, "def.jpg")
            #host.close()

def printnewfile(file:callable(str)):
    print(file)

def d():
    w.folder = "/Users/olcayguzel/Desktop/Output"
    w.onnew = printnewfile
    w.start()

if __name__ == '__main__':
    config.load("./config.json")
    #start()
    d()
    time.sleep(15)
    w.stop()
    time.sleep(10)
    w.start()
