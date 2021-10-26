import os
import time
from paramiko import SSHException, AuthenticationException, PasswordRequiredException
from pysftp import Connection, CnOpts
from threading import Thread, Lock
from ftplib import FTP
from failaction import FailAction
from sendtypes import SendTypes
from logger import Logger, LogTypes
from process import Process
from actiontypes import ActionTypes
from database import Database
from compressfile import CompressFile
from configuration import Config
import const

mutex = Lock()

class Host:
    def __init__(self, config:Config):
        self.Address:str = ""
        self.Port:int = 22
        self.UserName:str = ""
        self.Password:str = ""
        self.CertPath = None
        self.RemotePath:str = ""
        self.ConnectTimeout:int = 10
        self.Compression:bool = False
        self.DeleteZipFile:bool = False
        self.MaxTryCount:int = 5
        self.SendType:SendTypes = SendTypes.SFTP
        self.FailAction:FailAction = FailAction()
        self.__trycount:int = 0
        self.__lastprocessedfilename:str = ""
        self.__lastprocessedfiledate:int = 0
        self.__files = []
        self.__connected:bool = False
        self.__running:bool = False
        self.__sftpoptions = CnOpts()
        self.__sftp = None
        self.__ftp = None
        self.__rsync = None
        self.__config:Config = config
        self.__logger = Logger()
        self.__db:Database = Database()
        self.__db.log = self.__logger
        self.__db.connectionstring = self.__config.ConnectionString
        self.__worker = Thread(target=self.__start)
        self.__lifechecker = Thread(target=self.__keepalive)

    @property
    def connected(self) -> bool:
        return self.__connected

    @property
    def running(self) -> bool:
        return self.__running
    
    @property
    def lastprocessedfile(self) -> tuple():
        return (self.__lastprocessedfilename, self.__lastprocessedfiledate)
    
    @lastprocessedfile.setter
    def lastprocessedfile(self, value:tuple()):
        self.__lastprocessedfilename = str(value[0])
        self.__lastprocessedfiledate = int(value[1])

    def __deletezipfile(self, file):
        try:
            os.remove(file)
        except Exception as ex:
            self.__logger.write(LogTypes.ERROR, f"File ({file}) could not been deleted. Error: {ex}", self.Address)

    def __compressfile(self, file):
        compress = CompressFile()
        compress.filename = file
        compress.level = 9
        compress.compress()
        return compress.outputfilename

    def __executeprocess(self, file):
        try:
            action = Process()
            action.path = self.FailAction.Command
            action.arguments = self.FailAction.Args
            action.start(False)
            action.wait()
        except Exception as ex:
            self.__logger.write(LogTypes.ERROR, f"An error occured during execute fail action command. Error: {ex}", self.Address)

    def __executequery(self, file):
        self.__db.connect()
        self.__db.query(self.FailAction.Command, self.FailAction.Args)

    def __doaction(self, file:str):
        if self.FailAction.Type == ActionTypes.ExecuteApp:
            self.__executeprocess(file)
        elif self.FailAction.Type.ExecuteQuery:
            self.__executequery(file)

    def __storelastsentfile(self, filename, log):
        fd = None
        try:
            mutex.acquire(blocking=True)
            if not os.path.exists(const.DAT_FILE_NAME):
                tmp = open(const.DAT_FILE_NAME, "w")
                tmp.flush()
                tmp.close()

            fd = open(const.DAT_FILE_NAME, "r+")
            lines = list()
            fileinfo = os.stat(filename)
            for line in fd.readlines():
                data = line.split("|")
                if data[0] != self.Address:
                    lines.append(line)
                else:
                    content = const.DAT_FILE_FORMAT.replace("[HOST]", self.Address)
                    content = content.replace("[FOLDER]", self.RemotePath)
                    content = content.replace("[NAME]", os.path.basename(filename))
                    content = content.replace("[DATE]", str(fileinfo.st_ctime_ns))
                    lines.append(content)
            if len(lines) == 0:
                content = const.DAT_FILE_FORMAT.replace("[HOST]", self.Address)
                content = content.replace("[FOLDER]", self.RemotePath)
                content = content.replace("[NAME]", os.path.basename(filename))
                content = content.replace("[DATE]", str(fileinfo.st_ctime_ns))
                lines.append(content)
            if fd.writable():
                fd.seek(0)
                fd.truncate()
                fd.flush()
                fd.write("\n".join(lines))
                fd.flush()
                log.write(LogTypes.DEBUG, f"Last processed file updated. File name: {filename}", self.Address)
                self.__lastprocessedfilename = os.path.basename(filename)
                self.__lastprocessedfiledate = fileinfo.st_ctime_ns
            else:
                log.write(LogTypes.ERROR,
                          f"Last processed file could not update. File is not writable. Possible there is no write permission. File name: {filename}", self.Address)
        except FileNotFoundError:
            fd = None
        except Exception as ex:
            log.write(LogTypes.ERROR, ex)
        finally:
            if fd is not None:
                fd.close()
            mutex.release()

    def __keepalive(self):
        try:
            while True:
                if self.SendType == SendTypes.FTP:
                    if self.__ftp is not None:
                        self.__ftp.voidcmd("TYPE I")
                        self.__connected = True
                elif self.SendType == SendTypes.SFTP:
                    if self.__sftp is not None:
                        self.__sftp.exists(self.RemotePath)
                        self.__connected = True
                else:
                    break
                time.sleep(5)
        except Exception as ex:
            self.__logger.write(LogTypes.ERROR, f"Unexpected error occurred. Error: {ex}", self.Address)
            self.__connected = False

    def __connectftp(self):
        self.__ftp = FTP(self.Address)
        self.__ftp.connect(self.Address, self.Port, timeout=self.ConnectTimeout)
        self.__ftp.login(self.UserName, self.Password)
        self.__ftp.cwd(self.RemotePath)
        self.__connected = True
        self.__logger.write(LogTypes.DEBUG, f"Connection established via FTP protocol", self.Address)

    def __connectsftp(self):
        self.__sftpoptions.hostkeys = None
        self.__sftpoptions.compression = True
        Connection.timeout = self.ConnectTimeout
        self.__sftp = Connection(host=self.Address, port=self.Port, username= self.UserName, password=self.Password, private_key=self.CertPath, cnopts=self.__sftpoptions)
        self.__connected = True
        self.__logger.write(LogTypes.DEBUG, "Connection established via SFTP protocol", self.Address)

    def __connect(self):
        try:
            if self.SendType == SendTypes.FTP:
                self.__connectftp()
            elif self.SendType == SendTypes.SFTP:
                self.__connectsftp()
        except PasswordRequiredException:
            self.__connected = False
            self.__logger.write(LogTypes.ERROR, f"Password required for '{self.UserName}", self.Address)
        except AuthenticationException:
            self.__connected = False
            self.__logger.write(LogTypes.ERROR, f"Authentication failed for '{self.UserName}'", self.Address)
        except SSHException as ex:
            self.__connected = False
            self.__logger.write(LogTypes.ERROR, f"An error occurred during connect. Error: {ex}", self.Address)
        except Exception as ex:
            self.__connected = False
            self.__logger.write(LogTypes.ERROR, f"An error occurred during connect. Error: {ex}", self.Address)
        finally:
            if not self.__lifechecker.is_alive():
                pass
               # self.__lifechecker.start()

    def __sendrsync(self):
        self.__rsync = Process()
        self.__rsync.path = "rsync"
        self.__rsync.arguments = f"-rt --contimeout={self.ConnectTimeout} --compress-level=9 --include={self.__config.InputFilePattern} "
        self.__rsync.start()

    def __put_ftp(self, source:str) -> bool:
        fd = None
        filename = os.path.basename(source)
        result = False
        try:
            fd = open(source, "rb")
            if self.__ftp is not None:
                self.__ftp.storbinary(cmd=f"STOR {filename}", fp=fd )
                result = True
                self.__logger.write(LogTypes.DEBUG, f"File ({filename}) has been sent to ({self.RemotePath})", self.Address)
        except Exception as ex:
            self.__logger.write(LogTypes.ERROR, f"An error occurred during send file ({filename}) via FTP protocol. Error: {ex}", self.Address)
        finally:
            if fd is not None:
                fd.close()
        return result

    def __put_sftp(self, source:str, target:str) -> bool:
        filename = ""
        result = False
        try:
            filename = os.path.basename(source)
            if self.__sftp is not None:
                self.__sftp.put(localpath=source, remotepath=target)
                result = True
                self.__logger.write(LogTypes.DEBUG, f"File ({filename}) has been sent to ({self.RemotePath})", self.Address)
        except IOError:
            self.__logger.write(LogTypes.ERROR, f"Remote folder ({self.RemotePath}) does not exists or unreachable", self.Address)
        except Exception as ex:
            self.__logger.write(LogTypes.ERROR, f"An error occurred during send file via SFTP protocol. Error: {ex}", self.Address)
        return result

    def __send(self, source:str) ->bool:
        result = False
        sentfilename = source
        try:
            if self.Compression:
                sentfilename = self.__compressfile(source)
            filename = os.path.basename(source)
            remoteFile = self.RemotePath
            remoteFile += "/" + filename
            if self.SendType == SendTypes.FTP:
                result = self.__put_ftp(sentfilename)
            elif self.SendType == SendTypes.SFTP:
                result = self.__put_sftp(sentfilename, remoteFile)
            else:
                self.__sendrsync()
        except Exception as ex:
            self.__logger.write(LogTypes.ERROR, f"An error occurred during send file ({filename}). Error: {ex}", self.Address)
        if result:
            self.__storelastsentfile(source, self.__logger)
            if self.Compression and self.DeleteZipFile:
                self.__deletezipfile(sentfilename)
        return result

    def __process(self):
        while True:
            try:
                if len(self.__files) > 0:
                    self.__trycount += 1
                    if not self.__connected:
                        self.__connect()
                    file = self.__files[0]
                    if self.__send(file):
                        self.__files.pop(0)
                        self.__trycount = 0
                    elif self.__trycount >= self.MaxTryCount:
                        self.__doaction(self.__files.pop(0))
                    else:
                        time.sleep(5)
            except Exception as ex:
                self.__logger.write(LogTypes.ERROR, ex, self.Address)

    def __start(self):
        self.__running = True
        self.__process()
        self.__running = False

    def addtoqueue(self, file):
        stat = os.stat(file)
        if stat.st_ctime_ns > self.__lastprocessedfiledate and file.__eq__(self.__lastprocessedfilename) == False:
            self.__files.append(file)
            if not self.__worker.is_alive():
                self.__worker.start()

    def __close(self):
        try:
            if self.__sftp is not None:
                self.__sftp.close()
        except Exception as ex:
            self.__logger.write(LogTypes.ERROR, f"SFTP Connection could not close. Error: {ex}", self.Address)

        try:
            if self.__ftp is not None:
                self.__ftp.close()
        except Exception as ex:
            self.__logger.write(LogTypes.ERROR, f"FTP connection could not close. Error: {ex}", self.Address)
