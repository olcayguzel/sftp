import os
import threading
import time

from paramiko import SSHException, AuthenticationException, PasswordRequiredException
from pysftp import Connection, CnOpts
from threading import Thread, Lock
from ftplib import FTP
from authtypes import SendTypes
from logger import Logger, LogTypes
import const

mutex = Lock()

class Host:
    def __init__(self):
        self.Address = ""
        self.Port:int = 22
        self.SendType:SendTypes = SendTypes.SFTP
        self.UserName = ""
        self.Password = ""
        self.CertPath = None
        self.RemotePath = ""
        self.ConnectTimeout:int = 10
        self.Compression:bool = False
        self.__files = []
        self.__connected:bool = False
        self.__running:bool = False
        self.__sftpoptions = CnOpts()
        self.__sftp = None
        self.__ftp = None
        self.__logger = Logger()
        self.__worker = Thread(target=self.__start)
        self.__lifechecker = Thread(target=self.__keepalive)

    def storelastsentfile(host, filename, log):
        fd = None
        try:
            mutex.acquire(blocking=True)
            fd = open(const.DAT_FILE_NAME, "r+")
            lines = list()
            fileinfo = os.stat(filename)
            for line in fd.readlines():
                data = line.split("|")
                print(line, data[0], host)
                if data[0] != host:
                    lines.append(line)
                else:
                    content = const.DAT_FILE_FORMAT.replace("[HOST]", host)
                    content = content.replace("[NAME]", os.path.basename(filename))
                    content = content.replace("[DATE]", str(fileinfo.st_ctime_ns))
                    lines.append(content)
            if len(lines) == 0:
                content = const.DAT_FILE_FORMAT.replace("[HOST]", host)
                content = content.replace("[NAME]", os.path.basename(filename))
                content = content.replace("[DATE]", str(fileinfo.st_ctime_ns))
                lines.append(content)

            if fd.writable():
                fd.write("\n".join(lines))
                fd.flush()
                log.write(LogTypes.DEBUG, f"Last processed file updated. File name: {filename}")
            else:
                log.write(LogTypes.ERROR,
                          f"Last processed file could not update. File is not writable. Possible there is no write permission. File name: {filename}")
        except FileNotFoundError as ex:
            fd = None
        except Exception as ex:
            log.write(LogTypes.ERROR, ex)
        finally:
            if fd is not None:
                fd.close()
            mutex.release()

    @property
    def connected(self) -> bool:
        return self.__connected

    @property
    def cunning(self) -> bool:
        return self.__running

    def __keepalive(self):
        try:
            while True:
                if self.AuthType == SendTypes.FTP:
                    if self.__ftp is not None:
                        self.__ftp.voidcmd("TYPE I")
                        self.__connected = True
                else:
                    if self.__sftp is not None:
                        self.__sftp.exits(self.RemotePath)
                        self.__connected = True
                time.sleep(5)
        except Exception as ex:
            self.__connected = False

    def __connectFTP(self):
        self.__ftp = FTP(self.Address)
        self.__ftp.connect(self.Address, self.Port, timeout=self.ConnectTimeout)
        self.__ftp.login(self.UserName, self.Password)
        self.__ftp.cwd(self.RemotePath)
        self.__connected = True
        self.__logger.write(LogTypes.DEBUG, f"Connected established via FTP protocol", self.Address)

    def __connectSFTP(self):
        self.__sftpoptions.hostkeys = None
        self.__sftpoptions.compression = self.Compression
        Connection.timeout = self.ConnectTimeout
        self.__sftp = Connection(host=self.Address, port=self.Port, username= self.UserName, password=self.Password, private_key=self.CertPath, cnopts=self.__sftpoptions)
        self.__connected = True
        self.__logger.write(LogTypes.DEBUG, "Connection established via SFTP protocol", self.Address)

    def __connect(self):
        try:
            if self.AuthType == SendTypes.FTP:
                self.__connectFTP()
            else:
                self.__connectSFTP()
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
                self.__lifechecker.start()

    def __put_ftp(self, source:str) -> bool:
        fd = None
        filename = os.path.basename(source)
        result = False
        try:
            fd = open(source, "rb")
            if self.__ftp is not None:
                result = os.stat(source)
                filesize = result.st_size
                total_sent_bytes = 0
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
        filename = os.path.basename(source)
        result = False
        try:
            if self.__sftp is not None:
                self.__sftp.put(localpath=source, remotepath=target)
                result = True
                self.__logger.write(LogTypes.DEBUG, f"File ({filename}) has been sent to ({self.RemotePath})", self.Address)
        except IOError as ex:
            self.__logger.write(LogTypes.ERROR, f"Remote folder ({self.RemotePath}) does not exists or unreachable", self.Address)
        except OSError as ex:
            self.__logger.write(LogTypes.ERROR, f"Local file ({filename})does not exists", self.Address)
        except Exception as ex:
            self.__logger.write(LogTypes.ERROR, f"An error occurred during send file ({filename}) via SFTP protocol. Error: {ex}", self.Address)
        return result

    def __send(self, source:str) ->bool:
        filename = os.path.basename(source)
        result = False
        try:
            remoteFile = self.RemotePath
            remoteFile += "/" + filename
            if self.AuthType == SendTypes.FTP:
                result = self.__put_ftp(source)
            else:
                result = self.__put_sftp(source, remoteFile)
        except Exception as ex:
            self.__logger.write(LogTypes.ERROR, f"An error occurred during send file ({filename}). Error: {ex}", self.Address)
        if result:
            Host.storelastsentfile(self.Address, source, self.__logger)
        return result

    def __process(self):
        while True:
            try:
                if len(self.__files) > 0:
                    if not self.__connected:
                        self.__connect()
                    file = self.__files[0]
                    if self.__send(file):
                        self.__files.pop(0)
            except Exception as ex:
                self.__logger.write(LogTypes.ERROR, ex)

    def __start(self):
        self.__running = True
        self.__process()
        self.__running = False

    def addtoqueue(self, file):
        self.__files.append(file)
        if not self.__worker.is_alive():
            self.__worker.start()

    def __close(self):
        try:
            if self.__sftp is not None:
                self.__sftp.close()
        except Exception as ex:
            self.__logger.write(LogTypes.ERROR, f"SFTP Connection could not close", self.Address)

        try:
            if self.__ftp is not None:
                self.__ftp.close()
        except:
            self.__logger.write(LogTypes.ERROR, f"FTP connection could not close: {self.Address}")
