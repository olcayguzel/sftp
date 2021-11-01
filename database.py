import psycopg2
from logger import Logger, LogTypes

class Database:
	def __init__(self, connectionstring = None):
		self.__connectionstring = connectionstring
		self.__connection = None
		self.__log:Logger

	def __del__(self):
		try:
			if self.__connection is not None:
				self.__connection.close()
				self.__log.write(LogTypes.INFO, "Connection closed")
		except Exception as ex:
			self.__log.write(LogTypes.Error, f"An error occurred during dispose object: Exception: {ex}")

	@property
	def log(self):
		return self.__log

	@log.setter
	def log(self, value: Logger):
		self.__log = value

	@property
	def connectionstring(self):
		return self.__connectionstring

	@connectionstring.setter
	def connectionstring(self, value):
		self.__connectionstring = value

	def connect(self):
		try:
			if self.__connectionstring:
				self.__connection = psycopg2.connect(self.__connectionstring)
				self.__log.write(LogTypes.DEBUG, "Connection established")
			else:
				self.__log.write(LogTypes.ERROR, "Connection string was not initialized")
		except Exception as ex:
			self.__log.write(LogTypes.ERROR, f"An error occurred during connect to database: Exception: {str(ex)}")

	def disconnect(self):
		try:
			if self.__connection is not None:
				self.__connection.close()
				self.__log.write(LogTypes.INFO, "Connection closed")
		except Exception as ex:
			self.__log.write(LogTypes.ERROR, f"An error occurred during close database connection: Exception: {ex}")

	def getconnection(self):
		return self.__connection

	def query(self, sql, parameters = None, commit = True):
		cursor = None
		result = None
		try:
			cursor = self.__connection.cursor()
			cursor.execute(sql, parameters)
			if cursor.description is not None:
				result = cursor.fetchall()
			if commit:
				cursor.connection.commit()
			else:
				cursor.connection.rollback()
		except Exception as ex:
			self.__log.write(LogTypes.ERROR, f"An error occurred during execute query: Exception: {ex}")
		finally:
			if cursor is not None:
				cursor.close()
		return result

	def procedure(self, query, parameters = None):
		cursor = None
		result = None
		try:
			cursor = self.__connection.cursor()
			cursor.callproc(query, parameters)
			result = cursor.fetchall()
			cursor.connection.commit()
		except Exception as ex:
			self.__log.write(LogTypes.ERROR, f"An error occurred during execute query: Exception: {ex}")
		finally:
			if cursor is not None:
				cursor.close()
		return result

	def bulkinsert(self, filename, tablename, separator, fields=None, buffer=4096):
		cursor = None
		content = None
		try:
			if filename is not None:
				content = open(filename, "r")
				cursor = self.__connection.cursor()
				cursor.copy_from(file=content, table=tablename, sep=separator, size=buffer, columns=fields)
				cursor.connection.commit()
			else:
				self.__log.write(LogTypes.DEBUG, "File name must be provided")
		except Exception as ex:
			self.__log.write(LogTypes.ERROR, f"An error occurred during execute query: Exception: {ex}")
		finally:
			if cursor is not None:
				cursor.close()
			if content is not None:
				content.close()

	def exporttable(self, filename, tablename, separator, fields=None, nullvalue="NULL"):
		cursor = None
		content = None
		try:
			if filename is not None:
				content = open(filename, "w")
				cursor = self.__connection.cursor()
				cursor.copy_to(file=content, table=tablename, sep=separator, columns=fields, null = nullvalue)
			else:
				self.__log.write(LogTypes.DEBUG, "File name must be provided")
		except Exception as ex:
			self.__log.write(LogTypes.ERROR, f"An error occurred during execute query: Exception: {ex}")
		finally:
			if cursor is not None:
				cursor.close()
			if content is not None:
				content.flush()
				content.close()

	def isConnected(self):
		try:
			if self.__connection is None:
				return False
			else:
				return self.__connection.closed == 0
		except Exception as ex:
			return False

