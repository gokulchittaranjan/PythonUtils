import sqlite3, traceback;

from threading import Lock;

from Utils import Logging;

class DBRecordsParser:
	def __init__(self, dbManager, tableName, filters):
		self.logger = Logging.defaults(__name__);
		self.dbManager = dbManager;
		self.tableName = tableName;
		self.filters = filters;

	def withFunction(self, processor):
		records = self.dbmanager.getRecords(self.tableName, self.filters);
		self.logger.debug("Processing %d records with filters %s from %s" %(len(records), self.filters, self.tableName));
		cnt = 0;
		tot = len(records);
		for r in records:
			self.logger.debug("Processing record %d/%d" %(cnt, tot));
			processor.process(r);

class DBConnection:
	
	def __init__(self, dbName):
		self.logger = Logging.defaults(__name__);
		self.conn = sqlite3.connect(dbName);
		self.lock = Lock();
		self.dbName = dbName;

	def close(self):
		self.conn.close();

	def runSQL(self, sql, values="", executeMany = False):
		selectSQL = False;
		if sql.split(" ")[0].lower()=="select":
			selectSQL = True;
		try:
			self.lock.acquire();
			self.logger.debug("Lock acquired.");
			self.logger.debug("SQL: %s" %(sql));
			self.logger.debug("isSelect=%s" %(selectSQL));

			if not selectSQL:
				if values=="":
					self.conn.execute(sql);
				else:
					if executeMany:
						self.conn.executemany(sql, values);
					else:
						self.conn.execute(sql, values);
				self.conn.commit();
			else:
				cursor = self.conn.cursor();
				if values=="":
					cursor.execute(sql);
				else:
					if executeMany:
						cursor.executemany(sql, values);
					else:
						cursor.execute(sql, values);
				return cursor;
		except:
			self.logger.error(traceback.format_exc());
		finally:
			self.lock.release();
			self.logger.debug("Lock released.");

	def createTable(self, tableName="", fields = {}):
	
		fieldsStr = [];
		for f,t in fields.items():
			fieldsStr.append("%s %s" %(f,t));

		sql = "CREATE TABLE IF NOT EXISTS %s (%s)" %(tableName, ",".join(fieldsStr));
		self.runSQL(sql);
	
	def addRecord(self, tableName, values):
		qstr = ["?"]*len(values);
		qstr = ",".join(qstr);
		sql = "INSERT INTO %s (%s) VALUES (%s)" %(tableName, ",".join(values.keys()), qstr);
		self.runSQL(sql, values.values());

	def addRecords(self, tableName, valuesArray):
		keys = valuesArray[0].keys();
		qstr = ["?"]*len(keys);
		qstr = ",".join(qstr);
		sql = u"INSERT INTO %s (%s) VALUES (%s)" %(tableName, ",".join(keys), qstr);
		insertValues = [tuple(v.values()) for v in valuesArray];
		self.runSQL(sql, insertValues, executeMany=True);

	def deleteRecord(self, tableName, filters):
		values = [];
		filterStr = [];
		for k,v in filters.items():
			filterStr.append("%s=?" %(k));
			values.append(v);
		filterStr = ",".join(filterStr);
		sql = u"DELETE FROM %s WHERE %s" %(tableName, filterStr);
		self.runSQL(sql, values);

	def updateRecord(self, tableName, values, filters):
		filterValues = [];
		filterStr = [];
		for k,v in filters.items():
			filterStr.append("%s=?" %(k));
			filterValues.append(v);
		filterStr = ",".join(filterStr);
		updateStr = [];
		updateValues = [];
		for k,v in values.items():
			updateStr.append("%s=?" %(k));
			updateValues.append(v);
		updateValues.extend(filterValues);
		updateStr = ",".join(updateStr);
		sql = u"UPDATE %s SET %s WHERE %s" %(tableName, updateStr, filterStr);
		self.runSQL(sql, updateValues);

	def getRecords(self, tableName, filters={}):
		whereClause = [];
		for k,v in filters.items():
			whereClause.append("%s='%s'" %(k,v));
		whereClause = " AND ".join(whereClause);
		sql = u"SELECT * FROM %s" %(tableName);
		if whereClause !="":
			sql = u"SELECT * FROM %s WHERE %s" %(tableName, whereClause);
		cursor = self.runSQL(sql);
		if cursor == None:
			return [];
		names = list(map(lambda x: x[0], cursor.description));
		data = [];
		for e in cursor.fetchall():
			row = dict();
			for k,v in zip(names, e):
				row[k] = v;
			data.append(row);
		return data;

	def getJoinedRecords(self, tableName1, tableName2, joiningKeyTable1, joiningKeyTable2, joiningType="LEFT", filtersTable1={}, filtersTable2={}):
		whereClause = [];
		for k,v in filtersTable1.items():
			whereClause.append("table1.%s='%s'" %(k,v));
		#whereClause = " AND ".join(whereClause);
		for k,v in filtersTable2.items():
			whereClause.append("table2.%s='%s'" %(k,v));
		whereClause = " AND ".join(whereClause);
		
		sql = u"SELECT * FROM %s table1 %s JOIN %s table2 ON table1.%s = table2.%s" %(tableName1, joiningType, tableName2, joiningKeyTable1, joiningKeyTable2);
		if whereClause !="":
			sql = u"SELECT * FROM %s table1 %s JOIN %s table2 ON table1.%s = table2.%s WHERE %s " %(tableName1, joiningType, tableName2, joiningKeyTable1, joiningKeyTable2,  whereClause);
		cursor = self.runSQL(sql);
		if cursor == None:
			return [];
		names = list(map(lambda x: x[0], cursor.description));
		data = [];
		for e in cursor.fetchall():
			row = dict();
			for k,v in zip(names, e):
				print k,v
				row[k] = v;
			data.append(row);
		return data;