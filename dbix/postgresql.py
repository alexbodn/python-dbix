
from .sqlschema import SQLSchema, SQLResultSet

import psycopg2
import psycopg2.extensions as pe


import traceback

class MyConnection(pe.connection):

	def commit(self):
		print 'commit'
#		print traceback.print_stack()
		return super(MyConnection, self).commit()

	def rollback(self):
		print 'rollback'
#		print traceback.print_stack()
		return super(MyConnection, self).rollback()

class MyCursor(pe.cursor):

	def execute(self, query, param, **kw):
		print 'execute'
		print traceback.print_stack()
		return super(MyCursor, self).execute(query, param, **kw)


class POSTGRESQLResultSet(SQLResultSet):

	def perform_insert(self, script, param, pk_fields, table, new_key):
		script += u' returning %s' % u','. join ([
				self.schema.render_name(field) for field in pk_fields
			])
		res = self.schema.db_execute(script, param)

		return res.fetchone()


class POSTGRESQL(SQLSchema):

	rs_class = POSTGRESQLResultSet

	_type_conv = dict(
		enum='varchar', 
		boolean='integer', 
		datetime='timestamp', 
		tinyint='integer', 
		mediumtext='text', 
	)

	getdate = dict(
		timestamp="CLOCK_TIMESTAMP() at time zone 'utc'",
		date="cast((CLOCK_TIMESTAMP() at time zone 'utc') as DATE)",
		time="cast((CLOCK_TIMESTAMP() at time zone 'utc') as TIME)",
	)

	deferred_fk = "DEFERRABLE INITIALLY DEFERRED"

	render_paramplace = '%s'

	on_update_trigger = """
		CREATE OR REPLACE FUNCTION "trf_%(table)s%%(c)d_before"() RETURNS trigger AS 
		$BODY$
		BEGIN
			IF "new"."%(field)s"="old"."%(field)s" THEN
				"new"."%(field)s" = %(getdate_tr)s;
			END IF;
			RETURN NEW;
		END;
		$BODY$ LANGUAGE plpgsql;
		DROP TRIGGER IF EXISTS "tr_%(table)s%%(c)d_before" ON "%(table)s";
		CREATE TRIGGER "tr_%(table)s%%(c)d_before"
			BEFORE UPDATE
			ON "%(table)s" FOR EACH ROW
			EXECUTE PROCEDURE "trf_%(table)s%%(c)d_before"();
		"""

	inline_fk = False

	dsn = "dbname='%(db)s' user='%(user)s' host='%(host)s' password='%(password)s'"
	dsn_dba = "dbname='postgres' user='%(user_dba)s' host='%(host)s' password='%(password_dba)s'"

	def __init__(self, **connectparams):
		super(POSTGRESQL, self).__init__()
		self.type_render['serial primary key'] = self.type_render['integer']

		self.connectparams = dict(connectparams)
		self.connectparams.pop('db', None)

	def render_name(self, name):
		return '"%s"' % name

	def render_autoincrement(self, attrs, entity, name):
		attrs, __ = super(POSTGRESQL, self).render_autoincrement(
			attrs, entity, name)
		if attrs.get('is_auto_increment'):
			attrs['data_type'] = 'serial primary key'
			self.this_render_pk = False
		return attrs, ''

	def fk_disable(self):
		return [
			'ALTER TABLE %s DISABLE TRIGGER ALL' % entity['table'] \
			for entity in self.entities
		]

	def fk_enable(self):
		return [
			'ALTER TABLE %s ENABLE TRIGGER ALL' % entity['table'] \
			for entity in self.entities
		]

	def isdba(self):
		return 'user_dba' in self.connectparams \
			and 'password_dba' in self.connectparams

	def db_create(self, dbname):
		if not self.isdba():
			return
		conn = psycopg2.connect(self.dsn_dba % self.connectparams)
		conn.set_isolation_level(pe.ISOLATION_LEVEL_AUTOCOMMIT)
		cur = conn.cursor()
		connectparams = dict(db=dbname)
		connectparams.update(self.connectparams)
		cur.execute(
			"""
			CREATE DATABASE %(db)s WITH OWNER=%(user)s;
			""" % connectparams
		)
		cur.close()
		conn.close()
		dbs = self.db_list()
		return dbs and dbname in dbs

	def db_drop(self, dbname):
		if not self.isdba():
			return
		dbs = self.db_list()
		if dbs and dbname not in dbs:
			return True
		if dbname == self.dbname:
			self.db_disconnect()
		conn = psycopg2.connect(
			self.dsn_dba % self.connectparams, 
		)
		conn.set_isolation_level(pe.ISOLATION_LEVEL_AUTOCOMMIT)
		cur = conn.cursor()
		cur.execute("DROP DATABASE %(db)s;" % dict(db=dbname))
		cur.close()
		conn.close()
		dbs = self.db_list()
		return dbs and dbname not in dbs

	def db_reset(self):
		self.connection = None
		self.dbname = None

	def db_connect(self, dbname):
		try:
			connectparams = dict(db=dbname)
			connectparams.update(self.connectparams)
			self.connection = psycopg2.connect(
				self.dsn % connectparams, 
#				connection_factory=MyConnection, 
			)
			self.connection.set_isolation_level(
				pe.ISOLATION_LEVEL_READ_COMMITTED)
			self.dbname = dbname
			return True
		except:
			self.db_reset()
			return False

	def db_disconnect(self):
		if not self.connection:
			return
		self.connection.close()
		self.db_reset()

	def db_commit(self):
		if not self.connection:
			return
		self.connection.commit()

	def db_rollback(self):
		if not self.connection:
			return
		self.connection.rollback()

	def db_name(self):
		return self.dbname

	def db_list(self):
		try:
			conn = self.connection
			if not conn:
				connectparams = dict(db='postgres')
				connectparams.update(self.connectparams)
				conn = psycopg2.connect(self.dsn % connectparams)
			cur = conn.cursor()
			cur.execute("SELECT datname FROM pg_database;")
			res = [row[0] for row in cur.fetchall()]
			cur.close()
			if not self.connection:
				conn.close()
			return res
		except:
		    return None

	def db_execute(self, script, param=list()):
		cur = self.db_cursor()
		cur.execute(self.query_prefix + script, param)
		#for notice in self.connection.notices:
		#	print (notice)
		return cur

	def db_executemany(self, script, param=list()):
		cur = self.db_cursor()
		cur.executemany(self.query_prefix + script, param)
		return cur

	def db_executescript(self, script):
		return self.db_execute(script + ";\nselect 0=1;")

	def db_cursor(self):
		if not self.connection:
			return
		return self.connection.cursor(
#				cursor_factory=MyCursor, 
		)

