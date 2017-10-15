
from .sqlschema import SQLSchema

import MySQLdb as mysqldb
#import mysql.connector as mysqldb


class MYSQL(SQLSchema):

	inline_domains = True
	inline_timestamps = False

	_type_conv = {
		'datetime': 'timestamp', 
	}

	getdate = {
		'timestamp': "now()",
		'timestamp(6)': "now(6)",
		'datetime': "now()",
		'datetime(6)': "now(6)",
		'date': "now()",
		'time': "now()",
	}

	render_paramplace = '%s'

	oncreate_inline = " DEFAULT %(getdate)s"

	on_create_trigger_template = (
		'before', 
		"""
		create trigger `tr_%(table)s%%(c)d_insert` 
		before insert 
		on `%(table)s` for each row 
		begin
			%%(content)s
		end; 
		"""
	)

	onupdate_inline = " ON UPDATE %(getdate)s"

	on_update_trigger_template = (
		'before', 
		"""
		create trigger `tr_%(table)s%%(c)d_update` 
		before update 
		on `%(table)s` for each row 
		begin
			%%(content)s
		end; 
		"""
	)

	trigger_field_action_before = dict(
		update="""
		if `new`.`%(field)s`=`old`.`%(field)s` then
			set `new`.`%(field)s`=%(getdate_tr)s;
		end if;
		""",
		insert="""
		if `new`.`%(field)s` is null then
			set `new`.`%(field)s`=%(getdate_tr)s;
		end if;
		""",
	)

	_trigger_format = """
		delimiter $$
		%s
		$$
		delimiter ;
		"""

	auto_increment = " AUTO_INCREMENT"

	table_sufix = "ENGINE=MyISAM"


	@staticmethod
	def render_number(value):
		return u"'%s'" % value

	@staticmethod
	def render_concat(left, right):
		return 'concat(%s, %s)' % (left, right)

	def render_default(self):
		return ' DEFAULT %(default_value)s'

	def render_name(self, name):
		return '`%s`' % name

	def render_autoincrement(self, attrs, entity, name):
		attrs, __ = super(MYSQL, self).render_autoincrement(
			attrs, entity, name)
		if attrs.get('is_auto_increment'):
			return attrs, 'AUTO_INCREMENT'
		return attrs, ''

	def type_conv(self, attrs, entity, name):
		data_type = super(MYSQL, self).type_conv(attrs, entity, name)
		timestamp_attrs = ('set_on_create', 'set_on_update')
		if 1 in [attrs.get(parm, 0) for parm in timestamp_attrs]:
			if 'hastimestamps' in entity:
				if data_type == 'timestamp':
					data_type = 'datetime'
				self.inline_timestamps = False
			else:
				entity['hastimestamps'] = 1
				self.inline_timestamps = True
		return data_type

	def __init__(self, **connectparams):
		super(MYSQL, self).__init__()
		self.type_render['timestamp(6)'] = self.type_render['timestamp']
		self.type_render['datetime(6)'] = self.type_render['datetime']

		self.connectparams_user = dict(
			host=connectparams.get('host', 'localhost'),
			user=connectparams.get('user'),
			passwd=connectparams.get('password'),
		)
		self.connectparams_dba = dict(
			host=connectparams.get('host', 'localhost'),
			db='mysql',
			user=connectparams.get('user_dba'),
			passwd=connectparams.get('password_dba'),
		)

	def isdba(self):
		return self.connectparams_dba.get('user') \
			and self.connectparams_dba.get('passwd')

	def db_create(self, dbname):
		if not self.isdba():
			return
		conn = mysqldb.connect(**self.connectparams_dba)
		cur = conn.cursor()
		connectparams = dict(
			db=dbname,
			user=self.connectparams_user.get('user'),
		)
		cur.execute(
			"""
			CREATE DATABASE %(db)s;
			""" % connectparams
		)
		conn.commit()
		cur.execute(
			"""
			GRANT ALL PRIVILEGES ON `%(db)s`.* TO '%(user)s;'
			""" % connectparams
		)
		conn.commit()
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
		conn = mysqldb.connect(**self.connectparams_dba)
		cur = conn.cursor()
		cur.execute("DROP DATABASE %(db)s;" % dict(db=dbname))
		conn.commit()
		conn.close()
		dbs = self.db_list()
		return dbs and dbname not in dbs

	def db_connect(self, dbname):
		try:
			connectparams = dict(db=dbname)
			connectparams.update(self.connectparams_user)
			self.connection = mysqldb.connect(**connectparams)
			self.dbname = dbname
			return True
		except:
			self.connection = None
			self.dbname = None
			return False

	def db_disconnect(self):
		if not self.connection:
			return
		self.connection.close()
		self.connection = None
		self.dbname = None

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
			conn = mysqldb.connect(**self.connectparams_dba)
			cur = conn.cursor()
			cur.execute("SHOW DATABASES;")
			res = [row[0] for row in cur.fetchall()]
			if not self.connection:
				conn.close()
			return res
		except:
		    return None

	def db_execute(self, script, param=list()):
		if not self.connection:
			return
		cur = self.connection.cursor()
		with self.connection:
			if self.query_prefix:
				script = self.query_prefix + script
			cur.execute(script, param)
		#for notice in self.connection.notices:
		#	print (notice)
		return cur

	def db_executemany(self, script, param=list()):
		if not self.connection:
			return
		cur = self.connection.cursor()
		with self.connection:
			if self.query_prefix:
				script = self.query_prefix + script
			cur.executemany(script, param)
		return cur


	def db_executescript(self, script):
		self.db_execute(script)
		return self.db_execute("select 0=1;")


	def db_executelist(self, statements):
		if not self.connection:
			return
		cur = self.connection.cursor()
		with self.connection:
			for c, script in enumerate(statements):
				script = script.strip()
				if not script or script == self.default_delimiter:
					continue
				if self.query_prefix:
					script = self.query_prefix + script
				cur.execute(script)
				self.db_commit()
		return cur

	def perform_insert(self, script, param, pk_fields, table, new_key):
		self.db_execute(script, param)
		if new_key:
			return new_key
		script = u'select %s=last_insert_id()' % \
			self.render_name(pk_fields[0])
		res = self.db_execute(script)
		return res.fetchone()


