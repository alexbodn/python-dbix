#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `dbix` package."""

from __future__ import print_function

from click.testing import CliRunner

from dbix import dbix
from dbix import cli


#project imports
import sys, time, os, json
from datetime import datetime

from dbix.dbix import Schema, SQLSchema, SQLITE, POSTGRESQL, MYSQL


here = os.path.abspath(os.path.dirname(__file__))


def test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(cli.main)
    assert result.exit_code == 0
    assert 'dbix.cli.main' in result.output
    help_result = runner.invoke(cli.main, ['--help'])
    assert help_result.exit_code == 0
    assert '--help  Show this message and exit.' in help_result.output


class TestSchema:

	def setup_class(self):
		config = os.path.expanduser('~/.dbix-config.json')
		if not os.path.exists(config):
			raise Exception(
				'please copy %s to %s and edit your connection information' \
				% (os.path.join(here, 'config.json'), config)
			)
		self.config = json.load(open(config, 'r'))

	def _test_dml(self, schema):

		select = """
		select supplier_id, name, user_id, updated
		from supplier;
		"""
		insert = """
		insert into supplier (supplier_id, name, user_id)
		values (1, '2', 3);
		"""
		update = """
		update supplier
		set
			name=%s, 
			user_id=user_id+1
		;
		""" % schema.render_concat('name', "'1'")

		schema.db_execute(insert)
		cursor = schema.db_execute(select)
		try:
			print ([
				getattr(column, 'name', column[0]) \
				for column in cursor.description
			])
		except:
			print(dir(cursor))
#	#	schema.db_commit()
		print(list(cursor.fetchall()))

		time.sleep(1)
		schema.db_execute(update)
		cursor = schema.db_execute(select)
#	#	schema.db_commit()
		print(list(cursor.fetchall()))

		time.sleep(1)
		schema.db_execute(update)
		cursor = schema.db_execute(select)
#	#	schema.db_commit()
		print(list(cursor.fetchall()))


	def _test_resultset(self, schema):
		rs = schema.resultset(
			'Supplier', 
			select_columns=('supplier_id', 'name', 'user_id', 'updated'), 
			#dict_record=True, 
		)
#	#	rs.create(updated=datetime.now(), supplier_id=2, name='3', user_id=4)
		time_before = datetime.utcnow()
		time.sleep(1)
		supplier_id, name, user_id, updated = \
			rs.create(supplier_id=1, name='02', user_id=3)
		time.sleep(1)
		assert supplier_id == 1
		assert name == '02'
		assert user_id == 3
		assert time_before < updated < datetime.utcnow()

		rs.create(supplier_id=10, name='20', user_id=30)

		time_before = datetime.utcnow()
		time.sleep(1)
		rs.find(name=('>', '10')).update(user_id=7, name='aaa')
		c = 0
		for (supplier_id, name, user_id, updated) in rs.find(name=('>', '10')):
			c += 1
			assert supplier_id == 10
			assert name == 'aaa'
			assert user_id == 7
			time.sleep(1)
			assert time_before < updated < datetime.utcnow()
		assert c == 1

		time_before = datetime.utcnow()
		time.sleep(1)
		rs.find(supplier_id=['=', 1]).update(user_id=['+', 1], name=['||', '1'])
		c = 0
		for (supplier_id, name, user_id, updated) in rs.find(1):
			c += 1
			assert supplier_id == 1
			assert name == '021'
			assert user_id == 4
			time.sleep(1)
			assert time_before < updated < datetime.utcnow()
		assert c == 1


	def _test_schema(self, schema):

		schema_input = os.path.join(here, 'schema')
		dbname = 'icecat'

		#print(schema.db_list())

		ddl = schema.ddl(
			schema_input, 
			#with_fk=False, 
			#only_tables=['MeasureSign']
		)
		ddl_ext = schema.dump_extension
		open(os.path.join(here, 'dumps/%s%s' % (dbname, ddl_ext)), 'wb').write(ddl)

		schema.db_drop(dbname)
		schema.db_create(dbname)

		schema.db_connect(dbname)

		schema.load_ddl(
			schema_input, 
			#with_fk=False, 
			#only_tables=['MeasureSign']
		)

		if isinstance(schema, SQLSchema) and 0:
			_test_dml(schema)

		self._test_resultset(schema)

		schema.db_disconnect()


	def test_schema(self):
		schema = Schema()

		self._test_schema(schema)

	def test_sqlite(self):
		schema = SQLITE(path=os.path.join(here, 'data'))

		self._test_schema(schema)

	def test_postgresql(self):
		schema = POSTGRESQL(**self.config.get('POSTGRESQL', dict()))

		self._test_schema(schema)

	def test_mysql(self):
		schema = MYSQL(**self.config.get('MYSQL', dict()))

		self._test_schema(schema)


if __name__ == '__main__':

	test_schema()
	test_sqlite()
	test_postgresql()
	test_mysql()
