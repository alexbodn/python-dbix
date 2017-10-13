#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `dbix` package."""

from __future__ import print_function

from click.testing import CliRunner

from dbix import dbix
from dbix import cli


#project imports
import sys, time, os
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


def _test_dml(schema):

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
#	schema.db_commit()
	print(list(cursor.fetchall()))

	time.sleep(1)
	schema.db_execute(update)
	cursor = schema.db_execute(select)
#	schema.db_commit()
	print(list(cursor.fetchall()))

	time.sleep(1)
	schema.db_execute(update)
	cursor = schema.db_execute(select)
#	schema.db_commit()
	print(list(cursor.fetchall()))


def _test_resultset(schema):
	rs = schema.resultset(
		'Supplier', 
		select_columns=('supplier_id', 'name', 'user_id', 'updated'), 
		#dict_record=True, 
	)
#	rs.create(updated=datetime.now(), supplier_id=2, name='3', user_id=4)
	print('???????????', rs.create(supplier_id=1, name='02', user_id=3))
	print('???????????', rs.create(supplier_id=10, name='20', user_id=30))
	print('========', list(rs), file=sys.stderr)
	rs.find(name=('>', '10')).update(user_id=7, name='aaa')
	rs.find(supplier_id=['=', 1]).update(user_id=['+', 1], name=['||', '1'])
	#rs.find(name=('>', '10'))
	#print(rs.description())

	rs.find(*[])
	print('--------------', list(rs))



def _test_schema(schema):

	schema_input = os.path.join(here, 'schema')
	dbname = 'icecat'

	#print(schema.db_list())

	schema.db_drop(dbname)
	schema.db_create(dbname)

	schema.db_connect(dbname)

	schema.load_ddl(
		schema_input, 
		#with_fk=False, 
		#only_tables=['MeasureSign']
	)

	ddl = schema.ddl(
		schema_input, 
		#with_fk=False, 
		#only_tables=['MeasureSign']
	)
	ddl_ext = schema.dump_extension
	open(os.path.join(here, 'dumps/%s%s' % (dbname, ddl_ext)), 'wb').write(ddl)

	if isinstance(schema, SQLSchema) and 0:
		_test_dml(schema)

	_test_resultset(schema)

	schema.db_disconnect()


def test_main():
	schema = Schema()
	schema = SQLITE(path=os.path.join(here, 'data'))
	_schema = POSTGRESQL(
		host='localhost', port=5432, 
		user='tryton', password='tryton', 
		user_dba='tryton', password_dba='tryton', 
	)
	_schema = MYSQL(
		host='localhost', port=3306, 
		user='tryton', password='tryton', 
		user_dba='root', password_dba='el passo', 
	)

	_test_schema(schema)


if __name__ == '__main__':

	test_main()
