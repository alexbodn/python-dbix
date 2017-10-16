# python-dbix

load a perl DBIx::Class schema with python

WHAT IS DBIx::Class ?

DBIx::Class (also known as DBIC) is an extensible and 
flexible Object/Relational Mapper (ORM) written in Perl. 
ORMs speed development, abstract data and make it pole, 
allow you to represent your business rules through OO code 
and generate boilerplate code for CRUD operations.

see http://www.dbix-class.org/

since perl syntax is quite complex, 
the input schema might need a few adjustments.

this code does correctly convert the examples 
included in the tests/schema and tests/example, 
both original schemas taken from the wild.

psycopg2 is needed for postgresql databases.
MySQL-python is needed for mysql databases.

