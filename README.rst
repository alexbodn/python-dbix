====
dbix
====


.. image:: https://img.shields.io/pypi/v/dbix.svg
        :target: https://pypi.python.org/pypi/dbix

.. image:: https://img.shields.io/travis/alexbodn/dbix.svg
        :target: https://travis-ci.org/alexbodn/dbix

.. image:: https://readthedocs.org/projects/dbix/badge/?version=latest
        :target: https://dbix.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://pyup.io/repos/github/alexbodn/dbix/shield.svg
     :target: https://pyup.io/repos/github/alexbodn/dbix/
     :alt: Updates


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

* Free software: Apache Software License 2.0
* Documentation: https://dbix.readthedocs.io.


Features
--------

* TODO

Credits
---------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage

