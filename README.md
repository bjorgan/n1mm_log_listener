QSO collector system
====================

See https://www.la1k.no/?p=5142 for description.

n1mm_listener.py
----------------

Listens for contacts from N1MM, pushes to QSO database. See
docstring for details.

Uses qso_database.py as an interface to the QSO database.

See `test_n1mm_listener.py` for tests of the listener functionality.

qso_database.sql
----------------

SQL table definitions for postgresql QSO database. See
comments within for rationale.

Bare-bones setup: Import into postgresql by
running psql as `psql -d [dbname] -U [user_name]`, and run `\i
qso_database.sql`.

See `test_sql_database.py` for tests of the database functionality.
