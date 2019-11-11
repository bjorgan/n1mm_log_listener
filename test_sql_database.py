"""
Tests for database specified in qso_database.sql.

Requires packages testing.postgresql and sqlalchemy.
"""

import unittest
import testing.postgresql
from sqlalchemy import create_engine
import datetime
import numpy as np

def str_to_datetime(string):
    """
    Convert string of format YYYY-mm-dd HH:MM to datetime.
    """
    return datetime.datetime.strptime(string, '%Y-%m-%d %H:%M')

def insert_qso(connection, record):
    """
    Insert record into database.
    """
    connection.execute("""INSERT INTO qsos (timestamp, call, operator) VALUES (%s, %s, %s)""", record)

#produces clean databases
Postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)

#standard record used for testing
record = [str_to_datetime('2019-08-10 13:30'), 'LA3WUA', 'LA9SSA']

def tearDownModule():
    Postgresql.clear_cache()

class db_testcase(unittest.TestCase):
    """
    Parent class for testing against database.
    """

    def setUp(self):
        #create new database
        self.postgresql = Postgresql()

        #connect to database
        self.engine = create_engine(self.postgresql.url())
        self.connection = self.engine.connect()
        self.trans = self.connection.begin()

        #import sql schemas
        with open("qso_database.sql", "r") as schema:
            self.connection.execute(schema.read())
            self.trans.commit()

    def tearDown(self):
        #disconnect from database
        self.connection.close()

        #remove database
        self.postgresql.stop()


class basic_functionality(db_testcase):
    """
    Test basic functionality like INSERT, DELETE and UPDATE.
    """

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_insert_yields_new_record_in_qsos_and_qsos_raw(self):
        #insert new QSO
        insert_qso(self.connection, record)

        #check that it exists in qsos
        results = self.connection.execute("""SELECT timestamp, call, operator FROM qsos""")
        np.testing.assert_array_equal(record, list(results)[0])

        #check that it exists in qso_raw
        results = self.connection.execute("""SELECT timestamp, call, operator FROM qsos_raw""")
        np.testing.assert_array_equal(record, list(results)[0])

    def test_delete_adds_blank_record_in_qsos_raw_and_removes_record_in_qsos(self):
        insert_qso(self.connection, record)

        #run delete
        self.connection.execute("""DELETE FROM qsos WHERE qsoid = 1""")

        #check that qso was deleted
        results = self.connection.execute("""SELECT timestamp, call, operator FROM qsos WHERE qsoid = 1""")
        self.assertEqual(len(list(results)), 0)

        #check that original version and a NULL-version exists in qsos_raw
        results = self.connection.execute("""SELECT timestamp, call, operator FROM qsos_raw WHERE qsoid = 1""")
        results = list(results)
        np.testing.assert_array_equal(results[0], record)
        np.testing.assert_array_equal(results[1], [None]*len(record))
        self.assertEqual(len(list(results)), 2)

    def test_update_adds_new_version_in_qsos_raw_and_updated_record_in_qsos(self):
        insert_qso(self.connection, record)

        #update the QSO
        self.connection.execute("""UPDATE qsos SET call = 'LB7RH' WHERE qsoid = 1""")

        updated_record = record.copy()
        updated_record[1] = 'LB7RH'

        #check that the QSO was updated
        results = list(self.connection.execute("""SELECT timestamp, call, operator FROM qsos WHERE qsoid = 1"""))
        self.assertEqual(len(list(results)), 1)

        np.testing.assert_array_equal(updated_record, results[0])

        #check that both versions exist in qsos_raw
        results = list(self.connection.execute("""SELECT timestamp, call, operator FROM qsos_raw WHERE qsoid = 1"""))
        self.assertEqual(len(list(results)), 2)
        np.testing.assert_array_equal(record, results[0])
        np.testing.assert_array_equal(updated_record, results[1])

class multiple_updates_of_multiple_records(db_testcase):
    """
    Test that a bit more complicated UPDATE of multiple records yield the expected results in the database tables.
    """

    def setUp(self):
        super().setUp()

        #add basic records
        self.records = [['2019-08-01 13:45', 'LA3WUA', 'LA9SSA'],
                        ['2019-09-02 10:15', 'LA1K', 'LA9SSA'],
                        ['2019-10-11 22:00', 'LA6MSA', 'LA3WUA'],
                        ['2018-05-17 10:00', 'LA1ARK', 'LB1SH']]
        for r in self.records:
            insert_qso(self.connection, r)

        #update each record a given number of times
        self.num_updates = (np.random.sample(len(self.records))*10).astype(int)
        self.new_operator = 'LA1BFA'
        for i, updates in enumerate(self.num_updates):
            for j in range(updates):
                self.connection.execute("""UPDATE qsos SET operator = %s WHERE qsoid = %s""", (self.new_operator, i+1))

    def test_qsos_contain_only_updated_records(self):
        results = list(self.connection.execute("""SELECT qsoid, timestamp, call, operator FROM qsos ORDER BY qsoid ASC"""))
        self.assertEqual(len(results), len(self.records))
        for i, result in enumerate(results):
            #unique qsoids
            self.assertEqual(result[0], i+1)

            #timestamps equal
            self.assertEqual(result[1], str_to_datetime(self.records[i][0]))

            #callsigns equal
            self.assertEqual(result[2], self.records[i][1])

            #either new or old operator, depending on whether this record was updated
            if self.num_updates[i] > 0:
                self.assertEqual(result[3], self.new_operator)
            else:
                self.assertEqual(result[3], self.records[i][2])

    def test_qsos_raw_contains_all_update_history_and_qsos_yields_last_modified_version(self):
        results = list(self.connection.execute("""SELECT qsoid, timestamp, call, operator FROM qsos ORDER BY qsoid ASC"""))
        for i in range(len(results)):
            qsoid = i+1
            results = list(self.connection.execute("""SELECT modified FROM qsos WHERE qsoid = %s""", (qsoid,)))
            results_raw = list(self.connection.execute("""SELECT modified FROM qsos_raw WHERE qsoid = %s ORDER BY modified ASC""", (qsoid,)))

            #one record per update + initial record
            self.assertEqual(len(results_raw), self.num_updates[i]+1)

            #record in qsos is the most recent version from qsos_raw
            self.assertEqual(results[0][0], results_raw[-1][0])

    def test_deletion_removes_specific_record_from_qsos(self):
        #delete a record
        self.connection.execute("""DELETE FROM qsos WHERE qsoid = 3""")

        #check that the record was removed, the rest are intact
        qsoids = list(self.connection.execute("""SELECT qsoid FROM qsos ORDER BY qsoid"""))
        self.assertFalse(3 in qsoids)
        self.assertEqual(len(qsoids), len(self.records)-1)
        np.testing.assert_array_equal(np.squeeze(qsoids), np.unique(qsoids))

    def tearDown(self):
        super().tearDown()

if __name__ == '__main__':
    unittest.main()
