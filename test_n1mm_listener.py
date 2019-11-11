"""
Tests for N1MM listener.
"""

import time
import unittest
import testing.postgresql
from sqlalchemy import create_engine
import datetime
import numpy as np
import test_sql_database
from qso_database import qso_database
from n1mm_listener import n1mm_handler
import tempfile
import shutil
import subprocess
import socket

def tearDownModule():
    test_sql_database.tearDownModule()

class n1mm_handler_test(test_sql_database.db_testcase):
    """
    Basic functionality test of N1MM handler.  Test that list of N1MM xml
    messages input directly are handled correctly.
    """

    def setUp(self):
        super().setUp()
        dsn = self.postgresql.dsn()
        self.db = qso_database(host=dsn['host'], user=dsn['user'], dbname=dsn['database'], password='', port=dsn['port'])
        self.handler = n1mm_handler(self.db)

    def tearDown(self):
        super().tearDown()

    def test_operation_on_list_of_n1mm_xml_messages(self):
        fid = open('test_data/n1mm_messages.dat', 'r')
        n1mm_messages = fid.readlines()
        fid.close()
        for msg in n1mm_messages:
            self.handler.handle_message(msg)

        #end results should be 4 QSOs
        results = [r[0] for r in self.connection.execute('''SELECT call FROM qsos ORDER BY timestamp ASC''')]
        np.testing.assert_array_equal(results, ['LA3WUA', 'LA1ARK', 'LA6MSA', 'LA6PRA'])

class n1mm_listener_executable_test(test_sql_database.db_testcase):
    """
    Test that the listener executable works as expected, and can insert QSOs through
    the socket interface.
    """

    def setUp(self):
        super().setUp()
        dsn = self.postgresql.dsn()

        #create database config in temporary directory
        self.test_dir = tempfile.mkdtemp()

        db_config_path = self.test_dir + '/db_config.ini'
        f = open(db_config_path, 'w')
        f.write('[db_config]\n')
        f.write('user=' + dsn['user'] + '\n')
        f.write('password=' + '\n')
        f.write('dbname=' + dsn['database'] + '\n')
        f.write('hostname=' + dsn['host'] + '\n')
        f.write('port=' + str(dsn['port']) + '\n')
        f.close()

        #run n1mm listener
        self.listener = subprocess.Popen(["/usr/bin/python3", "n1mm_listener.py", "--db-config-path=" + db_config_path])
        time.sleep(0.5) #takes some time to start up, quickfixed

        #create socket
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def tearDown(self):
        self.listener.terminate()
        self.listener.wait()
        super().tearDown()
        shutil.rmtree(self.test_dir)
        self.udp.close()

    def test_listener_executable_can_receive_n1mm_messages_over_socket_connection(self):
        #send xml messages to socket
        fid = open('test_data/n1mm_messages.dat', 'r')
        n1mm_messages = fid.readlines()
        fid.close()
        for msg in n1mm_messages:
            self.udp.sendto(msg.encode('utf-8'), ('localhost', 12060))
        time.sleep(0.5) #wait for listener to process

        #check that all results were inserted
        results = [r[0] for r in self.connection.execute('''SELECT call FROM qsos ORDER BY timestamp ASC''')]
        np.testing.assert_array_equal(results, ['LA3WUA', 'LA1ARK', 'LA6MSA', 'LA6PRA'])

if __name__ == '__main__':
    unittest.main()
