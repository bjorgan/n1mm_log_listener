"""
Interface against QSO database, primarily for use by n1mm_listener.py for
pushing new QSOs from N1MM, made to be convenient for that usecase. Might in
other case be more convenient to work on pyscopg-interface directly.
"""

import psycopg2
import sqlite3
import datetime

class qso_database:
    """
    Interface for QSO database.
    """

    def __init__(self, dbname, user, password, host='localhost', port=5432):
        """
        Connect to postgresql database.

        Parameters
        ----------
        dbname: str
            Name of database.
        user: str
            Username
        password: str
            Password
        host: str, optional
            Hostname of postgresql server
        port: int, optional
            Port of postgresql server
        """

        self.connection = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

    def insert_qso(self, contactinfo):
        """
        Insert new QSO into database.

        Parameters
        ----------
        contactinfo: dict
            Contactinfo structure as obtained directly from N1MM XML message.
            Assumed to contain fields: contestname, timestamp, band, rxfreq,
            txfreq, countryprefix, operator, mode, call, snt, rcv, comment,
            continent.
        """

        c = self.connection.cursor()
        c.execute('''INSERT INTO qsos (contestname, timestamp, band, rxfreq, txfreq, countryprefix, operator, mode, call, snt, rcv, comment, continent)
                              VALUES (%(contestname)s, %(timestamp)s, %(band)s, %(rxfreq)s, %(txfreq)s, %(countryprefix)s, %(operator)s, %(mode)s, %(call)s, %(snt)s, %(rcv)s, %(comment)s, %(continent)s)''',
            (contactinfo))
        self.connection.commit()

    def update_qso(self, qso_id, contactinfo):
        """
        Update QSO.

        Parameters
        ----------
        qso_id: int
            ID of QSO to update.
        contactinfo: dict
            QSO details. See insert_qso() for assumed fields.
        """

        c = self.connection.cursor()
        contactinfo['qsoid'] = qso_id

        c.execute('''UPDATE qsos SET contestname = %(contestname)s, timestamp = %(timestamp)s, band = %(band)s, rxfreq = %(rxfreq)s, txfreq = %(txfreq)s, countryprefix = %(countryprefix)s, operator = %(operator)s, mode = %(mode)s, call = %(call)s, snt = %(snt)s, rcv = %(rcv)s, comment = %(comment)s, continent = %(continent)s
                              WHERE qsoid = %(qsoid)s''',
            (contactinfo))
        self.connection.commit()

    def delete_qso(self, qso_id):
        """
        Delete a QSO.

        Parameters
        ----------
        qso_id: int
            ID of QSO to remove
        """
        c = self.connection.cursor()
        c.execute('DELETE FROM qsos WHERE qsoid = %s', (qso_id,))
        self.connection.commit()

    def undo_delete(self, qso_id):
        """
        Undo deletion of a QSO. Has no effect on non-deleted QSOs.  Needed due
        to N1MM shenanigans when it updates QSOs (sends contactdelete before
        contactreplace).

        Parameters
        ----------
        qso_id: int
            ID of QSO to remove
        """

        # This was first implemented as a part of the database API itself, but
        # got rather ugly since it was implemented as the DELETE of the view
        # deleted_qsos. Found that it was more confusing than anything else,
        # and should not really be needed in general except for the specific
        # usecase when N1MM sends contactdelete followed by contactreplace, so
        # doing it here in the wrapper instead.
        #
        # Still, since this function has to access qsos_raw directly, this is
        # not an optimal situation.

        c = self.connection.cursor()

        #check first if QSO is deleted
        c.execute('''SELECT * FROM deleted_qsos WHERE qsoid = %s''', (qso_id,))
        if c.fetchone() is not None:
            #insert previous version of QSO before it was deleted into the database again.
            #finds most recent, non-null entry in qsos_raw and uses its values to add a new record in qsos_raw.
            c.execute('''INSERT INTO qsos_raw
                         (qsoid, contestname, timestamp, band, rxfreq, txfreq, countryprefix, operator, mode, call, snt, rcv, comment, continent, modifiedby)
                         SELECT
                         qsos_raw.qsoid, contestname, timestamp, band, rxfreq, txfreq, countryprefix, operator, mode, call, snt, rcv, comment, continent, modifiedby
                         FROM (SELECT qsoid, MAX(modified) as modified
                               FROM qsos_raw WHERE call IS NOT NULL AND timestamp IS NOT NULL
                               GROUP BY qsoid) as f
                               JOIN qsos_raw ON qsos_raw.qsoid = f.qsoid AND qsos_raw.modified = f.modified
                         WHERE qsos_raw.qsoid = %s''', (qso_id,))
            self.connection.commit()

    def get_qso_id(self, timestamp, call):
        """
        Get QSO ID of a QSO in the database with the given field properties.
        Used for look-up for N1MM, since (timestamp, call) is a unique ID in
        the N1MM database.

        Parameters
        ----------
        timestamp: str
            Timestamp
        call: str
            Callsign
        """

        c = self.connection.cursor()
        c.execute('''SELECT qsoid FROM qsos WHERE timestamp = %s AND call = %s''', (timestamp, call,))
        result = c.fetchone()
        if hasattr(result, '__len__'):
            return result[0]
        else:
            return None

