"""
Listen to a UDP socket for QSO information from N1MM instances, and push them
to a QSO database.

N1MM has to be configured with [IP of current host]:12060 in the "Contact" field under Config -> Config Ports ... -> Broadcast Data.

Run with --help to see options.

Assumes a configuration file containing postgresql connection information.
Example:

    ```
    [db_config]
    dbname=qso_database
    user=qso_database_user
    password=somepassword
    hostname=localhost
    port=5432
    ```
"""


#!/usr/bin/env python3
import socket
import sys
import xmltodict
import qso_database
import argparse
import configparser

def contact_to_short_string(contact):
    return str(contact['timestamp']) + ' ' + str(contact['call'])

class n1mm_handler:
    """
    Handler of N1MM XML contact messages.
    """

    def __init__(self, qso_db, verbose=False):
        """
        Create handler.

        Parameters
        ----------
        qso_db: qso_database instance
            QSO database.
        verbose: boolean, optional
            Whether to print status messages to stdout.
        """

        self.last_deleted_contact = None
        self.qso_db = qso_db
        self.verbose = verbose

    def handle_message(self, msg):
        """
        Read N1MM XML message and do the appropriate database action.

        Parameters
        ----------
        msg: str
            XML message.
        """

        msg = xmltodict.parse(msg)

        if 'contactinfo' in msg.keys():
            #insert new QSO
            contact = msg['contactinfo']
            self.qso_db.insert_qso(contact)

            if self.verbose:
                print('Inserted ' + contact_to_short_string(contact))
        elif 'contactdelete' in msg.keys():
            #delete QSO
            contact = msg['contactdelete']
            qso_id = self.qso_db.get_qso_id(contact['timestamp'], contact['call'])
            self.qso_db.delete_qso(qso_id)
            if self.verbose:
                print('Deleted ', contact_to_short_string(contact), ' at qsoid = ', qso_id)

            #keep information about the deleted QSO since it might lead to
            #a subsequent contactreplace
            self.last_deleted_contact = contact
            self.last_deleted_contact['qsoid'] = qso_id
        elif 'contactreplace' in msg.keys():
            contact = msg['contactreplace']

            #contactreplace is preceded by contactdelete, but contactdelete
            #sometimes does not contain sufficient information to identify the QSO.
            #Wild ride for obtaining a qsoid:

            if self.last_deleted_contact['qsoid'] is None:
                #qsoid was not possible to obtain from contactdelete message, got None.
                #(contactdelete sometimes contains 1970-01-01 as timestamp, None as call.)
                #Try to obtain from contactreplace message instead
                qsoid = self.qso_db.get_qso_id(contact['timestamp'], contact['call'])
            else:
                #qsoid was obtainable from contactdelete message.
                qsoid = self.last_deleted_contact['qsoid']

                #... but this also means that the QSO was deleted, so need
                #to revert that in order to be able to modify it
                self.qso_db.undo_delete(qsoid)

            if qsoid is None:
                #could not look up qsoid based on contactdelete or contactreplace,
                #so timestamp or call is to be changed without us having obtained the information from
                #contactdelete. Try to guess:
                candidates = []
                candidates.append(self.qso_db.get_qso_id(contact['timestamp'], self.last_deleted_contact['call']))
                candidates.append(self.qso_db.get_qso_id(self.last_deleted_contact['timestamp'], contact['call']))
                qsoid = [v for v in candidates if v is not None]
                if len(qsoid) > 0:
                    qsoid = qsoid[0]
                else:
                    if self.verbose:
                        print('Gave up, not able to find matching QSO record for contactreplace: ', contact_to_short_string(contact))
                    return

            #modify QSO
            self.qso_db.update_qso(qsoid, contact)
            if self.verbose:
                print('Replaced ', contact_to_short_string(contact), 'at qsoid = ', qsoid)

if __name__ == '__main__':
    #UDP socket against N1MM
    n1mm = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    n1mm.bind(('', 12060))

    #read db config
    parser = argparse.ArgumentParser(description='Listen for XML messages from N1MM, push to QSO database.')
    parser.add_argument('--db-config-path', default='db_config.ini', help='Path to config file containing postgresql connection information. See docstring of n1mm_listener.py for description.')
    parser.add_argument('--verbose', action='store_true', help='Whether to print verbose messages.')
    args = parser.parse_args()
    db_config = configparser.ConfigParser()
    db_config.read(args.db_config_path)
    db_config = db_config['db_config']

    #qso database
    qso_db = qso_database.qso_database(dbname=db_config['dbname'],
                                     user=db_config['user'],
                                     password=db_config['password'],
                                     host=db_config['hostname'],
                                     port=db_config['port'])

    #n1mm handler
    handler = n1mm_handler(qso_db, verbose=args.verbose)

    #receive and parse contacts
    while (True):
        msg = n1mm.recv(5024);
        try:
            msg = msg.decode('utf-8')
            handler.handle_message(msg)
        except xmltodict.expat.ExpatError:
            print("XML message not well-formed, continuing")
            continue
