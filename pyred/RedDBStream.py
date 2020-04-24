import copy
import datetime
import json
import os

import dbstream
import psycopg2
import re
import requests
from psycopg2.extras import RealDictCursor
from pyred.core.Column import choose_columns_to_extend
from pyred.core.Table import create_table, create_columns
from pyred.core.tools.print_colors import C
import time


class RedDBStream(dbstream.DBStream):
    def __init__(self, instance_name, client_id):
        super().__init__(instance_name, client_id=client_id)
        self.instance_type_prefix = "RED"
        self.ssh_init_port = 6543

    def connection(self):
        connection_kwargs = self.credentials()
        try:
            con = psycopg2.connect(**connection_kwargs, cursor_factory=RealDictCursor)
        except psycopg2.OperationalError:
            time.sleep(2)
            if self.ssh_tunnel:
                self.ssh_tunnel.close()
                self.create_tunnel()
            con = psycopg2.connect(**connection_kwargs, cursor_factory=RealDictCursor)
        return con

    def _execute_query_custom(self, query):
        con = self.connection()
        cursor = con.cursor()
        try:
            cursor.execute(query)
        except Exception as e:
            cursor.close()
            con.close()
            raise e
        con.commit()
        try:
            result = cursor.fetchall()
        except psycopg2.ProgrammingError:
            result = None
        cursor.close()
        con.close()
        query_create_table = re.search("(?i)(?<=((create table ))).*(?= as)", query)
        if result:
            return [dict(r) for r in result]
        elif query_create_table:
            return {'execute_query': query_create_table}
        else:
            return None

    def _send(self, data, replace, batch_size=1000):
        print(C.WARNING + "Initiate send_to_redshift..." + C.ENDC)
        con = self.connection()
        cursor = con.cursor()
        if replace:
            cleaning_request = '''DELETE FROM ''' + data["table_name"] + ''';'''
            print(C.WARNING + "Cleaning" + C.ENDC)
            self.execute_query(cleaning_request)
            print(C.OKGREEN + "[OK] Cleaning Done" + C.ENDC)

        boolean = True
        index = 0
        total_rows = len(data["rows"])
        total_nb_batches = len(data["rows"]) // batch_size + 1
        while boolean:
            temp_row = []
            for i in range(batch_size):
                if not data["rows"]:
                    boolean = False
                    continue
                temp_row.append(data["rows"].pop())

            final_data = []
            for x in temp_row:
                for y in x:
                    final_data.append(y)

            temp_string = ','.join(map(lambda a: '(' + ','.join(map(lambda b: '%s', a)) + ')', tuple(temp_row)))

            inserting_request = '''INSERT INTO ''' + data["table_name"] + ''' (\"''' + "\", \"".join(
                data["columns_name"]) + '''\") VALUES ''' + temp_string + ''';'''
            if final_data:
                try:
                    cursor.execute(inserting_request, final_data)
                except Exception as e:
                    cursor.close()
                    con.close()
                    raise e
            index = index + 1
            percent = round(index * 100 / total_nb_batches, 2)
            if percent < 100:
                print("\r   %s / %s (%s %%)" % (str(index), total_nb_batches, str(percent)), end='\r')
            else:
                print("\r   %s / %s (%s %%)" % (str(index), total_nb_batches, str(percent)))
        con.commit()

        cursor.close()
        con.close()
        print(C.HEADER + str(total_rows) + ' rows sent to Redshift' + C.ENDC)
        print(C.OKGREEN + "[OK] Sent to redshift" + C.ENDC)
        return 0

    def _send_data_custom(self,
                          data,
                          replace=True,
                          batch_size=1000,
                          other_table_to_update=None
                          ):
        """
        data = {
            "table_name" 	: 'name_of_the_redshift_schema' + '.' + 'name_of_the_redshift_table' #Must already exist,
            "columns_name" 	: [first_column_name,second_column_name,...,last_column_name],
            "rows"		: [[first_raw_value,second_raw_value,...,last_raw_value],...]
        }
        """
        data_copy = copy.deepcopy(data)
        try:
            self._send(data, replace=replace, batch_size=batch_size)
        except Exception as e:
            if "value too long for type character" in str(e).lower():
                choose_columns_to_extend(
                    self,
                    data=data_copy,
                    other_table_to_update=other_table_to_update
                )
            elif "does not exist" in str(e).lower() and "column" in str(e).lower():
                create_columns(
                    self,
                    data=data_copy,
                    other_table_to_update=other_table_to_update
                )
            elif "does not exist" in str(e).lower() and ("relation" in str(e).lower() or "schema" in str(e).lower()):
                print("Destination table doesn't exist! Will be created")
                create_table(
                    self,
                    data=data_copy,
                    other_table_to_update=other_table_to_update
                )
                replace = False

            else:
                raise e

            self._send_data_custom(data_copy, replace=replace, batch_size=batch_size,
                                   other_table_to_update=other_table_to_update)
