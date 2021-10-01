import copy
import datetime
import json
import os

import dbstream
import psycopg2
import re
import requests
from psycopg2.extras import RealDictCursor
from pyred.core.Column import change_columns_type, choose_columns_to_extend, columns_type_bool_to_str
from pyred.core.Table import create_table, create_columns
from pyred.core.tools.compare import schema_compare_tool
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
            time.sleep(5)
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
            empty_list = []
            return empty_list

    def _send(self, data, replace, batch_size=1000):
        print(C.WARNING + "Initiate send to table %s..." % data["table_name"] + C.ENDC)
        con = self.connection()
        cursor = con.cursor()
        if replace:
            cleaning_request = '''DELETE FROM ''' + data["table_name"] + ''';'''
            print(C.WARNING + "Cleaning" + C.ENDC)
            self.execute_query(cleaning_request, apply_special_env=False)
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
        print(C.HEADER + str(total_rows) + ' rows sent to Postgres/Redshift' + C.ENDC)
        print(C.OKGREEN + "[OK] Sent to postgres/redshift" + C.ENDC)
        return 0

    def _send_data_custom(self,
                          data,
                          replace=True,
                          batch_size=1000,
                          other_table_to_update=None,
                          retry=1
                          ):
        """
        data = {
            "table_name" 	: 'name_of_the_redshift_schema' + '.' + 'name_of_the_redshift_table' #Must already exist,
            "columns_name" 	: [first_column_name,second_column_name,...,last_column_name],
            "rows"		: [[first_raw_value,second_raw_value,...,last_raw_value],...]
        }
        """
        data["columns_name"] = [c.lower() for c in data["columns_name"]]
        data_copy = copy.deepcopy(data)
        try:
            self._send(data, replace=replace, batch_size=batch_size)
        except Exception as e:
            if "invalid input syntax for integer" in str(e).lower() \
                    or "invalid input syntax for type integer" in str(e).lower() \
                    or "invalid input syntax for type double precision" in str(e).lower() \
                    or "is out of range for type integer" in str(e).lower():
                change_columns_type(
                    self,
                    data=data_copy,
                    other_table_to_update=other_table_to_update
                )
            elif "invalid input syntax for type boolean:" in str(e).lower():
                columns_type_bool_to_str(
                    self,
                    data=data_copy,
                    other_table_to_update=other_table_to_update
                )
            elif "value too long for type character" in str(e).lower():
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
                if retry == 1:
                    time.sleep(10)
                    self._send_data_custom(data_copy, replace=replace, batch_size=batch_size,
                                           other_table_to_update=other_table_to_update, retry=2)
                else:
                    raise e

            self._send_data_custom(data_copy, replace=replace, batch_size=batch_size,
                                   other_table_to_update=other_table_to_update)

    def clean(self, selecting_id, schema_prefix, table):
        print('trying to clean table %s.%s using %s' % (schema_prefix, table, selecting_id))
        cleaning_query = """
                DELETE FROM %(schema_name)s.%(table_name)s WHERE %(id)s IN (SELECT distinct %(id)s FROM %(schema_name)s.%(table_name)s_temp);
                INSERT INTO %(schema_name)s.%(table_name)s 
                SELECT * FROM %(schema_name)s.%(table_name)s_temp;
                DELETE FROM %(schema_name)s.%(table_name)s_temp;
                """ % {"table_name": table,
                       "schema_name": schema_prefix,
                       "id": selecting_id}
        self.execute_query(cleaning_query)
        print('cleaned')

    def get_max(self, schema, table, field, filter_clause=""):
        try:
            r = self.execute_query("SELECT max(%s) as max FROM %s.%s %s" % (field, schema, table, filter_clause))
            return r[0]["max"]
        except IndexError:
            return None
        except (psycopg2.ProgrammingError, psycopg2.errors.InvalidSchemaName) as e:
            if "relation" in str(e) or "schema" in str(e):
                return None
            raise e

    def get_data_type(self, table_name, schema_name):
        query = """ SELECT
                column_name, data_type
                FROM
                information_schema.columns
                WHERE
                table_name = '%s' and table_schema = '%s'
                union 
                select col_name as column_name , col_type as data_type
                from pg_get_late_binding_view_cols() cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int)
                WHERE
                view_name = '%s' and view_schema = '%s'
                
                """ % (table_name, schema_name, table_name, schema_name)

        return self.execute_query(query=query, apply_special_env=False)

    def create_view_from_columns(self, view_name, columns, schema_name, table_name):
        view_query = '''DROP VIEW IF EXISTS %s ;CREATE VIEW %s as (SELECT %s FROM %s.%s)''' \
                     % (view_name, view_name, columns, schema_name, table_name)
        self.execute_query(view_query)

    def create_schema(self, schema_name):
        self.execute_query("CREATE SCHEMA %s" % schema_name)

    def drop_schema(self, schema_name):
        self.execute_query("DROP SCHEMA %s CASCADE" % schema_name)

    def schema_compare(self, schema_ref, new_schema_test):
        schema_compare_tool(self, schema_ref, new_schema_test)

    @staticmethod
    def build_pydatasource_view(query_string):
        return """
                drop view if exists {{ table_name }};
                create view {{ table_name }} as (
                %s
                ) with no schema binding ;
                """ % query_string

    @staticmethod
    def build_pydatasource_table(query_string):
        return """
                drop table if exists {{ table_name }};
                create table {{ table_name }} as (
                %s
                );
                """ % query_string

    @staticmethod
    def build_pydatasource_table_cascade(query_string):
        return """
                drop table if exists {{ table_name }} CASCADE;
                create table {{ table_name }} as (
                %s
                );
                """ % query_string
