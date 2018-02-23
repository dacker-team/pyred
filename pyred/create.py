# -*- coding: utf-8 -*-
import psycopg2 as psycopg2
import redshift_credentials
import datetime

connection_kwargs = redshift_credentials.credential("MH")

redshift_types = ["SMALLINT", "INTEGER", "BIGINT", "DECIMAL", "REAL", "DOUBLE PRECISION", "BOOLEAN", "CHAR", "VARCHAR",
                  "DATE", "TIMESTAMP", "TIMESTAMPTZ", "INT2", "INT4", "INT8", "NUMERIC", "FLOAT", "FLOAT4", "FLOAT8",
                  "BOOL", "CHARACTER", "NCHAR", "BPCHAR", "CHARACTER VARYING", "NVARCHAR", "TEXT"]


def execute_query(instance, query):
    con = psycopg2.connect(**connection_kwargs)
    cursor = con.cursor()
    cursor.execute(query)
    con.commit()
    cursor.close()
    con.close()


def detect_type(instance, example):
    try:
        query = "SELECT CAST('%s' as TIMESTAMP)" % example
        execute_query(instance, query)
        return "TIMESTAMP"

    except psycopg2.Error:
        pass

    if type(example) is str:
        return "VARCHAR(256)"
    elif type(example) is int:
        if example > 2147483646:
            return "BIGINT"
        else:
            return "INTEGER"
    elif type(example) is float:
        return "FLOAT"
    else:
        print("Cannot find type for %s \nPlease define it in 'types' dictionnary argument" % example)
        exit()


def def_type(instance, name, example, types=None):
    if not types:
        return detect_type(instance, example)

    try:
        result = types[name]
        if result.split('(')[0] not in redshift_types:
            boolean = raw_input('%s is apparently not in RedShift Types, do you want to continue (y or n) ?\n' % result)
            if boolean.lower() in ('y', 'yes'):
                return result
            else:
                exit()
        else:
            return result
    except KeyError:
        return detect_type(instance, example)


def format_create_table(instance, data, primary_key, types=None):
    table_name = data["table_name"]
    columns_name = data["columns_name"]
    sample_row = data["rows"][0]
    params = {}
    for i in range(len(columns_name)):
        name = columns_name[i]
        example = sample_row[i]
        col = dict()
        col["example"] = example
        col["type"] = def_type(instance, name, example, types)
        params[name] = col

    print(params)

    query = """"""
    query = query + "CREATE TABLE " + table_name + " ("
    col = params.keys()
    pk_bool = (primary_key == ())
    for i in range(len(col)):
        k = col[i]
        if (i == len(col) - 1) and pk_bool:
            query = query + "\n     " + k + ' ' + params[k]["type"] + ' ' + 'NULL ' + " --example:" + str(
                params[k]["example"]) + ''
        else:
            query = query + "\n     " + k + ' ' + params[k]["type"] + ' ' + 'NULL ,' + " --example:" + str(
                params[k]["example"]) + ''
    if not pk_bool:
        query = query + '\n     ' + "PRIMARY KEY " + str(primary_key)
    else:
        query = query[:-1]
    query = query + "\n )"
    print(query)
    return query


def create_table(instance, data, primary_key=(), types=None):
    query = format_create_table(instance, data, primary_key, types)

    def ex_query(q):
        return execute_query(instance, q)

    boolean = raw_input(
        "Do you really want to execute this query (y or n) ? \n You can modify attributes in primary_key or types arguments \n")
    if boolean.lower() in ('y', 'yes'):
        try:
            ex_query(query)
        except psycopg2.ProgrammingError, e:
            if e[:7] == "schema ":
                ex_query("CREATE SCHEMA " + data['table_name'].split(".")[0])
                ex_query(query)
            elif e[0][:9] == "Relation ":
                boolean = raw_input("Do you really want to drop table %s (y or n) ? \n" % data['table_name'])
                if boolean.lower() in ('y', 'yes'):
                    ex_query("DROP TABLE " + data['table_name'])
                    ex_query(query)
                else:
                    exit()
            else:
                print(e[:9])
    else:
        exit()


def test():
    data = {
        "table_name": 'test.test',
        "columns_name": ["nom", "prenom", "age", "date"],
        "rows": [["pif", "pif", {}, "2017-02-23"]]
    }
    primary_key = ()

    types = {
        'nom': 'VARCHAR(12)',
    }
    create_table('MH', data, primary_key, types)