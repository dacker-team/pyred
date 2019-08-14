# -*- coding: utf-8 -*-
import pandas as pd
import psycopg2 as psycopg2
from pyred.execute import execute_query

from . import execute

redshift_types = ["SMALLINT", "INTEGER", "BIGINT", "DECIMAL", "REAL", "DOUBLE PRECISION", "BOOLEAN", "CHAR", "VARCHAR",
                  "DATE", "TIMESTAMP", "TIMESTAMPTZ", "INT2", "INT4", "INT8", "NUMERIC", "FLOAT", "FLOAT4", "FLOAT8",
                  "BOOL", "CHARACTER", "NCHAR", "BPCHAR", "CHARACTER VARYING", "NVARCHAR", "TEXT"]


def get_table_info(instance, table_and_schema_name, existing_tunnel):
    split = table_and_schema_name.split(".")
    if len(split) == 1:
        table_name = split[0]
        schema_name = None

    elif len(split) == 2:
        table_name = split[1]
        schema_name = split[0]
    else:
        raise Exception("Invalid table or schema name")
    query = "SELECT column_name, data_type, character_maximum_length, is_nullable FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='%s'" % table_name
    if schema_name:
        query = query + " AND TABLE_SCHEMA='%s'" % schema_name
    return execute_query(instance, query, existing_tunnel)


def existing_test(instance, table_name, existing_tunnel=None):
    try:
        query = "SELECT COUNT(*) FROM " + table_name
        execute.execute_query(instance, query, existing_tunnel)
        return True
    except psycopg2.ProgrammingError:
        return False


def detect_type(instance, example, existing_tunnel=None):
    try:
        query = "SELECT CAST('%s' as TIMESTAMP)" % example
        execute.execute_query(instance, query, existing_tunnel)
        return "TIMESTAMP"

    except psycopg2.Error:
        pass

    if isinstance(example, str):
        if len(example) >= 255:
            return "VARCHAR(65000)"
        return "VARCHAR(255)"
    elif isinstance(example, bool):
        return "BOOLEAN"
    elif isinstance(example, int):
        if example > 2147483646:
            return "BIGINT"
        else:
            return "INTEGER"
    elif isinstance(example, float):
        return "FLOAT"
    else:
        return "VARCHAR(255)"


def def_type(instance, name, example, existing_tunnel, types=None):
    print('Define type of %s...' % name)
    if not types:
        return detect_type(instance, example, existing_tunnel)

    try:
        result = types[name]
        if result.split('(')[0] not in redshift_types:
            return result
        else:
            return result
    except KeyError:
        return detect_type(instance, example, existing_tunnel)


def find_sample_value(df, name, i):
    if df[name].dtype == 'object':
        df[name] = df[name].apply(lambda x: str(x) if x is not None else '')
        return df[name][df[name].map(len) == df[name].map(len).max()].iloc[0]
    else:
        rows = df.values.tolist()
        for row in rows:
            value = row[i]
            if value is not None:
                return value
        return None


def create_column(instance, data, column_name, existing_tunnel):
    table_name = data["table_name"]
    rows = data["rows"]
    columns_name = data["columns_name"]
    df = pd.DataFrame(rows, columns=columns_name)
    df = df.where((pd.notnull(df)), None)
    example = find_sample_value(df, column_name, columns_name.index(column_name))
    type = def_type(instance=instance, name=column_name, existing_tunnel=existing_tunnel, example=example)
    query = """
    alter table %s
    add column %s %s
    default NULL;
    """ % (table_name, column_name, type)
    print(query)
    execute_query(instance, query, existing_tunnel)
    return query


def create_columns(instance, data, existing_tunnel):
    table_name = data["table_name"]
    rows = data["rows"]
    columns_name = data["columns_name"]
    infos = get_table_info(instance, table_name, existing_tunnel)
    all_column_in_table = [e['column_name'] for e in infos]
    df = pd.DataFrame(rows, columns=columns_name)
    df = df.where((pd.notnull(df)), None)
    queries = []
    for column_name in columns_name:
        if column_name not in all_column_in_table:
            example = find_sample_value(df, column_name, columns_name.index(column_name))
            type_ = def_type(instance=instance, name=column_name, existing_tunnel=existing_tunnel, example=example)
            query = """
            alter table %s
            add "%s" %s
            default NULL
            """ % (table_name, column_name, type_)
            queries.append(query)
    if queries:
        query = '; '.join(queries)
        execute_query(instance, query, existing_tunnel)
    return 0


def extend_column(instance, data, column_name, existing_tunnel):
    table_name = data["table_name"]
    query = """
    ALTER TABLE %(table_name)s ADD COLUMN %(new_column_name)s VARCHAR(65000);
    UPDATE %(table_name)s SET  %(new_column_name)s = %(column_name)s;
    ALTER TABLE %(table_name)s DROP COLUMN %(column_name)s CASCADE ;
    ALTER TABLE %(table_name)s RENAME COLUMN %(new_column_name)s TO %(column_name)s;
    """ % {
        "table_name": table_name,
        "column_name": column_name,
        "new_column_name": column_name + "_new"
    }
    print(query)
    execute_query(instance, query, existing_tunnel)
    return query


def get_columns_length(instance, schema_name, table_name, existing_tunnel):
    query = """
    SELECT column_name, character_maximum_length
    FROM information_schema.columns
    WHERE table_name='%s' and table_schema='%s'
    AND character_maximum_length IS NOT NULL  
    """ % (table_name, schema_name)
    d = {}
    r = execute_query(instance, query, existing_tunnel)
    for i in r:
        d[i["column_name"]] = i["character_maximum_length"]
    return d


def choose_columns_to_extend(instance, data, existing_tunnel):
    table_name = data["table_name"].split('.')
    columns_length = get_columns_length(instance, table_name=table_name[1], schema_name=table_name[0],
                                        existing_tunnel=existing_tunnel)
    rows = data["rows"]
    columns_name = data["columns_name"]
    df = pd.DataFrame(rows, columns=columns_name)

    for c in columns_name:
        example = find_sample_value(df, c, columns_name.index(c))
        print(example)
        if isinstance(example, str):
            if len(example) >= 255:
                if not columns_length.get(c) or columns_length.get(c) < len(example):
                    extend_column(instance, data, c, existing_tunnel)


def format_create_table(instance, data, existing_tunnel, types=None):
    table_name = data["table_name"]
    columns_name = data["columns_name"]
    rows = data["rows"]
    params = {}
    df = pd.DataFrame(rows, columns=columns_name)
    df = df.where((pd.notnull(df)), None)
    for i in range(len(columns_name)):
        name = columns_name[i]
        example = find_sample_value(df, name, i)
        col = dict()
        col["example"] = example
        col["type"] = def_type(instance=instance, name=name, existing_tunnel=existing_tunnel, example=example,
                               types=types)
        params[name] = col

    query = """"""
    query = query + "CREATE TABLE " + table_name + " ("
    col = list(params.keys())
    for i in range(len(col)):
        k = col[i]
        string_example = " --example:" + str(params[k]["example"])[:10] + ''
        if i == len(col) - 1:
            query = query + "\n     " + k + ' ' + params[k]["type"] + ' ' + 'NULL ' + string_example
        else:
            query = query + "\n     " + k + ' ' + params[k]["type"] + ' ' + 'NULL ,' + string_example
    else:
        query = query[:-1]
    query = query + "\n )"
    print(query)
    return query


def create_table(instance, data, types=None, existing_tunnel=None):
    query = format_create_table(instance, data, existing_tunnel, types=types)

    def ex_query(q):
        return execute.execute_query(instance, q, existing_tunnel)

    try:
        ex_query(query)
    except psycopg2.ProgrammingError as e:
        e = str(e)
        if e[:7] == "schema ":
            ex_query("CREATE SCHEMA " + data['table_name'].split(".")[0])
            ex_query(query)
        else:
            print(e)
