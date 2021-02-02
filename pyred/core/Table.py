import pandas as pd
import psycopg2

from pyred.core.Column import detect_type, find_sample_value


def get_table_info(_dbstream, table_and_schema_name):
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
    return _dbstream.execute_query(query, apply_special_env=False)


def format_create_table(_dbstream, data):
    columns_name = data["columns_name"]
    rows = data["rows"]
    params = {}
    df = pd.DataFrame(rows, columns=columns_name)
    df = df.where((pd.notnull(df)), None)
    for i in range(len(columns_name)):
        name = columns_name[i]
        example_max, example_min = find_sample_value(df, name, i)
        col = dict()
        col["example"] = example_max
        type_max = detect_type(_dbstream, name=name, example=example_max)
        if type_max == "TIMESTAMP":
            type_min = detect_type(_dbstream, name=name, example=example_min)
            if type_min == type_max:
                col["type"] = type_max
            else:
                col["type"] = type_min
        else:
            col["type"] = type_max
        params[name] = col

    query = """"""
    query = query + "CREATE TABLE %(table_name)s ("
    col = list(params.keys())
    for i in range(len(col)):
        k = col[i]
        string_example = " --example:" + str(params[k]["example"])[:10].replace("\n", "").replace("%", "") + ''
        if i == len(col) - 1:
            query = query + "\n     " + k + ' ' + params[k]["type"] + ' ' + 'NULL ' + string_example
        else:
            query = query + "\n     " + k + ' ' + params[k]["type"] + ' ' + 'NULL ,' + string_example
    query = query + "\n )"
    return query


def create_table(_dbstream, data, other_table_to_update):
    query = format_create_table(_dbstream, data)
    try:
        filled_query = query % {"table_name": data["table_name"]}
        print(filled_query)
        _dbstream.execute_query(filled_query, apply_special_env=False)
        if other_table_to_update:
            _dbstream.execute_query(query % {"table_name": other_table_to_update}, apply_special_env=False)
    except psycopg2.ProgrammingError as e:
        e = str(e)
        if e[:7] == "schema ":
            _dbstream.execute_query("CREATE SCHEMA " + data['table_name'].split(".")[0], apply_special_env=False)
            _dbstream.execute_query(query % {"table_name": data["table_name"]}, apply_special_env=False)
            if other_table_to_update:
                _dbstream.execute_query(query % {"table_name": other_table_to_update}, apply_special_env=False)
        else:
            print(e)


def create_columns(_dbstream, data, other_table_to_update):
    table_name = data["table_name"]
    rows = data["rows"]
    columns_name = data["columns_name"]
    infos = get_table_info(_dbstream, table_name)
    all_column_in_table = [e['column_name'] for e in infos]
    df = pd.DataFrame(rows, columns=columns_name)
    df = df.where((pd.notnull(df)), None)
    queries = []
    for column_name in columns_name:
        if column_name not in all_column_in_table:
            example_max, example_min = find_sample_value(df, column_name, columns_name.index(column_name))
            type_max = detect_type(_dbstream, name=column_name, example=example_max)
            if type_max =="TIMESTAMP":
                type_min = detect_type(_dbstream, name=column_name, example=example_min)
                if type_min == type_max:
                    type_ = type_max
                else:
                    type_ = "VARCHAR(255)"
            else:
                type_ = type_max
            query = """
            alter table %s
            add "%s" %s
            default NULL
            """ % (table_name, column_name, type_)
            queries.append(query)
            if other_table_to_update:
                query = """
                            alter table %s
                            add "%s" %s
                            default NULL
                            """ % (other_table_to_update, column_name, type_)
                queries.append(query)
    if queries:
        query = '; '.join(queries)
        _dbstream.execute_query(query, apply_special_env=False)
    return 0
