import copy
import datetime
import pandas as pd
import psycopg2


def change_type(_dbstream, table_name, column_name, type):
    query = """
    ALTER TABLE %(table_name)s ADD COLUMN %(new_column_name)s %(type)s;
    UPDATE %(table_name)s SET  %(new_column_name)s = %(column_name)s;
    ALTER TABLE %(table_name)s DROP COLUMN %(column_name)s CASCADE ;
    ALTER TABLE %(table_name)s RENAME COLUMN %(new_column_name)s TO %(column_name)s;
    """ % {
        "table_name": table_name,
        "column_name": column_name,
        "new_column_name": column_name + "_new",
        "type": type
    }
    _dbstream.execute_query(query, apply_special_env=False)
    return query


def bool_to_str(_dbstream, table_name, column_name):
    query = """
    ALTER TABLE %(table_name)s ADD COLUMN %(new_column_name)s VARCHAR(255);
    UPDATE %(table_name)s SET  %(new_column_name)s = case when %(column_name)s=True then 'True' when %(column_name)s=False then 'False' end;
    ALTER TABLE %(table_name)s DROP COLUMN %(column_name)s CASCADE ;
    ALTER TABLE %(table_name)s RENAME COLUMN %(new_column_name)s TO %(column_name)s;
    """ % {
        "table_name": table_name,
        "column_name": column_name,
        "new_column_name": column_name + "_new"
    }
    _dbstream.execute_query(query, apply_special_env=False)
    return query


def get_columns_length(_dbstream, schema_name, table_name):
    query = """
    SELECT column_name, character_maximum_length
    FROM information_schema.columns
    WHERE table_name='%s' and table_schema='%s'
    AND character_maximum_length IS NOT NULL  
    """ % (table_name, schema_name)
    d = {}
    r = _dbstream.execute_query(query, apply_special_env=False)
    for i in r:
        d[i["column_name"]] = i["character_maximum_length"]
    return d


def get_columns_type(_dbstream, schema_name, table_name):
    query = """
    SELECT column_name, udt_name
    FROM information_schema.columns
    WHERE table_name='%s' and table_schema='%s' 
    """ % (table_name, schema_name)
    d = {}
    r = _dbstream.execute_query(query, apply_special_env=False)
    for i in r:
        d[i["column_name"]] = i["udt_name"]
    return d


def choose_columns_to_extend(_dbstream, data, other_table_to_update):
    table_name = data["table_name"].split('.')
    columns_length = get_columns_length(_dbstream, table_name=table_name[1], schema_name=table_name[0])
    rows = data["rows"]
    columns_name = data["columns_name"]
    df = pd.DataFrame(rows, columns=columns_name)

    for c in columns_name:
        example = find_sample_value(df, c, columns_name.index(c))[0]
        if isinstance(example, str):
            if len(str(example.encode())) >= 255:
                if not columns_length.get(c) or columns_length.get(c) < len(str(example.encode())):
                    change_type(_dbstream, table_name=data["table_name"], column_name=c,
                                type="VARCHAR(%s)" % len(str(example.encode())))
                    if other_table_to_update:
                        change_type(_dbstream, table_name=other_table_to_update, column_name=c,
                                    type="VARCHAR(%s)" % len(str(example.encode())))


def change_columns_type(_dbstream, data, other_table_to_update):
    table_name = data["table_name"].split('.')
    columns_type = get_columns_type(_dbstream, table_name=table_name[1], schema_name=table_name[0])
    rows = data["rows"]
    columns_name = data["columns_name"]
    df = pd.DataFrame(rows, columns=columns_name)

    for c in columns_name:
        example = find_sample_value(df, c, columns_name.index(c))[0]
        if isinstance(example, float):
            if columns_type.get(c) != "float8":
                change_type(_dbstream, table_name=data["table_name"], column_name=c, type="float8")
                if other_table_to_update:
                    change_type(_dbstream, table_name=other_table_to_update, column_name=c, type="float8")
        if isinstance(example, str):
            if columns_type.get(c) not in ["varchar", "bool", "timestamp"]:
                change_type(_dbstream, table_name=data["table_name"], column_name=c, type="VARCHAR(255)")
                if other_table_to_update:
                    change_type(_dbstream, table_name=other_table_to_update, column_name=c, type="VARCHAR(255)")
        if isinstance(example, int) and (example > 2147483646):
            if columns_type.get(c) != "int8":
                change_type(_dbstream, table_name=data["table_name"], column_name=c, type="int8")
                if other_table_to_update:
                    change_type(_dbstream, table_name=other_table_to_update, column_name=c, type="float8")


def columns_type_bool_to_str(_dbstream, data, other_table_to_update):
    table_name = data["table_name"].split('.')
    columns_type = get_columns_type(_dbstream, table_name=table_name[1], schema_name=table_name[0])
    rows = data["rows"]
    columns_name = data["columns_name"]
    df = pd.DataFrame(rows, columns=columns_name)

    for c in columns_name:
        example = find_sample_value(df, c, columns_name.index(c))[0]
        if isinstance(example, str):
            if columns_type.get(c) == "bool":
                bool_to_str(_dbstream, table_name=data["table_name"], column_name=c)
                if other_table_to_update:
                    bool_to_str(_dbstream, table_name=other_table_to_update, column_name=c)


def detect_type(_dbstream, name, example):
    print('Define type of %s...' % name)
    try:
        query = "SELECT CAST('%s' as TIMESTAMP)" % example
        _dbstream.execute_query(query, apply_special_env=False)
        return "TIMESTAMP"

    except psycopg2.Error:
        pass
    if isinstance(example, datetime.date):
        return "TIMESTAMP"
    elif isinstance(example, str):
        if len(example) >= 255:
            return "VARCHAR(%s)" % len(str(example.encode()))
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


def convert_to_bool(x):
    if x.lower() == "true" or x == 1 or x.lower() == "t":
        return True
    if x.lower() == "false" or x == 0 or x.lower() == "f":
        return False
    else:
        raise Exception


def convert_to_int(x):
    if x[-2:] == ".0":
        return int(x.replace(".0", ""))
    else:
        return int(x)


def len_or_max(s):
    if isinstance(s, str):
        return len(s)
    return s


def find_sample_value(df, name, i):
    df1 = df[name].dropna()
    try:
        df1 = df1.apply(lambda x: str(x))
    except:
        pass
    try:
        df1 = df1.apply(lambda x: convert_to_bool(x))
    except:
        try:
            df1 = df1.apply(lambda x: convert_to_int(x))
        except:
            try:
                df1 = df1.apply(lambda x: float(x))
            except:
                pass
    df1_copy = copy.deepcopy(df1)
    if df1.dtype == 'object':
        df1 = df1.apply(lambda x: (str(x.encode()) if isinstance(x, str) else x) if x is not None else '')
        if df1.empty:
            return None, None
        else:
            return df1_copy[df1.map(len_or_max) == df1.map(len_or_max).max()].iloc[0], \
                   df1_copy[df1.map(len_or_max) == df1.map(len_or_max).min()].iloc[0]
    elif df1.dtype == 'int64':
        max = int(df1.max())
        min = int(df1.min())
        return max, min
    elif df1.dtype == 'float64':
        max = float(df1.max())
        min = float(df1.min())
        return max, min
    else:
        rows = df.values.tolist()
        for row in rows:
            if row[i] is not None:
                return row[i], row[i]
        return None, None
