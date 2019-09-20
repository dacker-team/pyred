import pandas as pd
import psycopg2


def extend_column(_dbstream, table_name, column_name):
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
    _dbstream.execute_query(query)
    return query


def get_columns_length(_dbstream, schema_name, table_name):
    query = """
    SELECT column_name, character_maximum_length
    FROM information_schema.columns
    WHERE table_name='%s' and table_schema='%s'
    AND character_maximum_length IS NOT NULL  
    """ % (table_name, schema_name)
    d = {}
    r = _dbstream.execute_query(query)
    for i in r:
        d[i["column_name"]] = i["character_maximum_length"]
    return d


def choose_columns_to_extend(_dbstream, data, other_table_to_update):
    table_name = data["table_name"].split('.')
    columns_length = get_columns_length(_dbstream, table_name=table_name[1], schema_name=table_name[0])
    rows = data["rows"]
    columns_name = data["columns_name"]
    df = pd.DataFrame(rows, columns=columns_name)

    for c in columns_name:
        example = find_sample_value(df, c, columns_name.index(c))
        if isinstance(example, str):
            if len(example) >= 255:
                if not columns_length.get(c) or columns_length.get(c) < len(example):
                    extend_column(_dbstream, table_name=data["table_name"], column_name=c)
                    if other_table_to_update:
                        extend_column(_dbstream, table_name=other_table_to_update, column_name=c)


def detect_type(_dbstream, name, example):
    print('Define type of %s...' % name)
    try:
        query = "SELECT CAST('%s' as TIMESTAMP)" % example
        _dbstream.execute_query(query)
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