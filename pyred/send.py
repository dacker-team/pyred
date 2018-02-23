# -*- coding: utf-8 -*-
import psycopg2 as psycopg2
from . import redshift_credentials


def send_to_redshift(instance, data, replace=True, batch_size=1000):
    """
    data = {
        "table_name" 	: 'name_of_the_redshift_schema' + '.' + 'name_of_the_redshift_table' #Must already exist,
        "columns_name" 	: [first_column_name,second_column_name,...,last_column_name],
        "rows"		: [[first_raw_value,second_raw_value,...,last_raw_value],...]
    }
    """

    connection_kwargs = redshift_credentials.credential(instance)
    print("Initiate send_to_redshift...")
    con = psycopg2.connect(**connection_kwargs)
    cursor = con.cursor()

    if replace:
        cleaning_request = '''DELETE FROM ''' + data["table_name"] + ''';'''
        print("Cleaning")
        cursor.execute(cleaning_request)
        print("Cleaning Done")

    test = True
    index = 0
    while test:
        temp_row = []
        for i in range(batch_size):
            if not data["rows"]:
                test = False
                continue
            temp_row.append(data["rows"].pop())

        final_data = []
        for x in temp_row:
            for y in x:
                final_data.append(y)

        temp_string = ','.join(map(lambda a: '(' + ','.join(map(lambda b: '%s', a)) + ')', tuple(temp_row)))

        inserting_request = '''INSERT INTO ''' + data["table_name"] + ''' (''' + ", ".join(
            data["columns_name"]) + ''') VALUES ''' + temp_string + ''';'''
        if final_data:
            print("Execute")
            cursor.execute(inserting_request, final_data)
        index = index + 1
        print(index)
    con.commit()

    cursor.close()
    con.close()

    print("data sent to redshift")
    return 0
