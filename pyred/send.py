# -*- coding: utf-8 -*-
import copy
import os
import psycopg2 as psycopg2
import sshtunnel

from pyred.tools.print_colors import C
from pyred.tunnel import create_tunnel

from pyred.create import choose_columns_to_extend, create_column, create_columns

from . import create
from . import redshift_credentials


def send_to_redshift(
        instance,
        data,
        replace=True,
        batch_size=1000,
        types=None,
        existing_tunnel=None):
    """
    data = {
        "table_name" 	: 'name_of_the_redshift_schema' + '.' + 'name_of_the_redshift_table' #Must already exist,
        "columns_name" 	: [first_column_name,second_column_name,...,last_column_name],
        "rows"		: [[first_raw_value,second_raw_value,...,last_raw_value],...]
    }
    """
    data_copy = copy.deepcopy(data)
    try:
        send_data_to_redshift(
            instance,
            data,
            replace=replace,
            batch_size=batch_size,
            types=types,
            existing_tunnel=existing_tunnel)
    except Exception as e:
        if "value too long for type character" in str(e).lower():
            choose_columns_to_extend(instance, data_copy, existing_tunnel)
        elif "does not exist" in str(e).lower() and "column" in str(e).lower():
            create_columns(instance, data_copy, existing_tunnel)
        else:
            print(e)
            return 0
        send_to_redshift(
            instance,
            data_copy,
            replace=replace,
            batch_size=batch_size,
            types=types,
            existing_tunnel=existing_tunnel)


def send_data_to_redshift(
        instance,
        data,
        replace,
        batch_size,
        types,
        existing_tunnel):
    connection_kwargs = redshift_credentials.credential(instance)
    print(C.WARNING + "Initiate send_to_redshift..." + C.ENDC)

    print("Test to know if the destination table exists...")
    if not create.existing_test(instance, data["table_name"], existing_tunnel):
        print("Destination table doesn't exist! Will be created")
        create_boolean = True
    else:
        create_boolean = False
        print("Destination table exists well")

    if create_boolean:
        create.create_table(instance, data, types, existing_tunnel)

    # Create an SSH tunnel
    ssh_host = os.environ.get("SSH_%s_HOST" % instance)
    if ssh_host:
        if not existing_tunnel:
            tunnel = create_tunnel(instance)
        connection_kwargs["host"] = "localhost"
        connection_kwargs["port"] = 6543

    con = psycopg2.connect(**connection_kwargs)
    cursor = con.cursor()

    if replace:
        cleaning_request = '''DELETE FROM ''' + data["table_name"] + ''';'''
        print(C.WARNING + "Cleaning" + C.ENDC)
        cursor.execute(cleaning_request)
        print(C.OKGREEN + "[OK] Cleaning Done" + C.ENDC)

    boolean = True
    index = 0
    total_rows = len(data["rows"])
    total_nb_batchs = len(data["rows"]) // batch_size + 1
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

        inserting_request = '''INSERT INTO ''' + data["table_name"] + ''' (''' + ", ".join(
            data["columns_name"]) + ''') VALUES ''' + temp_string + ''';'''
        if final_data:
            try:
                cursor.execute(inserting_request, final_data)
            except Exception as e:
                cursor.close()
                con.close()
                if ssh_host and not existing_tunnel and tunnel:
                    tunnel.close()
                    print(C.OKBLUE + "[>>>>>] Tunnel closed" + C.ENDC)
                raise e
        index = index + 1
        percent = round(index * 100 / total_nb_batchs, 2)
        if percent < 100:
            print("\r   %s / %s (%s %%)" % (str(index), total_nb_batchs, str(percent)), end='\r')
        else:
            print("\r   %s / %s (%s %%)" % (str(index), total_nb_batchs, str(percent)))
    con.commit()

    cursor.close()
    con.close()

    if ssh_host and not existing_tunnel and tunnel:
        tunnel.close()
        print(C.OKBLUE + "[>>>>>] Tunnel closed" + C.ENDC)
    print(C.HEADER + str(total_rows) + ' rows sent to Redshift' + C.ENDC)
    print(C.OKGREEN + "[OK] Sent to redshift" + C.ENDC)
    return 0
