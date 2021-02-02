import pandas as pd


def schema_compare_tool(self, schema_ref, new_schema_test):
    # same table uploaded ?
    query1 = """select "table_name" from information_schema.tables where table_schema='%s'""" % schema_ref
    query2 = """select "table_name" from information_schema.tables where table_schema='%s'""" % new_schema_test
    table_schema_ref = pd.DataFrame(self.execute_query(query1, apply_special_env=False))
    table_new_schema_test = pd.DataFrame(self.execute_query(query2, apply_special_env=False))
    if table_schema_ref['table_name'].equals(table_new_schema_test['table_name']):
        print("Same tables in both schemas")
    if not table_schema_ref['table_name'].equals(table_new_schema_test['table_name']):
        print("Table not loaded in schema %s:" % new_schema_test)
        df = table_schema_ref.merge(table_new_schema_test, how='outer', indicator=True).loc[
            lambda x: x['_merge'] == 'left_only']
        if not df.empty:
            print(df)
        print("")

    # for each data loaded : same columns loaded ? if loaded same type? if same type : equals ?
    for table in table_new_schema_test['table_name']:
        print("")
        print('\033[1m' + table + '\033[0m')
        query1 = """select "column_name" from information_schema.columns where table_schema='%s' and table_name='%s'""" \
                 % (schema_ref, table)
        query2 = """select "column_name" from information_schema.columns where table_schema='%s' and table_name='%s'""" \
                 % (new_schema_test, table)
        columns_table_1 = pd.DataFrame(self.execute_query(query1, apply_special_env=False))
        columns_table_2 = pd.DataFrame(self.execute_query(query2, apply_special_env=False))
        if columns_table_1['column_name'].equals(columns_table_2['column_name']):
            print("     Same columns in both tables")
        if not columns_table_1['column_name'].equals(columns_table_2['column_name']):
            print("     columns not loaded :")
            df = columns_table_1.merge(columns_table_2, how='outer', indicator=True).loc[
                lambda x: x['_merge'] == 'left_only']
            if not df.empty:
                print(df)
            print("     columns loaded in addition:")
            df = columns_table_1.merge(columns_table_2, how='outer', indicator=True).loc[
                lambda x: x['_merge'] == 'right_only']
            if not df.empty:
                print(df)
        for column in columns_table_1.merge(columns_table_2, how='inner', indicator=True)['column_name']:
            print("         ", column)
            query1 = """select "udt_name" from information_schema.columns where table_schema='%s' and table_name='%s' and column_name='%s'""" \
                     % (schema_ref, table, column)
            query2 = """select "udt_name" from information_schema.columns where table_schema='%s' and table_name='%s' and column_name='%s'""" \
                     % (new_schema_test, table, column)
            type1 = str(self.execute_query(query1, apply_special_env=False))
            type2 = str(self.execute_query(query2, apply_special_env=False))
            if type1 != type2:
                print("         ", "type from schema 1 :", type1, "// type from schema 2:", type2)
            if type1 == type2:
                query1 = """select '%s' from %s.%s""" \
                         % (column, schema_ref, table)
                query2 = """select '%s' from %s.%s""" \
                         % (column, new_schema_test, table)
                value1 = pd.DataFrame(self.execute_query(query1, apply_special_env=False))
                value2 = pd.DataFrame(self.execute_query(query2, apply_special_env=False))
                print("         ", value2.equals(value1))
