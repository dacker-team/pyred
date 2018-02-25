pyred
=====

A python package to easily send data to Amazon Redshift
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1) Installation
'''''''''''''''

Open a terminal and install pyred package
                                                   
::

    pip install pyred


2) Use
''''''

1) Be sure that you have set environment variables with Redshift credentials like this:


::

    export RED_{INSTANCE}_DATABASE=""
    export RED_{INSTANCE}_USERNAME=""
    export RED_{INSTANCE}_HOST=""
    export RED_{INSTANCE}_PORT=""
    export RED_{INSTANCE}_PASSWORD=""

2) Be also sure that your IP address is authorized for the redshift cluster/instance.

3) Prepare your data like that:


.. code:: python

    data = {
            "table_name"    : 'name_of_the_redshift_schema' + '.' + 'name_of_the_redshift_table'
            "columns_name"  : [first_column_name,second_column_name,...,last_column_name],
            "rows"      : [[first_raw_value,second_raw_value,...,last_raw_value],...]
        }

4) Send your data (use the same {INSTANCE} parameter as environment variables):


.. code:: python

    import pyred
    pyred.send_to_redshift(instance, data, replace=True, batch_size=1000, types=None, primary_key=(), create_boolean=False)

-  replace (default=True) argument allows you to replace or append data
   in the table
-  batch\_size (default=1000) argument also exists to send data in
   batchs
- types, primary_key and create_boolean are explained below

3) First Example
''''''''''''''''

You have a table called dog in you animal scheme. This table has two columns : 'name' and 'size'.
You want to add two dogs (= two rows) : Pif which is big and Milou which is small.
*data* will be like that:

.. code:: python

    import pyred
    data = {
            "table_name"    : 'animal.dog'
            "columns_name"  : ['name','size'],
            "rows"      : [['Pif','big'], ['Milou','small']]
        }
    pyred.send_to_redshift({INSTANCE},data)

4) Function *create_table*
''''''''''''''''''''''''''
pyred has a *create_table* function with this signature:

.. code:: python

    import pyred
    pyred.create_table({INSTANCE}, data, primary_key=(), types=None)

This function is automatically called in the *send_to_redshift* function if the table is not created. You can also call it with the "create_boolean" parameter of the *send_to_reshift* function or by setting "primary_key" or "types" parameters.

-  primary_key : if you have 3 columns (ie: columns_name=[a,b,c]) and you want to set b as primary key, set primary_key=(b)
-  types: *create_table* function guesses types of each column. But you can set a "types" argument. It is a dictionary like {'b': 'VARCHAR(12)'} or  {'b': 'INTEGER NOT NULL'} to set types of b column.
