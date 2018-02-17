pyred
=====

A python package to easily send data to Amazon Redshift
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1) Installation
'''''''''''''''

1) Open a terminal and clone the report where you want
                                                   
::

    git clone https://github.com/pflucet/pyred

2) Go to the pyred folder you just cloned

::

    cd pyred/

3) Install package

::

    pip install .

*or*

::

    python setup.py install

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
            #Table must already exist
            "columns_name"  : [first_column_name,second_column_name,...,last_column_name],
            "rows"      : [[first_raw_value,second_raw_value,...,last_raw_value],...]
        }

4) Send your data (use the same {INSTANCE} parameter as environment variables):
                

.. code:: python

    import pyred
    pyred.send_to_redshift({INSTANCE},data,replace=True)

-  replace (default=True) argument allows you to replace or append data
   in the table
-  batch\_size (default=1000) argument also exists to send data in
   batchs

3) Example
''''''
You have a table called dog in you animal scheme. This table has two columns : 'name' and 'size'.
You want to add two dogs (= two rows) : Pif which is big and Milou which is small.
*data* will be like that:

.. code:: python

    data = {
            "table_name"    : 'animal.dog'
            "columns_name"  : ['name','size'],
            "rows"      : [['Pif','big'], ['Milou','small']]
        }
