import psycopg2 as psycopg2
from psycopg2.extras import RealDictCursor

from . import redshift_credentials


def execute_query(instance, query):
    connection_kwargs = redshift_credentials.credential(instance)
    con = psycopg2.connect(**connection_kwargs, cursor_factory=RealDictCursor)
    cursor = con.cursor()
    cursor.execute(query)
    con.commit()
    try:
        result = cursor.fetchall()
    except psycopg2.ProgrammingError:
        result = None
    cursor.close()
    con.close()
    return result