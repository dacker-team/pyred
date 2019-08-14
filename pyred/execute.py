import os
import psycopg2 as psycopg2
import sshtunnel
from psycopg2.extras import RealDictCursor
from sshtunnel import SSHTunnelForwarder

from pyred.tools.print_colors import C
from pyred.tunnel import create_tunnel
from . import redshift_credentials


def execute_query(instance, query, existing_tunnel=None):
    connection_kwargs = redshift_credentials.credential(instance)

    # Create an SSH tunnel
    ssh_host = os.environ.get("SSH_%s_HOST" % instance)
    if ssh_host:
        if not existing_tunnel:
            tunnel = create_tunnel(instance)
            # Start the tunnel
            try:
                tunnel.start()
                print("Tunnel opened!")
            except sshtunnel.HandlerSSHTunnelForwarderError:
                pass

        connection_kwargs["host"] = "localhost"
        connection_kwargs["port"] = 6543

    con = psycopg2.connect(**connection_kwargs, cursor_factory=RealDictCursor)

    cursor = con.cursor()
    try:
        cursor.execute(query)
    except Exception as e:
        cursor.close()
        con.close()
        if ssh_host and not existing_tunnel:
            tunnel.close()
            print("Tunnel closed!")
        raise e
    con.commit()
    try:
        result = cursor.fetchall()
    except psycopg2.ProgrammingError:
        result = None
    cursor.close()
    con.close()

    if ssh_host and not existing_tunnel:
        tunnel.close()
        print(C.BOLD + "Tunnel closed!" + C.ENDC)

    return [dict(r) for r in result] if result else result
