import os
import psycopg2 as psycopg2
import sshtunnel
from psycopg2.extras import RealDictCursor
from sshtunnel import SSHTunnelForwarder

from . import redshift_credentials


def execute_query(instance, query):
    connection_kwargs = redshift_credentials.credential(instance)

    # Create an SSH tunnel
    ssh_host = os.environ.get("SSH_%s_HOST" % instance)
    ssh_user = os.environ.get("SSH_%s_USER" % instance)
    ssh_path_private_key = os.environ.get("SSH_%s_PATH_PRIVATE_KEY" % instance)

    if ssh_host:
        tunnel = SSHTunnelForwarder(
            (ssh_host, 22),
            ssh_username=ssh_user,
            ssh_private_key=ssh_path_private_key,
            remote_bind_address=(
            os.environ.get("RED_%s_HOST" % instance), int(os.environ.get("RED_%s_PORT" % instance))),
            local_bind_address=('localhost', 6543),  # could be any available port
        )
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
    cursor.execute(query)
    con.commit()
    try:
        result = cursor.fetchall()
    except psycopg2.ProgrammingError:
        result = None
    cursor.close()
    con.close()

    if ssh_host:
        tunnel.close()
        print("Tunnel closed!")

    return result
