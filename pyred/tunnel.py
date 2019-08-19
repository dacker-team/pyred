import os
from sshtunnel import SSHTunnelForwarder, HandlerSSHTunnelForwarderError

from pyred.tools.print_colors import C


def create_tunnel(instance):
    # Create an SSH tunnel
    ssh_host = os.environ["SSH_%s_HOST" % instance]
    ssh_user = os.environ["SSH_%s_USER" % instance]
    try:
        ssh_path_private_key = os.environ["SSH_%s_PATH_PRIVATE_KEY" % instance]
    except KeyError:
        ssh_private_key = os.environ["SSH_%s_PRIVATE_KEY" % instance]
        ssh_path_private_key = 'ssh_path_private_key'
        with open(ssh_path_private_key, 'w') as w:
            w.write(ssh_private_key)
            w.close()
    try:
        tunnel = SSHTunnelForwarder(
            (ssh_host, 22),
            ssh_username=ssh_user,
            ssh_private_key=ssh_path_private_key,
            remote_bind_address=(
                os.environ.get("RED_%s_HOST" % instance), int(os.environ.get("RED_%s_PORT" % instance))),
            local_bind_address=('localhost', 6543),  # could be any available port
        )
        tunnel.start()
        print(C.OKBLUE + 'Tunnel opened and started [<<<<<]' + C.ENDC)
        return tunnel
    except HandlerSSHTunnelForwarderError:
        print(C.OKBLUE + 'HandlerSSHTunnelForwarderError' + ' [<<<<<]' + C.ENDC)
        return None
