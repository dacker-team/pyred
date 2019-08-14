import os
from sshtunnel import SSHTunnelForwarder


def create_tunnel(instance):
    # Create an SSH tunnel
    ssh_host = os.environ["SSH_%s_HOST" % instance]
    ssh_user = os.environ["SSH_%s_USER" % instance]
    ssh_path_private_key = os.environ["SSH_%s_PATH_PRIVATE_KEY" % instance]

    tunnel = SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username=ssh_user,
        ssh_private_key=ssh_path_private_key,
        remote_bind_address=(
            os.environ.get("RED_%s_HOST" % instance), int(os.environ.get("RED_%s_PORT" % instance))),
        local_bind_address=('localhost', 6543),  # could be any available port
    )
    return tunnel
