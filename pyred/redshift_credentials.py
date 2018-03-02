import os


def credential(instance):
    alias = "RED_" + instance
    connection_kwargs = {
        'database': os.environ[alias + "_DATABASE"],
        'user': os.environ[alias + "_USERNAME"],
        'host': os.environ[alias + "_HOST"],
        'port': os.environ[alias + "_PORT"],
        'password': os.environ[alias + "_PASSWORD"],
    }
    return connection_kwargs
