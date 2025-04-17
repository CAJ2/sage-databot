import os
from prefect_sqlalchemy import SqlAlchemyConnector
from src.utils import is_production

def create_polars_uri(conn: SqlAlchemyConnector):
    """
    Create a connection string for the given SqlAlchemyConnector object.
    """
    c = conn.connection_info

    sslmode = "disable"
    sslparams = ""
    if is_production() or "CRDB_SSL_CERT" in os.environ:
        sslmode = "verify-full"
        sslparams = f"&sslrootcert={os.environ['CRDB_SSL_ROOTCERT']}" + \
            f"&sslcert={os.environ['CRDB_SSL_CERT']}&sslkey={os.environ['CRDB_SSL_KEY']}"
    elif not is_production() and "CRDB_SSL_MODE" in os.environ:
        sslmode = os.environ["CRDB_SSL_MODE"]

    conn_str = f"postgresql://{c.username}:{c.password}@{c.host}:{c.port}/{c.database}?sslmode={sslmode}{sslparams}"
    return conn_str