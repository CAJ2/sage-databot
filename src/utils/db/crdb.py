import os
from prefect_sqlalchemy import SqlAlchemyConnector
import polars as pl
from src.utils import is_production


def create_polars_uri(conn: SqlAlchemyConnector, client_certs: bool = True):
    """
    Create a connection string for the given SqlAlchemyConnector object.

    Notes for Polars read_database and read_database_uri:
    - They do not work with CRDB since the COPY TO binary format is not
      implemented: https://go.crdb.dev/issue-v/97180/v24.3
    """
    c = conn.connection_info

    sslmode = "disable"
    sslparams = ""
    if is_production() or "CRDB_SSL_CERT" in os.environ:
        if client_certs:
            sslmode = "verify-full"
            sslparams = (
                f"&sslrootcert={os.environ['CRDB_SSL_ROOTCERT']}"
                + f"&sslcert={os.environ['CRDB_SSL_CERT']}&sslkey={os.environ['CRDB_SSL_KEY']}"
            )
        else:
            sslmode = "require"
            sslparams = f"&sslrootcert={os.environ['CRDB_SSL_ROOTCERT']}"
    elif not is_production() and "CRDB_SSL_MODE" in os.environ:
        sslmode = os.environ["CRDB_SSL_MODE"]

    conn_str = f"postgresql://{c.username}:{c.password.get_secret_value()}@{c.host}:{c.port}/{c.database}?sslmode={sslmode}{sslparams}"
    return conn_str


def db_write_dataframe(
    df: pl.DataFrame, table: str, id_cols: list[str] = ["id"], conn: str = "crdb-sage"
):
    """
    Write a Polars DataFrame to a CRDB database table using the crdb-sage SqlAlchemyConnector.

    Converts struct columns to JSONB format.
    """
    crdb = SqlAlchemyConnector.load(conn)
    conn = create_polars_uri(crdb)

    # Convert struct columns to JSONB format
    for col in df.columns:
        col_info = df.get_column(col)
        if col_info.dtype == pl.Struct:
            df = df.with_columns(pl.col(col).struct.json_encode().alias(col))
        elif col_info.dtype == pl.List:
            # Better option when implemented: https://github.com/pola-rs/polars/issues/14029
            # Converting to a struct is probably better than using map_elements, 'cause Python slow
            # The actual list value will be under the JSON key with the same name as the column
            df = df.with_columns(pl.struct(pl.col(col)).struct.json_encode().alias(col))

    CHUNK_SIZE = 50_000
    if df.height > CHUNK_SIZE:
        # If the dataframe is too large, write it in chunks
        df.slice(0, CHUNK_SIZE).write_database(
            connection=conn,
            table_name=f"databot.{table}",
            if_table_exists="replace",
            engine="adbc",
        )
        for i in range(CHUNK_SIZE, df.height, CHUNK_SIZE):
            chunk = df.slice(i, CHUNK_SIZE)
            chunk.write_database(
                connection=conn,
                table_name=f"databot.{table}",
                if_table_exists="append",
                engine="adbc",
            )
    else:
        df.write_database(
            connection=conn,
            table_name=f"databot.{table}",
            if_table_exists="replace",
            engine="adbc",
        )

    for col in id_cols:
        crdb.execute(f"ALTER TABLE databot.{table} ALTER COLUMN {col} SET NOT NULL;")
    crdb.execute(
        f"ALTER TABLE databot.{table} ALTER PRIMARY KEY USING COLUMNS ({','.join(id_cols)});"
    )
