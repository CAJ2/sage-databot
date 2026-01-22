import json
from typing import Iterator
import wmill
import os
import stat
import polars as pl
from urllib.parse import urlparse, urlencode, parse_qs
from sqlalchemy import create_engine, Engine, text
from sqlalchemy.orm import DeclarativeBase


def _check_or_create_cert_file(content: str, file_name: str):
    if os.path.isfile(file_name):
        return
    with open(file_name, "w") as fp:
        fp.write(content)
    os.chmod(file_name, stat.S_IREAD)


def create_crdb_uri(resource="f/db_config/db_sage") -> str:
    """
    Creates a connection string for CockroachDB

    If TLS/SSL is used, the certs/key need to be extracted from the resource,
    stored as temporary files, and injected into the connection string.
    """
    c: dict | None = wmill.get_resource(resource)
    if c is None:
        raise ValueError(f"Invalid resource '{resource}'")

    uri = urlparse(c["uri"])
    query_params: dict[str, list[str]] = parse_qs(uri.query)
    sslmode: str = "disable"

    file_name_prefix = resource.replace("/", "_")
    if len(c["ssl_rootcert"]) > 0:
        file_name = file_name_prefix + "_ssl_rootcert.crt"
        _check_or_create_cert_file(c["ssl_rootcert"], file_name)
        query_params["sslrootcert"] = [file_name]
        sslmode = "verify-ca"
    if len(c["ssl_cert"]) > 0:
        file_name = file_name_prefix + "_ssl_cert.crt"
        _check_or_create_cert_file(c["ssl_cert"], file_name)
        query_params["sslcert"] = [file_name]
        sslmode = "verify-full"
    if len(c["ssl_key"]) > 0:
        file_name = file_name_prefix + "_ssl_key.key"
        _check_or_create_cert_file(c["ssl_key"], file_name)
        query_params["sslkey"] = [file_name]
    query_params["sslmode"] = [sslmode]
    uri = uri._replace(query=urlencode(query_params, doseq=True))
    return uri.geturl()


def create_polars_uri(resource="f/db_config/db_sage") -> str:
    """
    Create a connection string for use with Polars.

    Notes for Polars read_database and read_database_uri:
    - They do not work with CRDB since the COPY TO binary format is not
      implemented: https://go.crdb.dev/issue-v/97180/v24.3
    """
    return create_crdb_uri(resource=resource).replace("cockroachdb", "postgresql", 1)


_sql_engine = None


def create_sql_engine(resource="f/db_config/db_sage") -> Engine:
    global _sql_engine
    if _sql_engine is None:
        _sql_engine = create_engine(
            create_crdb_uri(resource=resource).replace(
                "cockroachdb", "cockroachdb+psycopg", 1
            ),
            json_serializer=lambda obj: json.dumps(obj, ensure_ascii=False),
        )
    return _sql_engine


class Base(DeclarativeBase):
    pass


def export_table_by_ids(
    crdb: Engine,
    table: str,
    ids: list[str],
    cols: str = "*",
    schema: dict | None = None,
    batch_size: int = 5000,
) -> Iterator[pl.DataFrame]:
    """
    Export a selection of rows keyed by primary id from the database to a Polars DataFrame.
    """
    df_iter = pl.read_database(
        f"SELECT {cols} FROM {table} WHERE id IN ('{"','".join(ids)}')",
        connection=crdb,
        schema_overrides=schema,
        iter_batches=True,
        batch_size=batch_size,
    )
    return df_iter


def db_write_dataframe(
    df: pl.DataFrame,
    table: str,
    id_cols: list[str] = ["id"],
    resource: str = "f/db_config/db_sage",
):
    """
    Write a Polars DataFrame to a CRDB database table using the crdb-sage SqlAlchemyConnector.

    Converts struct columns to JSONB format.
    """
    conn = create_polars_uri(resource=resource)
    crdb = create_sql_engine(resource=resource)

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
        with crdb.begin() as conn:
            conn.execute(
                text(f"ALTER TABLE databot.{table} ALTER COLUMN {col} SET NOT NULL")
            )
    with crdb.begin() as conn:
        conn.execute(
            text(
                f"ALTER TABLE databot.{table} ALTER PRIMARY KEY USING COLUMNS ({','.join(id_cols)})"
            )
        )
