import json
from typing import Any, Iterator
import wmill
import os
import stat
import polars as pl
from urllib.parse import urlparse, urlencode, parse_qs
from sqlalchemy import create_engine, Engine, text, JSON
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.types import TypeDecorator
from dataclasses import asdict


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
    c: dict[str, Any] | None = wmill.get_resource(resource)
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


class JSONData(TypeDecorator[Any]):
    impl = JSON

    def __init__(self, dataclass, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataclass = dataclass

    def process_bind_param(self, value, dialect):
        if value is not None:
            return json.dumps(asdict(value))
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
            value = self.dataclass(**value)
        return value


def export_table_by_ids(
    crdb: Engine,
    table: str,
    ids: list[str],
    cols: str = "*",
    schema: dict[str, Any] | None = None,
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
    df: pl.DataFrame | pl.LazyFrame,
    table: str,
    id_cols: list[str] = ["id"],
    append: bool = False,
    resource: str = "f/db_config/db_sage",
    chunk_size: int = 5_000,
):
    """
    Write a Polars DataFrame or LazyFrame to a CRDB database table.
    Handles large DataFrames by writing/materializing in batches.

    Converts struct columns to JSONB format.
    """
    crdb = create_sql_engine(resource=resource)

    print(f"Writing to table databot.{table} with chunk size {chunk_size}...")
    df = df.lazy()
    # Convert struct columns to JSONB format
    schema = df.collect_schema()
    print(f"Schema has {len(schema)} columns")
    for col in schema.names():
        if schema[col] == pl.Struct:
            df = df.with_columns(pl.col(col).struct.json_encode().alias(col))
        elif schema[col] == pl.List:
            # Better option when implemented: https://github.com/pola-rs/polars/issues/14029
            # Converting to a struct is probably better than using map_elements, 'cause Python slow
            # The actual list value will be under the JSON key with the same name as the column
            df = df.with_columns(pl.struct(pl.col(col)).struct.json_encode().alias(col))

    first = not append
    total_count = 0
    opt = pl.QueryOptFlags(
        predicate_pushdown=True,
        projection_pushdown=True,
        simplify_expression=True,
        slice_pushdown=True,
        comm_subplan_elim=True,
        comm_subexpr_elim=True,
        cluster_with_columns=True,
        collapse_joins=True,
        check_order_observe=True,
        fast_projection=True,
    )
    print("Starting to write batches...")
    print(f"Query plan: \n{df.explain(optimizations=opt)}")
    for next_df in df.collect_batches(
        chunk_size=chunk_size,
        maintain_order=False,
        optimizations=opt,
    ):
        next_df.write_database(
            connection=crdb,
            table_name=f"databot.{table}",
            if_table_exists="replace" if first else "append",
        )
        if first:
            for col in id_cols:
                with crdb.begin() as conn:
                    conn.execute(
                        text(
                            f"ALTER TABLE databot.{table} ALTER COLUMN {col} SET NOT NULL"
                        )
                    )
            with crdb.begin() as conn:
                conn.execute(
                    text(
                        f"ALTER TABLE databot.{table} ALTER PRIMARY KEY USING COLUMNS ({','.join(id_cols)})"
                    )
                )
            print(f"Wrote initial {next_df.height} rows to databot.{table}")
        else:
            print(f"Appended {next_df.height} rows to databot.{table}")
        first = False
        total_count += next_df.height
    print(f"Finished writing {total_count} rows to databot.{table}")
