# requirements: project

from collections.abc import Iterator

import polars as pl
from sqlalchemy import Engine


def export_table(
    crdb: Engine,
    table: str,
    cols: str = "*",
    schema: dict[str, pl.DataType] | None = None,
    batch_size: int = 5000,
) -> Iterator[pl.DataFrame]:
    """
    Export a table from the database to a Polars DataFrame.
    """
    df_iter = pl.read_database(
        f"SELECT {cols} FROM {table}",
        connection=crdb,
        schema_overrides=schema,
        iter_batches=True,
        batch_size=batch_size,
    )
    return df_iter
