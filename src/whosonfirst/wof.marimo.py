import marimo

__generated_with = "0.12.9"
app = marimo.App(width="medium")


@app.cell
def _():
    import polars as pl
    import bz2
    import os
    country = 'SE'
    download_url = f'https://data.geocode.earth/wof/dist/sqlite/whosonfirst-data-admin-{country.lower()}-latest.db.bz2'
    return bz2, country, download_url, os, pl


@app.cell
def _(bz2, country, download_url, os):
    filename = os.path.basename(download_url)
    basepath = os.path.join(os.getcwd(), 'data', 'whosonfirst')
    try:
        os.stat(os.path.join(basepath, country.lower()))
    except:
        os.mkdir(os.path.join(basepath, country.lower()))
    filepath = os.path.join(basepath, country.lower(), filename)

    if not os.path.exists(filepath):
        print(f"Downloading {download_url} to {filepath}")
        os.system(f"curl -o {filepath} {download_url}")
    else:
        print(f"File {filepath} already exists, skipping download.")

    # Extract the bzip2 file using bz2 python module
    print(f"Extracting {filepath}")
    with bz2.BZ2File(filepath, 'rb') as f_in:
        with open(filepath[:-4], 'wb') as f_out:
            f_out.write(f_in.read())
    print(f"Extracted {filepath} to {filepath[:-4]}")
    return basepath, f_in, f_out, filename, filepath


@app.cell
def _(filepath, pl):
    names_df = pl.read_database_uri(
        query="SELECT id, placetype, language, script, region, name FROM names WHERE privateuse = 'preferred'",
        uri=f"sqlite:///{filepath[:-4]}",
        engine="connectorx"
    )
    print("Total names:", names_df.height)
    names_df.describe()

    names_df = names_df.with_columns(pl.concat_str([pl.col('language'), pl.col('script'), pl.col('region')], separator='-', ignore_nulls=True).alias('lang'))
    lang_df = names_df.pivot(
        index=['id', 'placetype'],
        on=['lang'],
        values=['name'],
        aggregate_function='first'
    )
    lang_df = lang_df.with_columns(pl.struct(pl.all().exclude(['id', 'placetype'])).alias('name'))
    lang_df = lang_df.drop(pl.all().exclude(['id', 'placetype', 'name']))
    lang_df = lang_df.with_columns(pl.col('name').struct.json_encode())
    print(lang_df.head())

    geojson_df = pl.read_database_uri(
        query="SELECT id, body FROM geojson WHERE is_alt = 0",
        uri=f"sqlite:///{filepath[:-4]}",
        engine="connectorx"
    )
    print("Total geojson:", geojson_df.height)
    geojson_df.describe()
    geojson_df = geojson_df.with_columns(
        pl.col('body').str.json_path_match("$.geometry").alias('geo'),
        pl.col('body').str.json_path_match("$.properties").alias('properties')
    )
    geojson_df = geojson_df.drop('body')
    combined_df = lang_df.join(geojson_df, on='id', how='inner')
    print(combined_df.head(10))
    combined_df.describe()
    return combined_df, geojson_df, lang_df, names_df


@app.cell
def _():
    import marimo as mo
    return (mo,)


if __name__ == "__main__":
    app.run()
