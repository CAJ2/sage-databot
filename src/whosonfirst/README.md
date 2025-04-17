## Who's On First

The Who's On First data is loaded in country-by-country to avoid huge downloads and extracts and since we don't need to support every country.

The flow follows these steps:

- Download the country SQLite database from Geocode Earth
- Import translations and the GeoJSON sources and join into one Polars DataFrame
- Write the DataFrame to CockroachDB in a table in the databot schema
- Upsert the table's contents to the public.regions table for access by the API

Important notes:

- The actual geometry data for each region can be a Point, a Polygon, or a MultiPolygon. The regions table only supports MultiPolygon in the geo field, so Polygons are converted to MultiPolygon, and Points are simply not written (geo is null). The point location is still available in the properties JSON.
- All available language translations are stored in the names column. This can probably be trimmed down in the future to a supported language list.
