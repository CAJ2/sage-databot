## Database Setup

### Environments and IDs

Since the Sage database is an open database, we can easily use the same data across both the dev/staging and production environments. This excludes user and auth information for obvious reasons (and user info is not distributed externally either). In order to make this mirroring process easier, we have to adopt some rules especially concerning IDs:

1. IDs/NanoIDs must be the same across both environments. This means any automations that add new data or change any IDs need to produce the same data in both dev and prod.
2. The dev environment will only contain a subset of prod data for efficiency once prod data becomes quite large.
3. The dev environment is used for testing new features, etc. so information can be added, but that information should be considered temporary and eventually added to prod since dev will be reset to prod data occasionally, probably during larger releases.

### CockroachDB

For some things, databot needs direct access to the CockroachDB main database. Generally it is preferred to use the API, but for some cases like region geo data, it is better to update the database directly.
Follow this guide to setup a database connection locally for testing or for deployment.

1. Ensure a CockroachDB database is running in your environment (local or deployed).
2. Run the database migrations in the Sage API. Refer to the API docs for details. Databot relies on that schema for flows.
3. Execute the following in the database shell:

```sql
CREATE USER IF NOT EXISTS databot WITH PASSWORD '<password>';
GRANT ALL ON DATABASE sage TO databot WITH GRANT OPTION;
```

4. Create a Windmill resource at path `f/db_config/db_sage` with resource type `c_cockroachdb`. In the Windmill UI, go to Resources → New Resource, select `c_cockroachdb`, and fill in:

```
Path: f/db_config/db_sage
uri: cockroachdb://databot:<password>@<host>:26257/sage
ssl_rootcert: <contents of CA cert, or empty string>
ssl_cert: <contents of client cert, or empty string>
ssl_key: <contents of client key, or empty string>
```

For local development, the resource is defined in `f/db_config/db_sage.dev.resource.yaml`. The `uri` and `ssl_rootcert` values are stored as Windmill variables (`f/db_config/db_sage_uri` and `f/db_config/db_sage_rootcert`).

5. Scripts access the database via the `create_sql_engine` or `create_crdb_uri` helpers in `f/utils/db/crdb.py`:

```python
from f.utils.db.crdb import create_sql_engine

engine = create_sql_engine(resource="f/db_config/db_sage")
```

The resource path can be passed as a parameter to support different environments (dev/prod).
