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

4. Run/Access the Prefect UI. If running locally, you can use `prefect server start` and click the UI link. Click on "Blocks", then the plus button, search for "sqlalchemy" and click "Create".

```
Block Name: crdb-sage
Driver: string -> cockroachdb+psycopg2
Database: sage
Username: databot
Password: <password>
Host: Can leave blank if local or address of Docker/K8s container
Port: 26257
```
Click "Create"

5. Now you should be able to access the credentials with:

```python
from prefect_sqlalchemy import SqlAlchemyConnector

crdb = SqlAlchemyConnector.load("crdb-sage")
# OR
with SqlAlchemyConnector.load("crdb-sage") as database:
```
