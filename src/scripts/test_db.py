from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy_cockroachdb import run_transaction
from prefect_sqlalchemy import SqlAlchemyConnector
from dotenv import load_dotenv

from src.utils.db.crdb import create_polars_uri

if __name__ == "__main__":
    load_dotenv()
    # Load the SqlAlchemyConnector
    crdb = SqlAlchemyConnector.load("crdb-sage")

    # Create the Polars URI
    uri = create_polars_uri(crdb)
    uri = uri.replace("postgresql://", "cockroachdb://")

    crdb.execute("SELECT 1")
    try:
        engine = create_engine(uri)
    except Exception as e:
        print(f"Failed to create engine: {e}")
        raise
    Session = sessionmaker(bind=engine)
    session = Session()
    run_transaction(
        session,
        lambda s: s.execute(text("SELECT 1")),
    )

    print("Connection successful!")
