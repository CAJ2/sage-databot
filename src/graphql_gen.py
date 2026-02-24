from pathlib import Path
from urllib.request import urlretrieve

from ariadne_codegen import main


def import_gql_from_repo():
    """
    Downloads the latest schema.gql from the Sage app repo and saves it to f/graphql/schema.gql
    """
    repo = "https://raw.githubusercontent.com/CAJ2/sage-app/main/"
    url = repo + "apps/api/schema/schema.gql"
    root = Path(__file__).parents[1]
    output_path = root / "f/graphql/schema.gql"
    urlretrieve(url, output_path)

if __name__ == "__main__":
    import_gql_from_repo()
    main.main()
