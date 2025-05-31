from prefect_sqlalchemy import SqlAlchemyConnector
from prefect.variables import Variable
from prefect.blocks.system import Secret
import httpx

from src.graphql.api_client.client import Client


def api_connect(crdb: SqlAlchemyConnector = None):
    """
    Connects to the API and returns the client and user.
    """
    if crdb is None:
        crdb = SqlAlchemyConnector.load("crdb-sage")
    # Load the databot
    user = crdb.fetch_one(
        "SELECT * FROM public.users WHERE email = 'databot@sageleaf.app'",
    )
    if len(user) == 0:
        raise ValueError("No databot user found in the database.")
    # Create an API client
    api_url = Variable.get("api_url")
    r = httpx.post(
        api_url + "/auth/sign-in/email",
        json={
            "email": user[3],
            "password": Secret.load("databot-password").get(),
        },
    )
    if r.status_code != 200 or len(r.cookies) == 0:
        raise ValueError(f"Failed to sign in to the API: {r.status_code} {r.text}")
    cookies = httpx.Cookies()
    cookies.set("sage.session_token", r.cookies.get("sage.session_token"))
    cookies.set("sage.session_data", r.cookies.get("sage.session_data"))
    httpx_client = httpx.Client(base_url=api_url + "/graphql", cookies=cookies)
    client = Client(http_client=httpx_client)
    # Test the API connection
    try:
        client.get_root_category()
    except Exception as e:
        raise ValueError(f"Failed to connect to the GraphQL API: {e}")
    return (client, user)
