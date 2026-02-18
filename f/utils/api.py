import wmill
import httpx
import json
from http import cookies
from urllib.parse import unquote

from f.graphql.api_client.client import Client


def api_connect():
    """
    Connects to the API and returns the client and user.
    """
    # Create an API client
    api_url = wmill.get_variable("f/api_config/api_sage_url")
    api_key = json.loads(wmill.get_variable("f/api_config/api_sage_key"))
    r = httpx.post(
        api_url + "/auth/sign-in/email",
        json={
            "email": api_key["email"],
            "password": api_key["password"],
        },
        headers={
            "Content-Type": "application/json",
            "Origin": api_url,
            "Host": api_url.replace("https://", ""),
        },
    )
    c = extract_cookies(r)
    if r.status_code != 200 or len(c.keys()) == 0:
        raise ValueError(f"Failed to sign in to the API: {r.status_code} {r.text}")
    cx = httpx.Cookies()
    for k in c.keys():
        cx.set(k, c[k].value)

    body = r.json()
    if "user" not in body:
        raise ValueError("Failed to sign in to the API: user key not found")

    httpx_client = httpx.Client(base_url=api_url + "/graphql", cookies=cx)
    client = Client(http_client=httpx_client)
    # Test the API connection
    try:
        client.get_root_category()
    except Exception as e:
        raise ValueError(f"Failed to connect to the GraphQL API: {e}")
    return (client, body["user"])


def extract_cookies(r: httpx.Response) -> cookies.SimpleCookie:
    """
    Extracts cookies from the response and returns them as a dictionary.
    """
    cs = cookies.SimpleCookie()
    for c in r.headers.get_list("Set-Cookie"):
        c_parts = c.split(";")
        c_key = c_parts[0].split("=")[0]
        cs[c_key] = c_parts[0].split("=")[1]

    return cs
