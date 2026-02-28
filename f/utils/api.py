import os
import wmill
import httpx
import json
from http import cookies
from urllib.parse import unquote

from f.graphql.api_client.client import Client


def api_connect(extra_headers: dict[str, str] | None = None):
    """
    Connects to the API and returns the client and user.

    Reads credentials from ``f/api_config/api_sage_key`` (JSON object).
    If the object contains an ``apikey`` field, it is sent as the
    ``x-api-key`` header and the current user is fetched via the
    ``GetCurrentUser`` GraphQL query.  Otherwise, ``email``/``password`` are
    used to sign in and the authenticated user object is returned.

    Args:
        extra_headers: Additional headers to include on every request.
                       For tests, pass {"x-env": "test"} to enable test mode.
    """
    api_url = wmill.get_variable("f/api_config/api_sage_url")
    api_key_raw = wmill.get_variable("f/api_config/api_sage_key")

    headers = {}
    if extra_headers:
        headers.update(extra_headers)
    # Auto-detect test workspace and add test header
    workspace = os.environ.get("WM_WORKSPACE", "")
    if workspace.startswith("wm-fork-test") and "x-env" not in headers:
        headers["x-env"] = "test"

    creds = json.loads(api_key_raw)

    if "apikey" in creds:
        # API key auth — fetch current user via GetCurrentUser GraphQL query
        headers["x-api-key"] = creds["apikey"]
        httpx_client = httpx.Client(base_url=api_url + "/graphql", headers=headers)
        client = Client(http_client=httpx_client)
        result = client.get_current_user()
        if not result.me:
            raise ValueError("Failed to fetch current user: no me in response")
        me = result.me
        user = {"id": me.id, "email": me.email, "name": me.name, "username": me.username}
    else:
        # Email/password auth
        r = httpx.post(
            api_url + "/auth/sign-in/email",
            json={"email": creds["email"], "password": creds["password"]},
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
        httpx_client = httpx.Client(base_url=api_url + "/graphql", cookies=cx, headers=headers)
        user = body["user"]
        client = Client(http_client=httpx_client)
    # Test the API connection
    try:
        client.get_root_category()
    except Exception as e:
        raise ValueError(f"Failed to connect to the GraphQL API: {e}")
    return (client, user)


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
