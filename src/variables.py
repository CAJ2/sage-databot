from prefect.variables import Variable
import os


def set_variable(name: str, value: str, tags: list = []):
    """
    Sets a variable in the Prefect backend only if it does not already exist.
    Args:
        name (str): The name of the variable.
        value (str): The value of the variable.
        tags (list): The tags for the variable.
    """
    if Variable.get(name) is None:
        Variable.set(name, value, tags=tags)


def default_variables(env: str):
    """
    Sets the default variables for the project. It will only set the variables if they do not already exist
    to avoid overwriting changed variables.
    This is useful for setting up a local environment.
    It also serves as a documentation tool for the variables used in the project.
    Args:
        env (str): The environment to set the variables for. Set to "production" for production defaults.
    """

    if env == "production":
        # general
        set_variable("cache_dir", "/data/cache", tags=["general", "paths"])

        # whosonfirst
        set_variable(
            "whosonfirst_basepath",
            "/data/cache/whosonfirst",
            tags=["whosonfirst", "paths"],
        )
        set_variable("whosonfirst_countries", ["se"], tags=["whosonfirst", "regions"])

        # osm
        set_variable("osm_basepath", "/data/cache/osm", tags=["osm", "paths"])
        set_variable("osm_countries", ["sweden"], tags=["osm", "countries"])

    else:
        # general
        set_variable("cache_dir", "data", tags=["general", "paths"])

        # whosonfirst
        set_variable(
            "whosonfirst_basepath", "data/whosonfirst", tags=["whosonfirst", "paths"]
        )
        set_variable("whosonfirst_countries", ["se"], tags=["whosonfirst", "regions"])

        # osm
        set_variable("osm_basepath", "data/osm", tags=["osm", "paths"])
        set_variable("osm_countries", ["sweden"], tags=["osm", "countries"])


if __name__ == "__main__":
    env = None
    if "DATABOT_ENV" in os.environ:
        env = os.environ["DATABOT_ENV"]
    default_variables(env)
