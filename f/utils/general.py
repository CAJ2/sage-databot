import os
import re

import wmill
import yaml
from google.oauth2 import service_account
from pydantic_ai.models import Model
from pydantic_ai.models.google import GoogleModel
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.gateway import gateway_provider, normalize_gateway_provider
from pydantic_ai.providers.google import GoogleProvider
from pydantic_ai.providers.openai import OpenAIProvider


def is_production() -> bool:
    """
    Check if the environment is production.
    """
    env = os.environ.get("WM_WORKSPACE")
    return env == "sage-prod"


def find_path_or_default(cfg: dict) -> str:
    """
    Looks for a config value based on:
    1. The current script path (from WM_JOB_PATH)
    2. The current flow path (from WM_FLOW_PATH)
    3. A "default" value in the config
    """
    script_path = os.environ.get("WM_JOB_PATH")
    flow_path = os.environ.get("WM_FLOW_PATH")
    if script_path and script_path in cfg:
        return cfg[script_path]
    elif flow_path and flow_path in cfg:
        return cfg[flow_path]
    elif "default" in cfg:
        return cfg["default"]
    else:
        raise ValueError(
            f"No path found for script {script_path} and no default provided."
        )


def llm_agent() -> Model:
    models = wmill.get_variable("llm_models")
    model_dict = yaml.full_load(models)
    model_name = find_path_or_default(model_dict)
    if model_name.startswith("gateway/"):
        # Pydantic AI Gateway
        provider_name = normalize_gateway_provider(model_name)
        provider = gateway_provider(
            provider_name, api_key=wmill.get_variable("llm_pydantic_gateway_key")
        )
        if isinstance(provider, GoogleProvider):
            model = GoogleModel(model_name.split(":")[1], provider=provider)
            return model
        else:
            raise ValueError(
                f"Gateway provider {provider_name} is not supported for llm_agent."
            )
    try:
        ollama = wmill.get_variable("llm_ollama_api")
    except Exception:
        ollama = None
    if ollama:
        provider = OpenAIProvider(base_url=ollama)
        llm = OpenAIChatModel(model_name=model_name, provider=provider)
        return llm
    creds = service_account.Credentials.from_service_account_info(
        wmill.get_variable("api_gcp_key"),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    provider = GoogleProvider(credentials=creds)
    llm = GoogleModel(model_name, provider=provider)
    return llm


def slugify(s):
    s = s.lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_-]+", "-", s)
    s = re.sub(r"^-+|-+$", "", s)
    return s
