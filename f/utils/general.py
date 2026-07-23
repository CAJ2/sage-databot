import json
import os
import re
from typing import Any

import logfire
import wmill
import yaml
from google.genai.types import ThinkingLevel
from google.oauth2 import service_account
from pydantic_ai.models import Model
from pydantic_ai.models.google import GoogleModel, GoogleModelSettings
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.models.openrouter import OpenRouterModel
from pydantic_ai.providers.gateway import gateway_provider, normalize_gateway_provider
from pydantic_ai.providers.google import GoogleProvider
from pydantic_ai.providers.openai import OpenAIProvider
from pydantic_ai.providers.openrouter import OpenRouterProvider


def is_production() -> bool:
    """
    Check if the environment is production.
    """
    env = os.environ.get("WM_WORKSPACE")
    return env == "sage-prod"


def environment() -> str:
    """
    Get the environment name (prod or dev).
    """
    env = os.environ.get("WM_WORKSPACE")
    if env == "sage-prod":
        return "prod"
    elif env == "localdev":
        return "local"
    else:
        return "dev"


def find_path_or_default(cfg: dict[str, Any]) -> dict[str, Any]:
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


def llm_agent(preset: str | None = None) -> Model:
    # Configure logfire
    logfire_token = wmill.get_variable("f/api_config/llm_logfire_token")
    if logfire_token:
        logfire.configure(token=logfire_token, environment=environment())
        logfire.instrument_pydantic_ai()

    models = wmill.get_variable("f/api_config/llm_models")
    model_dict = yaml.full_load(models)
    if preset is not None:
        if preset not in model_dict:
            raise ValueError(f"LLM preset '{preset}' not found in llm_models config.")
        model_opts = model_dict[preset]
    else:
        model_opts = find_path_or_default(model_dict)
    model_name = model_opts["model"]
    if model_name.startswith("gateway/"):
        # Pydantic AI Gateway
        provider_name = normalize_gateway_provider(model_name.split(":")[0])
        provider = gateway_provider(
            provider_name,
            api_key=wmill.get_variable("f/api_config/llm_pydantic_gateway_key"),
            route=model_opts["route"],
        )
        if isinstance(provider, GoogleProvider):
            model = GoogleModel(model_name.split(":")[1], provider=provider)
            return model
        else:
            raise ValueError(
                f"Gateway provider {provider_name} is not supported for llm_agent."
            )
    try:
        ollama = wmill.get_variable("f/api_config/llm_ollama_api")
    except Exception:
        ollama = None
    if model_name.startswith("openrouter/"):
        openrouter_model_id = model_name[len("openrouter/") :]
        api_key = wmill.get_variable("f/api_config/llm_openrouter_key")
        provider = OpenRouterProvider(api_key=api_key)
        return OpenRouterModel(openrouter_model_id, provider=provider)
    if ollama:
        provider = OpenAIProvider(base_url=ollama)
        llm = OpenAIChatModel(model_name=model_name, provider=provider)
        return llm
    creds = service_account.Credentials.from_service_account_info(
        json.loads(wmill.get_variable("f/api_config/gcp_service_account_key")),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    project = wmill.get_variable("f/api_config/gcp_project")
    provider = GoogleProvider(credentials=creds, project=project)
    llm = GoogleModel(model_name, provider=provider)
    return llm


def llm_model_settings() -> GoogleModelSettings | None:
    """
    Returns appropriate GoogleModelSettings based on the configured model name.
    - gemini-2.x models: thinking_budget=512
    - gemini-3+ models: thinking_level=LOW
    - Non-Gemini models: None
    """
    models = wmill.get_variable("f/api_config/llm_models")
    model_dict = yaml.full_load(models)
    model_opts = find_path_or_default(model_dict)
    model_name: str = model_opts["model"]

    # Strip provider prefixes to get the bare model name
    for prefix in ("gateway/google:", "openrouter/google/"):
        if model_name.startswith(prefix):
            model_name = model_name[len(prefix) :]
            break

    if not model_name.startswith("gemini-"):
        return None

    # Extract major version number (e.g. "gemini-2.5-flash" → 2, "gemini-3.0-pro" → 3)
    parts = model_name.split("-")
    try:
        major = int(parts[1].split(".")[0])
    except (IndexError, ValueError):
        return None

    if major >= 3:
        return GoogleModelSettings(
            google_thinking_config={"thinking_level": ThinkingLevel.LOW}
        )
    else:
        return GoogleModelSettings(google_thinking_config={"thinking_budget": 512})


def slugify(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_-]+", "-", s)
    s = re.sub(r"^-+|-+$", "", s)
    return s
