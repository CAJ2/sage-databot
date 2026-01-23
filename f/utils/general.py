import wmill
import os
import re
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.models.google import GoogleModel
from pydantic_ai.providers.google import GoogleProvider
from pydantic_ai.providers.openai import OpenAIProvider
from google.oauth2 import service_account

def is_production() -> bool:
    """
    Check if the environment is production.
    """
    env = os.environ.get("WM_WORKSPACE")
    return env == "sage-prod"


def llm_agent(model_var: str = "llm_model"):
    model_name = wmill.get_variable(model_var)
    if not model_name:
        raise ValueError(f"Variable {model_var} is not set.")
    ollama = wmill.get_variable("ollama_api")
    if len(ollama) > 0:
        provider = OpenAIProvider(base_url=ollama)
        llm = OpenAIModel(model_name=model_name, provider=provider)
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
