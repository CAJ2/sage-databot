# requirements: project

from typing import Any

from f.context.context_types import SchemaMode


def fetch_context_entity(
    *,
    entity_id: str | None,
    entity_name: str,
    fetch_fn: Any,
    result_attr: str,
) -> tuple[dict[str, Any], Any | None]:
    if entity_id is None:
        return {}, None

    try:
        result = fetch_fn(id=entity_id)
    except Exception as e:
        raise ValueError(f"Could not fetch {entity_name} {entity_id}: {e}") from e

    entity = getattr(result, result_attr)
    if entity is None:
        raise ValueError(f"Could not fetch {entity_name} {entity_id}: not found")

    return entity.model_dump(by_alias=False), result


def fetch_context_schema(
    *,
    entity_name: str,
    schema_mode: SchemaMode,
    fetch_fn: Any,
    schema_attr: str,
) -> dict[str, Any]:
    try:
        schema_result = fetch_fn()
    except Exception as e:
        raise ValueError(f"Could not fetch {entity_name} schema: {e}") from e

    schema_wrapper = getattr(schema_result, schema_attr)
    if schema_wrapper is None:
        raise ValueError(f"Could not fetch {entity_name} schema: missing schema")

    schema_obj = (
        schema_wrapper.create if schema_mode == "create" else schema_wrapper.update
    )
    if schema_obj is None or schema_obj.schema_ is None:
        raise ValueError(
            f"Could not fetch {entity_name} schema: missing {schema_mode} schema"
        )

    return schema_obj.schema_
