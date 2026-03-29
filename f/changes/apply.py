# requirements: project

from typing import Any

from f.graphql.api_client.input_types import (
    CreateCategoryInput,
    CreateChangeInput,
    CreateComponentInput,
    CreateItemInput,
    CreateProcessInput,
    CreateVariantInput,
    UpdateCategoryInput,
    UpdateComponentInput,
    UpdateItemInput,
    UpdateProcessInput,
    UpdateVariantInput,
)
from f.utils.api import api_connect


def _change_fields(
    apply: bool,
    change_id: str | None,
    change_title: str | None,
    entity_name: str,
) -> dict[str, Any]:
    if apply:
        return {"apply": True}
    if change_id:
        return {"changeID": change_id}
    return {
        "change": CreateChangeInput(
            title=change_title or f"Auto-suggested {entity_name}"
        )
    }


def _id_list(items: list[Any]) -> list[str]:
    return [x["id"] if isinstance(x, dict) else x for x in items]


def _id_obj_list(items: list[Any]) -> list[dict[str, Any]]:
    return [{"id": x["id"] if isinstance(x, dict) else x} for x in items]


def _coerce_singular(val: Any) -> dict[str, Any] | None:
    """Unwrap an array to a singular ref dict (for fallback branch that always sends lists)."""
    if val is None:
        return None
    if isinstance(val, list):
        val = val[0]
    return val if isinstance(val, dict) else {"id": val}


# --- Variant ---


def _build_variant_create(
    data: dict[str, Any], chg: dict[str, Any]
) -> CreateVariantInput:
    d: dict[str, Any] = {
        "name": data.get("name"),
        "nameTr": data.get("name_tr"),
        "code": data.get("code"),
        "desc": data.get("desc"),
        "descTr": data.get("desc_tr"),
        "imageURL": data.get("image_url"),
        "lang": data.get("lang"),
        "items": [{"id": x["id"]} for x in data["items"]]
        if data.get("items")
        else None,
        "components": [
            {"id": x["id"], "quantity": x.get("quantity"), "unit": x.get("unit")}
            for x in data["components"]
        ]
        if data.get("components")
        else None,
        "orgs": [{"id": x["id"]} for x in data["orgs"]] if data.get("orgs") else None,
        "regions": [{"id": x["id"]} for x in data["regions"]]
        if data.get("regions")
        else None,
        "region": {"id": data["region"]["id"]} if data.get("region") else None,
        "tags": [{"id": x["id"], "meta": x.get("meta")} for x in data["tags"]]
        if data.get("tags")
        else None,
        **chg,
    }
    return CreateVariantInput.model_validate(
        {k: v for k, v in d.items() if v is not None}
    )


def _build_variant_update(
    entity_id: str, data: dict[str, Any], chg: dict[str, Any]
) -> UpdateVariantInput:
    d: dict[str, Any] = {
        "id": entity_id,
        "name": data.get("name"),
        "nameTr": data.get("name_tr"),
        "code": data.get("code"),
        "desc": data.get("desc"),
        "descTr": data.get("desc_tr"),
        "imageURL": data.get("image_url"),
        "lang": data.get("lang"),
        # replace-style
        "items": [{"id": x["id"]} for x in data["items"]]
        if data.get("items")
        else None,
        "components": [
            {"id": x["id"], "quantity": x.get("quantity"), "unit": x.get("unit")}
            for x in data["components"]
        ]
        if data.get("components")
        else None,
        "orgs": [{"id": x["id"]} for x in data["orgs"]] if data.get("orgs") else None,
        "region": {"id": data["region"]["id"]} if data.get("region") else None,
        "tags": [{"id": x["id"], "meta": x.get("meta")} for x in data["tags"]]
        if data.get("tags")
        else None,
        # add/remove-style (from ai_ref)
        "addItems": _id_obj_list(data["add_items"]) if data.get("add_items") else None,
        "removeItems": _id_list(data["remove_items"])
        if data.get("remove_items")
        else None,
        "addComponents": [
            {
                "id": x["id"] if isinstance(x, dict) else x,
                "quantity": x.get("quantity") if isinstance(x, dict) else None,
                "unit": x.get("unit") if isinstance(x, dict) else None,
            }
            for x in data["add_components"]
        ]
        if data.get("add_components")
        else None,
        "removeComponents": _id_list(data["remove_components"])
        if data.get("remove_components")
        else None,
        "addOrgs": _id_obj_list(data["add_orgs"]) if data.get("add_orgs") else None,
        "removeOrgs": _id_list(data["remove_orgs"])
        if data.get("remove_orgs")
        else None,
        "addRegions": _id_obj_list(data["add_regions"])
        if data.get("add_regions")
        else None,
        "removeRegions": _id_list(data["remove_regions"])
        if data.get("remove_regions")
        else None,
        "addTags": [
            {
                "id": x["id"] if isinstance(x, dict) else x,
                "meta": x.get("meta") if isinstance(x, dict) else None,
            }
            for x in data["add_tags"]
        ]
        if data.get("add_tags")
        else None,
        "removeTags": _id_list(data["remove_tags"])
        if data.get("remove_tags")
        else None,
        **chg,
    }
    return UpdateVariantInput.model_validate(
        {k: v for k, v in d.items() if v is not None}
    )


# --- Item ---


def _build_item_create(data: dict[str, Any], chg: dict[str, Any]) -> CreateItemInput:
    d: dict[str, Any] = {
        "name": data.get("name"),
        "nameTr": data.get("name_tr"),
        "desc": data.get("desc"),
        "descTr": data.get("desc_tr"),
        "imageURL": data.get("image_url"),
        "lang": data.get("lang"),
        "categories": [{"id": x["id"]} for x in data["categories"]]
        if data.get("categories")
        else None,
        "tags": [{"id": x["id"], "meta": x.get("meta")} for x in data["tags"]]
        if data.get("tags")
        else None,
        **chg,
    }
    return CreateItemInput.model_validate({k: v for k, v in d.items() if v is not None})


def _build_item_update(
    entity_id: str, data: dict[str, Any], chg: dict[str, Any]
) -> UpdateItemInput:
    d: dict[str, Any] = {
        "id": entity_id,
        "name": data.get("name"),
        "nameTr": data.get("name_tr"),
        "desc": data.get("desc"),
        "descTr": data.get("desc_tr"),
        "imageURL": data.get("image_url"),
        "lang": data.get("lang"),
        "categories": [{"id": x["id"]} for x in data["categories"]]
        if data.get("categories")
        else None,
        "tags": [{"id": x["id"], "meta": x.get("meta")} for x in data["tags"]]
        if data.get("tags")
        else None,
        "addCategories": _id_obj_list(data["add_categories"])
        if data.get("add_categories")
        else None,
        "removeCategories": _id_list(data["remove_categories"])
        if data.get("remove_categories")
        else None,
        "addTags": [
            {
                "id": x["id"] if isinstance(x, dict) else x,
                "meta": x.get("meta") if isinstance(x, dict) else None,
            }
            for x in data["add_tags"]
        ]
        if data.get("add_tags")
        else None,
        "removeTags": _id_list(data["remove_tags"])
        if data.get("remove_tags")
        else None,
        **chg,
    }
    return UpdateItemInput.model_validate({k: v for k, v in d.items() if v is not None})


# --- Component ---


def _build_component_create(
    data: dict[str, Any], chg: dict[str, Any]
) -> CreateComponentInput:
    d: dict[str, Any] = {
        "name": data.get("name"),
        "nameTr": data.get("name_tr"),
        "desc": data.get("desc"),
        "descTr": data.get("desc_tr"),
        "imageURL": data.get("image_url"),
        "lang": data.get("lang"),
        "physical": data.get("physical"),
        "visual": data.get("visual"),
        "primaryMaterial": {
            "id": data["primary_material"]["id"],
            "materialFraction": data["primary_material"].get("material_fraction"),
        }
        if data.get("primary_material")
        else None,
        "materials": [
            {"id": x["id"], "materialFraction": x.get("material_fraction")}
            for x in data["materials"]
        ]
        if data.get("materials")
        else None,
        "region": _coerce_singular(data.get("region")),
        "tags": [{"id": x["id"], "meta": x.get("meta")} for x in data["tags"]]
        if data.get("tags")
        else None,
        **chg,
    }
    return CreateComponentInput.model_validate(
        {k: v for k, v in d.items() if v is not None}
    )


def _build_component_update(
    entity_id: str, data: dict[str, Any], chg: dict[str, Any]
) -> UpdateComponentInput:
    d: dict[str, Any] = {
        "id": entity_id,
        "name": data.get("name"),
        "nameTr": data.get("name_tr"),
        "desc": data.get("desc"),
        "descTr": data.get("desc_tr"),
        "imageURL": data.get("image_url"),
        "lang": data.get("lang"),
        "physical": data.get("physical"),
        "visual": data.get("visual"),
        "primaryMaterial": {
            "id": data["primary_material"]["id"],
            "materialFraction": data["primary_material"].get("material_fraction"),
        }
        if data.get("primary_material")
        else None,
        "materials": [
            {"id": x["id"], "materialFraction": x.get("material_fraction")}
            for x in data["materials"]
        ]
        if data.get("materials")
        else None,
        "region": _coerce_singular(data.get("region")),
        "tags": [{"id": x["id"], "meta": x.get("meta")} for x in data["tags"]]
        if data.get("tags")
        else None,
        "addTags": [
            {
                "id": x["id"] if isinstance(x, dict) else x,
                "meta": x.get("meta") if isinstance(x, dict) else None,
            }
            for x in data["add_tags"]
        ]
        if data.get("add_tags")
        else None,
        "removeTags": _id_list(data["remove_tags"])
        if data.get("remove_tags")
        else None,
        **chg,
    }
    return UpdateComponentInput.model_validate(
        {k: v for k, v in d.items() if v is not None}
    )


# --- Process ---


def _build_process_create(
    data: dict[str, Any], chg: dict[str, Any]
) -> CreateProcessInput:
    intent = data.get("intent")
    if not intent:
        raise ValueError("CreateProcessInput requires 'intent' field")
    d: dict[str, Any] = {
        "intent": intent,
        "name": data.get("name"),
        "nameTr": data.get("name_tr"),
        "desc": data.get("desc"),
        "descTr": data.get("desc_tr"),
        "lang": data.get("lang"),
        "efficiency": data.get("efficiency"),
        "instructions": data.get("instructions"),
        "rules": data.get("rules"),
        "material": _coerce_singular(data.get("material")),
        "org": _coerce_singular(data.get("org")),
        "place": _coerce_singular(data.get("place")),
        "region": _coerce_singular(data.get("region")),
        "variant": _coerce_singular(data.get("variant")),
        **chg,
    }
    return CreateProcessInput.model_validate(
        {k: v for k, v in d.items() if v is not None}
    )


def _build_process_update(
    entity_id: str, data: dict[str, Any], chg: dict[str, Any]
) -> UpdateProcessInput:
    d: dict[str, Any] = {
        "id": entity_id,
        "intent": data.get("intent"),
        "name": data.get("name"),
        "nameTr": data.get("name_tr"),
        "desc": data.get("desc"),
        "descTr": data.get("desc_tr"),
        "lang": data.get("lang"),
        "efficiency": data.get("efficiency"),
        "instructions": data.get("instructions"),
        "rules": data.get("rules"),
        "material": _coerce_singular(data.get("material")),
        "org": _coerce_singular(data.get("org")),
        "place": _coerce_singular(data.get("place")),
        "region": _coerce_singular(data.get("region")),
        "variant": _coerce_singular(data.get("variant")),
        **chg,
    }
    return UpdateProcessInput.model_validate(
        {k: v for k, v in d.items() if v is not None}
    )


# --- Category ---


def _build_category_create(
    data: dict[str, Any], chg: dict[str, Any]
) -> CreateCategoryInput:
    d: dict[str, Any] = {
        "name": data.get("name"),
        "nameTr": data.get("name_tr"),
        "desc": data.get("desc"),
        "descTr": data.get("desc_tr"),
        "descShort": data.get("desc_short"),
        "descShortTr": data.get("desc_short_tr"),
        "imageURL": data.get("image_url"),
        "lang": data.get("lang"),
        **chg,
    }
    return CreateCategoryInput.model_validate(
        {k: v for k, v in d.items() if v is not None}
    )


def _build_category_update(
    entity_id: str, data: dict[str, Any], chg: dict[str, Any]
) -> UpdateCategoryInput:
    d: dict[str, Any] = {
        "id": entity_id,
        "name": data.get("name"),
        "nameTr": data.get("name_tr"),
        "desc": data.get("desc"),
        "descTr": data.get("desc_tr"),
        "descShort": data.get("desc_short"),
        "descShortTr": data.get("desc_short_tr"),
        "imageURL": data.get("image_url"),
        "lang": data.get("lang"),
        **chg,
    }
    return UpdateCategoryInput.model_validate(
        {k: v for k, v in d.items() if v is not None}
    )


# --- Dispatch ---


_ENTITY_HANDLERS: dict[str, tuple[Any, Any]] = {
    "Variant": (_build_variant_create, _build_variant_update),
    "Item": (_build_item_create, _build_item_update),
    "Component": (_build_component_create, _build_component_update),
    "Process": (_build_process_create, _build_process_update),
    "Category": (_build_category_create, _build_category_update),
}


def _call_mutation(
    client: Any, entity_name: str, entity_id: str | None, input_obj: Any
) -> tuple[str, str | None]:
    if entity_name == "Variant":
        if entity_id is None:
            r = client.add_variant(input=input_obj)
            return (
                r.create_variant.variant.id,
                r.create_variant.change.id if r.create_variant.change else None,
            )
        else:
            r = client.update_variant(input=input_obj)
            return (
                r.update_variant.variant.id,
                r.update_variant.change.id if r.update_variant.change else None,
            )
    elif entity_name == "Item":
        if entity_id is None:
            r = client.add_item(input=input_obj)
            return (
                r.create_item.item.id,
                r.create_item.change.id if r.create_item.change else None,
            )
        else:
            r = client.update_item(input=input_obj)
            return (
                r.update_item.item.id,
                r.update_item.change.id if r.update_item.change else None,
            )
    elif entity_name == "Component":
        if entity_id is None:
            r = client.add_component(input=input_obj)
            return (
                r.create_component.component.id,
                r.create_component.change.id if r.create_component.change else None,
            )
        else:
            r = client.update_component(input=input_obj)
            return (
                r.update_component.component.id,
                r.update_component.change.id if r.update_component.change else None,
            )
    elif entity_name == "Process":
        if entity_id is None:
            r = client.add_process(input=input_obj)
            return (
                r.create_process.process.id,
                r.create_process.change.id if r.create_process.change else None,
            )
        else:
            r = client.update_process(input=input_obj)
            return (
                r.update_process.process.id,
                r.update_process.change.id if r.update_process.change else None,
            )
    elif entity_name == "Category":
        if entity_id is None:
            r = client.add_category(input=input_obj)
            return (
                r.create_category.category.id,
                r.create_category.change.id if r.create_category.change else None,
            )
        else:
            r = client.update_category(input=input_obj)
            return (
                r.update_category.category.id,
                r.update_category.change.id if r.update_category.change else None,
            )
    raise ValueError(f"Unsupported entity_name: {entity_name!r}")


def main(
    entity_name: str,
    entity_id: str | None,
    data: dict[str, Any],
    apply: bool = False,
    change_id: str | None = None,
    change_title: str | None = None,
) -> dict[str, Any]:
    """
    Submit a Create*Input or Update*Input to the GraphQL API.

    entity_name: one of Variant, Item, Component, Process, Category
    entity_id: None for create, entity ID for update
    data: mutation-ready payload dict from ai_create / ai_suggest / ai_ref
    apply: if True, immediately merge (no Change); if False, create a Change Edit
    change_id: attach to an existing Change
    change_title: title for a new Change (auto-generated if omitted)

    Returns: {"entity_id": str, "change_id": str | None, "applied": bool}
    """
    if entity_name == "Place":
        raise ValueError("Place has no create/update mutations — apply not supported")
    if entity_name not in _ENTITY_HANDLERS:
        raise ValueError(f"Unknown entity_name: {entity_name!r}")
    if not data:
        raise ValueError(f"No data to apply for {entity_name} {entity_id}")

    client, _ = api_connect()
    chg = _change_fields(apply, change_id, change_title, entity_name)
    create_fn, update_fn = _ENTITY_HANDLERS[entity_name]

    if entity_id is None:
        input_obj = create_fn(data, chg)
    else:
        input_obj = update_fn(entity_id, data, chg)

    result_entity_id, result_change_id = _call_mutation(
        client, entity_name, entity_id, input_obj
    )

    return {
        "entity_id": result_entity_id,
        "change_id": result_change_id,
        "applied": apply,
    }
