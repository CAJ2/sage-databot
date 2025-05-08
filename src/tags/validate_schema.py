from jsonschema import validate
import json
import sys


def validate_schema(schema, instance):
    """
    Validate a JSON instance against a JSON schema.

    Args:
        schema (dict): The JSON schema to validate against.
        instance (dict): The JSON instance to validate.

    Raises:
        jsonschema.exceptions.ValidationError: If the instance is not valid according to the schema.
    """
    try:
        validate(instance=instance, schema=schema)
        print("Validation successful!")
    except Exception as e:
        print(f"Validation error: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python validate_schema.py <schema_file> <instance_file>")
        sys.exit(1)
    with open("src/tags/schemas/" + sys.argv[1], "r") as schema_file:
        schema = json.load(schema_file)
    with open("src/tags/schemas/" + sys.argv[2], "r") as instance_file:
        instance = json.load(instance_file)
    validate_schema(schema, instance)
