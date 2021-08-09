import json

import pyarrow as pa

_SUPPORTED_DATATYPES = {
    "byte",
    "short",
    "integer",
    "long",
    "float",
    "double",
    "string",
    "boolean",
    "binary",
    "date",
    "timestamp",
}


def schema_from_string(schema_string: str):

    fields = []
    schema = json.loads(schema_string)
    for field in schema["fields"]:

        fields.append(map_field(field))
    return pa.schema(fields)


def map_field(field: dict):

    simple_type_mapping = {
        "byte": pa.int8(),
        "short": pa.int16(),
        "integer": pa.int32(),
        "long": pa.int64(),
        "float": pa.float32(),
        "double": pa.float64(),
        "string": pa.string(),
        "boolean": pa.bool_(),
        "binary": pa.binary(),
        "date": pa.date32(),
        "timestamp": pa.timestamp("ns"),
    }
    name = field["name"]
    type = field["type"]
    nullable = field["nullable"]
    metadata = field["metadata"]

    # check if type is supported
    if type not in _SUPPORTED_DATATYPES:
        raise TypeError(f"Got type unsupported {type} when trying to parse schema")

    # map simple data types, that can be directly converted
    if type in simple_type_mapping:
        type = simple_type_mapping[type]

    return pa.field(name, type, nullable=nullable, metadata=metadata)
