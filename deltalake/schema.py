import json
import re
from typing import Union

import pyarrow as pa


def schema_from_string(schema_string: str):
    fields = []
    schema = json.loads(schema_string)
    for field in schema["fields"]:
        name = field["name"]
        type = field["type"]
        nullable = field["nullable"]
        metadata = {key: str(value) for key, value in field["metadata"].items()}
        pa_type = map_type(type)

        fields.append(pa.field(name, pa_type, nullable=nullable, metadata=metadata))
    return pa.schema(fields)


def map_type(input_type: Union[dict, str]):
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

    # If type is string, it should be a "simple" datatype
    if isinstance(input_type, str):
        # map simple data types, that can be directly converted
        if input_type in simple_type_mapping:
            pa_type = simple_type_mapping[input_type]
        else:
            # check for decimal types
            match = re.findall(r"decimal\(([0-9]*),([0-9]*)\)", input_type)
            if len(match) > 0:
                pa_type = pa.decimal128(int(match[0][0]), int(match[0][1]))
            else:
                raise TypeError(
                    f"Got type unsupported {input_type} when trying to parse schema"
                )

    # nested field needs special handling
    else:
        if input_type["type"] == "array":
            # map list type to pyarrow types
            element_type = map_type(input_type["elementType"])
            # pass a field as the type to the list with a name of "element".
            # This is just to comply with the way pyarrow creates lists when infering schemas
            pa_field = pa.field("element", element_type)
            pa_type = pa.list_(pa_field)

        elif input_type["type"] == "map":
            key_type = map_type(input_type["keyType"])
            item_type = map_type(input_type["valueType"])
            pa_type = pa.map_(key_type, item_type)

        elif input_type["type"] == "struct":
            fields = []
            for field in input_type["fields"]:
                name = field["name"]
                input_type = field["type"]
                nullable = field["nullable"]
                metadata = field["metadata"]
                field_type = map_type(input_type)

                fields.append(
                    pa.field(name, field_type, nullable=nullable, metadata=metadata)
                )
            pa_type = pa.struct(fields)

        else:
            raise TypeError(
                f"Got type unsupported {input_type} when trying to parse schema"
            )

    return pa_type
