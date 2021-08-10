import json
import os
import shutil
import uuid
from datetime import datetime
from unittest import TestCase

import pyspark
from pyarrow.dataset import dataset as pyarrow_dataset
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from deltalake.schema import schema_from_string

os.environ["PYSPARK_PYTHON"] = ".venv/bin/python3"


class DeltaSchemaMappingTest(TestCase):
    @classmethod
    def setUpClass(self):
        self.path = f"tests/{str(uuid.uuid4())}/table1"
        self.spark = (
            pyspark.sql.SparkSession.builder.appName("deltalake")
            .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .getOrCreate()
        )

        schema = StructType(
            [
                StructField("byte", ByteType()),
                StructField("short", ShortType()),
                StructField("integer", IntegerType()),
                StructField("long", LongType()),
                StructField("float", FloatType()),
                StructField("double", DoubleType()),
                StructField("string", StringType()),
                StructField("binary", BinaryType()),
                StructField("boolean", BooleanType()),
                StructField("timestamp", TimestampType()),
                StructField("date", DateType()),
                StructField("array", ArrayType(IntegerType())),
                StructField("map", MapType(StringType(), IntegerType())),
                StructField(
                    "struct",
                    StructType(
                        [
                            StructField("nested_int", IntegerType()),
                            StructField("nested_string", StringType()),
                        ]
                    ),
                ),
            ]
        )

        data = [
            (
                1,
                1,
                1,
                1,
                1.0,
                1.0,
                "1",
                bytearray([1]),
                True,
                datetime.now(),
                datetime.now(),
                [1, 2, 3],
                {"a": 1, "b": 2},
                [1, "a"],
            )
        ]

        df = self.spark.createDataFrame(data=data, schema=schema)
        df.write.format("delta").mode("append").save(self.path)

    @classmethod
    def tearDownClass(self):
        # remove folder when we are done with the test
        shutil.rmtree(self.path)

    def test_schema_mapping(self):

        expected_schema = pyarrow_dataset(
            source=self.path,
            format="parquet",
        ).schema

        with open(f"{self.path}/_delta_log/00000000000000000000.json") as f:
            for line in f:
                if "metaData" in line:
                    schema_string = json.loads(line)["metaData"]["schemaString"]
                    break

        schema = schema_from_string(schema_string)
        assert expected_schema == schema
