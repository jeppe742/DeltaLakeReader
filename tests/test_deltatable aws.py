import os
import shutil
from unittest import TestCase

import pyspark
from pyspark.sql.functions import rand
from pandas.testing import assert_frame_equal
from s3fs import S3FileSystem

from deltalake import DeltaTable

AWS_BUCKET = os.getenv("AWS_BUCKET")


class DeltaReaderAppendTest(TestCase):
    @classmethod
    def setUpClass(self):
        self.path = "tests/data/table1"
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
        df = self.spark.range(0, 1000).withColumn("number", rand())

        for i in range(12):
            df.write.format("delta").mode("append").save(self.path)
        self.fs = S3FileSystem()

        self.fs.upload(self.path, f"{AWS_BUCKET}/{self.path}", recursive=True)
        self.table = DeltaTable(f"{AWS_BUCKET}/{self.path}", file_system=self.fs)

    @classmethod
    def tearDownClass(self):
        # remove folder when we are done with the test
        self.fs.rm(f"deltatable/{self.path}", recursive=True)
        shutil.rmtree(self.path)

    def test_paths(self):
        assert self.table.path == f"{AWS_BUCKET}/tests/data/table1"
        assert self.table.log_path == f"{AWS_BUCKET}/tests/data/table1/_delta_log"

    def test_versions(self):

        assert self.table.checkpoint == 10
        assert self.table.version == 11

    def test_data(self):

        # read the parquet files using pandas
        df_pandas = self.table.to_pandas()
        # read the table using spark
        df_spark = self.spark.read.format("delta").load(self.path).toPandas()

        # compare dataframes. The index may not be the same order, so we ignore it
        assert_frame_equal(
            df_pandas.sort_values("id").reset_index(drop=True),
            df_spark.sort_values("id").reset_index(drop=True),
        )

    def test_version(self):
        # read the parquet files using pandas
        df_pandas = self.table.as_version(5, inplace=False).to_pandas()
        # read the table using spark
        df_spark = (
            self.spark.read.format("delta")
            .option("versionAsOf", 5)
            .load(self.path)
            .toPandas()
        )

        # compare dataframes. The index may not be the same order, so we ignore it
        assert_frame_equal(
            df_pandas.sort_values("id").reset_index(drop=True),
            df_spark.sort_values("id").reset_index(drop=True),
        )
