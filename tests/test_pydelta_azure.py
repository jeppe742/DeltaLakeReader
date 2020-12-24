import os
import shutil
from unittest import TestCase

import pyspark
from pandas.testing import assert_frame_equal

from deltalake.azure import AzureDeltaReader

AZURE_CREDENTIALS = os.getenv("AZURE_STORAGE_ACCESS_KEY")


class DeltaReaderHttpsTest(TestCase):
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
        df = self.spark.range(0, 1000)

        for i in range(12):
            df.write.format("delta").mode("append").save(self.path)

        self.table = AzureDeltaReader(
            "https://pydelta.blob.core.windows.net/testdata/table1",
            credential=AZURE_CREDENTIALS,
        )

    @classmethod
    def tearDownClass(self):
        # remove folder when we are done with the test
        shutil.rmtree(self.path)

    def test_paths(self):
        assert (
            self.table.path == "https://pydelta.blob.core.windows.net/testdata/table1"
        )
        assert (
            self.table.log_path
            == "https://pydelta.blob.core.windows.net/testdata/table1/_delta_log"
        )

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
        df_pandas = self.table.as_version(5).to_pandas()
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


class DeltaReaderAbfssTest(TestCase):
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
        df = self.spark.range(0, 1000)

        for i in range(12):
            df.write.format("delta").mode("append").save(self.path)

        self.table = AzureDeltaReader(
            "abfss://testdata@pydelta.blob.core.windows.net/table1",
            credential=AZURE_CREDENTIALS,
        )

    @classmethod
    def tearDownClass(self):
        # remove folder when we are done with the test
        shutil.rmtree(self.path)

    def test_paths(self):
        assert (
            self.table.path == "abfss://testdata@pydelta.blob.core.windows.net/table1"
        )
        assert (
            self.table.log_path
            == "abfss://testdata@pydelta.blob.core.windows.net/table1/_delta_log"
        )

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
        df_pandas = self.table.as_version(5).to_pandas()
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


class DeltaReaderAppendNoCheckpointTest(TestCase):
    @classmethod
    def setUpClass(self):
        self.path = "tests/data/table2"

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
        df = self.spark.range(0, 1000)

        for i in range(10):
            df.write.format("delta").mode("append").save(self.path)

        self.table = AzureDeltaReader(
            "https://pydelta.blob.core.windows.net/testdata/table2",
            credential=AZURE_CREDENTIALS,
        )

    @classmethod
    def tearDownClass(self):
        # remove folder when we are done with the test
        shutil.rmtree(self.path)

    def test_paths(self):
        assert (
            self.table.path == "https://pydelta.blob.core.windows.net/testdata/table2"
        )
        assert (
            self.table.log_path
            == "https://pydelta.blob.core.windows.net/testdata/table2/_delta_log"
        )

    def test_versions(self):

        assert self.table.checkpoint == 0
        assert self.table.version == 9

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
        df_pandas = self.table.as_version(5).to_pandas()
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
