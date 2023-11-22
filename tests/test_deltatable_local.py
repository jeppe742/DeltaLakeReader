import shutil
import uuid
from unittest import TestCase

import pyarrow.dataset as ds
import pyspark
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.functions import col, rand, when

from deltalake import DeltaTable, __version__


def test_version():
    assert __version__ == "0.2.16"


class DeltaReaderAppendTest(TestCase):
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
        df = (
            self.spark.range(0, 1000)
            .withColumn("number", rand())
            .withColumn("number2", when(col("id") < 500, 0).otherwise(1))
        )

        for i in range(12):
            df.withColumn("id", col("id") + 1000 * i).write.partitionBy(
                "number2"
            ).format("delta").mode("append").save(self.path)

        self.table = DeltaTable(self.path)

    @classmethod
    def tearDownClass(self):
        # remove folder when we are done with the test
        shutil.rmtree(self.path)

    def test_paths(self):
        assert self.table.path == self.path
        assert self.table.log_path == f"{self.path}/_delta_log"

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
            df_pandas.set_index("id"), df_spark.set_index("id"), check_like=True
        )

    def test_version_no_checkpoint(self):
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

    def test_version_checkpoint(self):
        # read the parquet files using pandas
        df_pandas = self.table.as_version(11, inplace=False).to_pandas()
        # read the table using spark
        df_spark = (
            self.spark.read.format("delta")
            .option("versionAsOf", 11)
            .load(self.path)
            .toPandas()
        )

        # compare dataframes. The index may not be the same order, so we ignore it
        assert_frame_equal(
            df_pandas.set_index("id"), df_spark.set_index("id"), check_like=True
        )

    def test_partitioning(self):
        # Partition pruning should half number of rows
        assert self.table.to_table(filter=ds.field("number2") == 0).num_rows == 6000

    def test_predicate_pushdown(self):
        # number is random 0-1, so we should have fewer than 12000 rows no matter what
        assert self.table.to_table(filter=ds.field("number") < 0.5).num_rows < 12000

    def test_column_pruning(self):
        t = self.table.to_table(columns=["number", "number2"])
        assert t.column_names == ["number", "number2"]


class DeltaReaderUpdateTest(TestCase):
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
        df = (
            self.spark.range(0, 1000)
            .withColumn("number", rand())
            .withColumn("number2", when(col("id") < 500, 0).otherwise(1))
        )

        for i in range(12):
            df.withColumn("id", col("id") + 1000 * i).write.partitionBy(
                "number2"
            ).format("delta").mode("append").save(self.path)
        # Create a temp view of table, so we can update it
        self.spark.read.format("delta").load(self.path).createOrReplaceTempView(
            "table1"
        )
        self.spark.sql("UPDATE table1 set number=123 where id='0'")
        self.table = DeltaTable(self.path)

    @classmethod
    def tearDownClass(self):
        # remove folder when we are done with the test
        shutil.rmtree(self.path)

    def test_paths(self):
        assert self.table.path == self.path
        assert self.table.log_path == f"{self.path}/_delta_log"

    def test_versions(self):
        assert self.table.checkpoint == 10
        assert self.table.version == 12

    def test_data(self):
        # read the parquet files using pandas
        df_pandas = self.table.to_pandas()
        # read the table using spark
        df_spark = self.spark.read.format("delta").load(self.path).toPandas()

        # compare dataframes. The index may not be the same order, so we ignore it
        assert_frame_equal(
            df_pandas.set_index("id"), df_spark.set_index("id"), check_like=True
        )

    def test_version_no_checkpoint(self):
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
            df_pandas.set_index("id"), df_spark.set_index("id"), check_like=True
        )

    def test_version_checkpoint(self):
        # read the parquet files using pandas
        df_pandas = self.table.as_version(11, inplace=False).to_pandas()
        # read the table using spark
        df_spark = (
            self.spark.read.format("delta")
            .option("versionAsOf", 11)
            .load(self.path)
            .toPandas()
        )

        # compare dataframes. The index may not be the same order, so we ignore it
        assert_frame_equal(
            df_pandas.set_index("id"), df_spark.set_index("id"), check_like=True
        )

    def test_partitioning(self):
        # Partition pruning should half number of rows
        assert self.table.to_table(filter=ds.field("number2") == 0).num_rows == 6000

    def test_predicate_pushdown(self):
        # number is random 0-1, so we should have fewer than 12000 rows no matter what
        assert self.table.to_table(filter=ds.field("number") < 0.5).num_rows < 12000

    def test_column_pruning(self):
        t = self.table.to_table(columns=["number", "number2"])
        assert t.column_names == ["number", "number2"]


class DeltaReaderSchemaEvolutionTest(TestCase):
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

        df = (
            self.spark.range(0, 1000)
            .withColumn("number", rand())
            .withColumn("number2", when(col("id") < 500, 0).otherwise(1))
        )

        for i in range(5):
            df.withColumn("id", col("id") + 1000 * i).write.partitionBy(
                "number2"
            ).format("delta").mode("append").save(self.path)

        # Add data with one more column, using schema evolution
        df = df.withColumn("number3", rand())
        for i in range(5):
            df.withColumn("id", col("id") + 1000 * (i + 5)).write.partitionBy(
                "number2"
            ).option("mergeSchema", "true").format("delta").mode("append").save(
                self.path
            )

        # Add data with one more column, using schema evolution
        df = df.withColumn("number4", rand())
        for i in range(5):
            df.withColumn("id", col("id") + 1000 * (i + 10)).write.partitionBy(
                "number2"
            ).option("mergeSchema", "true").format("delta").mode("append").save(
                self.path
            )

        # remove some columns, using schema evolution
        df = df.select(col("id"), col("number"), col("number2"))
        for i in range(5):
            df.withColumn("id", col("id") + 1000 * (i + 15)).write.partitionBy(
                "number2"
            ).option("mergeSchema", "true").format("delta").mode("append").save(
                self.path
            )

        self.table = DeltaTable(self.path)

    @classmethod
    def tearDownClass(self):
        # remove folder when we are done with the test
        shutil.rmtree(self.path)

    def test_data(self):
        # read the parquet files using pandas
        df_pandas = self.table.to_pandas()
        # read the table using spark
        df_spark = self.spark.read.format("delta").load(self.path).toPandas()

        # compare dataframes. The index may not be the same order, so we ignore it
        assert_frame_equal(
            df_pandas.set_index("id"), df_spark.set_index("id"), check_like=True
        )

    def test_version_no_checkpoint(self):
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
            df_pandas.set_index("id"), df_spark.set_index("id"), check_like=True
        )

    def test_version_checkpoint(self):
        # read the parquet files using pandas
        df_pandas = self.table.as_version(11, inplace=False).to_pandas()
        # read the table using spark
        df_spark = (
            self.spark.read.format("delta")
            .option("versionAsOf", 11)
            .load(self.path)
            .toPandas()
        )

        # compare dataframes. The index may not be the same order, so we ignore it
        assert_frame_equal(
            df_pandas.set_index("id"), df_spark.set_index("id"), check_like=True
        )

    def test_version_removed_columns(self):
        # read the parquet files using pandas
        df_pandas = self.table.as_version(15, inplace=False).to_pandas()
        # read the table using spark
        df_spark = (
            self.spark.read.format("delta")
            .option("versionAsOf", 15)
            .load(self.path)
            .toPandas()
        )

        # compare dataframes. The index may not be the same order, so we ignore it
        assert_frame_equal(
            df_pandas.set_index("id"), df_spark.set_index("id"), check_like=True
        )

    def test_partitioning(self):
        # Partition pruning should half number of rows
        assert self.table.to_table(filter=ds.field("number2") == 0).num_rows == 10000

    def test_predicate_pushdown(self):
        # number is random 0-1, so we should have fewer than 12000 rows no matter what
        assert self.table.to_table(filter=ds.field("number") < 0.5).num_rows < 12000

    def test_column_pruning(self):
        t = self.table.to_table(columns=["number", "number2"])
        assert t.column_names == ["number", "number2"]


class DeltaReaderExistTest(TestCase):
    def test_is_deltatable(self):
        with pytest.raises(ValueError):
            DeltaTable("/foo")
