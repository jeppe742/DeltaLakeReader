import shutil
import uuid
from time import time

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyspark
from pyspark.sql.functions import col, rand, when

from deltalake import DeltaTable

spark = (
    pyspark.sql.SparkSession.builder.appName("deltalake")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.executor.memory", "24g")
    .config("spark.driver.memory", "24g")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

for n in np.logspace(3, 8):
    path = f"tests/data/{str(uuid.uuid4())}/table1"
    df = (
        spark.range(0, n)
        .withColumn("number", rand())
        .withColumn("number2", when(col("id") < 500, 0).otherwise(1))
    )

    df.write.format("delta").mode("append").save(path)

    table = DeltaTable(path)
    t = time()
    df_pandas = table.to_pandas()
    t_dt = time() - t

    t = time()
    df_spark = spark.read.format("delta").load(table.path).toPandas()
    t_spark = time() - t
    print(f"{n},t_df,{t_dt}\n{n},t_spark,{t_spark}")

    with open("performance_tests/results.txt", "a") as f:
        print(f"{n},delta-lake-reader,{t_dt}", file=f)
        print(f"{n},spark,{t_spark}", file=f)

    shutil.rmtree(path)

plt.style.use("fivethirtyeight")
df = pd.read_csv("performance_tests/results.txt")
df.columns = ["n", "type", "t"]
df.groupby(["n", "type"]).sum()["t"].unstack().plot()
plt.yscale("log")
plt.xscale("log")
plt.xlabel("Number of rows in table")
plt.ylabel("Time to load dataframe [s]")
plt.show()
