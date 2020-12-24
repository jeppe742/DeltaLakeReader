![Build package](https://github.com/jeppe742/DeltaLakeReader/workflows/Build%20python%20package/badge.svg)
# Delta Lake Reader
The [Delta](https://github.com/delta-io/delta) format, developed by Databricks, is often used to build data lakes.

While it tries to solve many issues with data lakes, one of the downsides is that delta tables rely on Spark to read the data. If you only need to read a small table, this can introduce a lot of unnecessary overhead.

This package tries to fix this, by providing a lightweight python wrapper around the delta file format.



# Usage
Package currently only support local file system, and azure blob storage, but should be easily extended to AWS and GCP in the future.
The main entry point should be the `DeltaReader` class. This will try to derrive the underlying file system, based on the input URL.

When the class is instantiated, it will try to parse the transaction log files, to find the files in the newest table version. It will, however, not read any data before you run the `to_pyarrow` or `to_pandas` functions.
## Local file system

```python
from deltalake import DeltaReader

# native file path
table_path = "somepath/mytable"
# Get table as pyarrow table
df = DeltaReader(table_path).to_pyarrow()
# Get table as pandas dataframe
df = DeltaReader(table_path).to_pandas()


# file url
table_path = "file://somepath/mytable"
df = DeltaReader(table_path).to_pandas()
```
## Azure
The Azure integration is based on the [Azure python SDK](https://docs.microsoft.com/en-us/azure/developer/python/azure-sdk-overview). The `credential` used to authenticate against the storage account, can be either a SAS token, Access Keys or one of the `azure.identity` classes ([read more](https://docs.microsoft.com/en-us/python/api/azure-identity/azure.identity?view=azure-python)).

The input path can either be the https or abfss protocol (will be converted to https under the hood). Note that the current implementation doesn't support the `dfs.core.windows.net` api. But you should simply be able to replace dfs with blob.
```python
from deltalake import DeltaReader

credential = "..." #SAS-token, Access keys or an azure.identity class

#abfss
table_url = "abfss://mycontainer@mystorage.blob.core.windows.net/mytable"
df = DeltaReader(table_url, credential).to_pandas()

#https
table_url = "https://mystorage.blob.core.windows.net/mycontainer/mytable"
df = DeltaReader(table_url, credential).to_pandas()
```

## Time travel
One of the features of the Delta format, is the ability to do timetravel.

This can be done using the `as_version` property. Note that this currenly only support specific version, and not timestamp.
```python
from deltalake import DeltaReader

table_url = "https://mystorage.blob.core.windows.net/mycontainer/mytable"
credential = "..."
df = DeltaReader(table_url, credential).as_version(5).to_pandas()
```


## Disclaimer
Databricks recently announced a stand alone reader for Delta tables in a [blogpost](https://databricks.com/blog/2020/12/22/natively-query-your-delta-lake-with-scala-java-and-python.html)
The python bindings mentioned, however, requires you to install the rust library which might sound scary for a python developer.

# Read more
[Delta transaction log](https://databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html)
