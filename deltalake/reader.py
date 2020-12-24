from deltalake.azure import AzureDeltaReader
from deltalake.local import LocalDeltaReader


class DeltaReader(AzureDeltaReader, LocalDeltaReader):
    """
    Main entry point for interacting with delta tables.
    Instantiating this class will only try to find the underlying files for the table.
    No data is read until you call the `to_pyarrow` or `to_pandas` functions.
    Parameters:
    ----------
    path: (string)
        Path to the delta table. Class will try to derive the storage location from the path.
        Currently only supports local and Azure.

    credential: (string, azure.identity class)
        credential to authenticate against storage.
        For local file system this can be omitted, but for Azure it should be a
        SAS-token, Access Key or `azure.identity` class

    Returns:
    -------
    dr : (DeltaReader)
        Delta reader that has parsed the log files
    """

    def __new__(self, path, credential=None):
        if "abfss://" in path:
            return AzureDeltaReader(path, credential)
        elif "https://" in path:
            return AzureDeltaReader(path, credential)
        elif "s3://" in path:
            raise NotImplementedError("S3 not supported yet")
        elif "file://" in path:
            return LocalDeltaReader(path)
        elif "://" not in path:
            return LocalDeltaReader(path)
        raise NotImplementedError(
            f"Could not find supported storage integration for {path}"
        )
