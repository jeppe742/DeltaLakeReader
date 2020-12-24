from deltalake.azure import AzureDeltaReader
from deltalake.local import LocalDeltaReader


class DeltaReader(AzureDeltaReader, LocalDeltaReader):
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
