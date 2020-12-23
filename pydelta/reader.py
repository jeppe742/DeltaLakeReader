class DeltaReader:

    __slots__ = (
        "path",
        "credential",
        "log_path",
        "latest_version",
        "latest_checkpoint",
        "parquet_files",
    )

    def __init__(self, path, credential=None):
        self.path = path
        self.credential = credential
        self.log_path = f"{self.path}/_delta_log"
        self.latest_version = 0
        self.latest_checkpoint = 0
        self.parquet_files = set()

        self._authenticate()
        if not self._is_delta_table():
            raise ValueError(f"No delta table found in {self.path}")

        self._as_newest_version()

    def _authenticate(self):
        pass

    def _is_delta_table(self):
        pass

    def _as_newest_version(self):
        pass

    def as_version(self, version):
        pass

    def to_pyarrow(self, columns=None):
        pass

    def to_pandas(self, columns=None):
        return self.to_pyarrow(columns=columns).to_pandas()
