import pandas as pd


class BaseDeltaReader:
    def __init__(self, path, credential=None):
        self.path = path
        self.credential = credential
        self.version = 0
        self.checkpoint = 0
        self.files = set()

        self._parse_path()
        self._authenticate()

        self.log_path = f"{self.path}/_delta_log"

        if not self._is_delta_table():
            raise ValueError(f"No delta table found in {self.path}")

        self._as_newest_version()

    def _parse_path(self):
        pass

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

    def to_pandas(self, columns=None) -> pd.DataFrame:
        """Reads the Delta table as a pandas dataframe

        Parameters:
        ----------
        columns : ([string], optional)
            Only read a subset of columns from the table. Defaults to None.

        Returns:
        -------
        df : (pd.Dataframe)
            Content of Delta table as pandas dataframe
        """
        return self.to_pyarrow(columns=columns).to_pandas()
