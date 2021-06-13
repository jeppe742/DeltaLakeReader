import json
import re
from copy import deepcopy

import pyarrow.parquet as pq
from fsspec.implementations.local import LocalFileSystem
from fsspec.spec import AbstractFileSystem
from pyarrow.dataset import dataset as pyarrow_dataset


class DeltaTable:
    """
    Access Delta tables on different filesystems.

    Parameters
    -----------
    path:  string
        Path to the table in the file system.
        Note that for Azure, AWS and GCP you need to include the container/bucket name in the path
    file_system: FSSpec compliant filesystem
        Filesystem to be used for reading the Delta files.

    """

    def __init__(self, path, file_system=None):
        if not isinstance(file_system, AbstractFileSystem) and file_system is not None:
            raise TypeError("file_system must be a fsspec compliant filesystem")

        self.path = path
        self.log_path = f"{self.path}/_delta_log"
        self.version = 0
        self.checkpoint = 0
        self.files = set()
        self._latest_file = ""
        self._latest_file_time = 0
        if file_system is None:
            file_system = LocalFileSystem()
        self.filesystem = file_system
        self._as_newest_version()

        # The PyArrow Dataset is exposed by a factory class,
        # which makes it hard to inherit from it directly.
        # Instead we will just have the dataset as an attribute and expose the important methods.
        self.pyarrow_dataset = self._pyarrow_dataset()

    def _pyarrow_dataset(self):
        # Use the latest file to fetch the schema.
        # This will allow for schema evolution
        schema = pyarrow_dataset(
            source=self._latest_file,
            filesystem=self.filesystem,
            partitioning="hive",
            format="parquet",
        ).schema

        return pyarrow_dataset(
            source=list(self.files),
            filesystem=self.filesystem,
            partitioning="hive",
            format="parquet",
            schema=schema,
        )

    @property
    def schema(self):
        return self.pyarrow_dataset.schema

    def _is_delta_table(self):
        return self.filesystem.exists(f"{self.log_path}/{0:020}.json")

    def _reset_state(self):
        self.files = set()
        self._latest_file = ""
        self._latest_file_time = 0

    def _apply_from_checkpoint(self, checkpoint_version: int):

        # reset file set, and checkpoint version
        self._reset_state()
        self.checkpoint = checkpoint_version

        if self.checkpoint == 0:
            return

        # read latest checkpoint
        with self.filesystem.open(
            f"{self.log_path}/{self.checkpoint:020}.checkpoint.parquet"
        ) as checkpoint_file:
            checkpoint = pq.read_table(checkpoint_file).to_pandas()

            for i, row in checkpoint.iterrows():
                if not row["add"]:
                    continue
                added_file = f"{self.path}/{row['add']['path']}"
                if added_file:
                    self.files.add(added_file)

                modificationTime = row["add"]["modificationTime"]
                if modificationTime > self._latest_file_time:
                    self._latest_file = added_file
                    self._latest_file_time = modificationTime

    def _apply_partial_logs(self, version: int):
        # Checkpoints are created every 10 transactions,
        # so we need to find all log files with version
        # up to 9 higher than checkpoint.
        # Effectively, this means that we can just create a
        # wild card for the first decimal of the checkpoint version

        log_files = self.filesystem.glob(
            f"{self.log_path}/{self.checkpoint//10:019}*.json"
        )
        # sort the log files, so we are sure we get the correct order
        log_files = sorted(log_files)
        for log_file in log_files:

            # Get version from log name
            log_version = re.findall(r"(\d{20})", log_file)[0]
            self.version = int(log_version)

            # Download log file
            with self.filesystem.open(log_file) as log:
                for line in log:
                    meta_data = json.loads(line)
                    # Log contains other stuff, but we are only
                    # interested in the add or remove entries
                    if "add" in meta_data.keys():
                        file = f"{self.path}/{meta_data['add']['path']}"
                        self.files.add(file)

                        self._latest_file_time = meta_data["add"]["modificationTime"]
                        self._latest_file = file

                    if "remove" in meta_data.keys():
                        remove_file = f"{self.path}/{meta_data['remove']['path']}"
                        # To handle 0 checkpoints, we might read the log file with
                        # same version as checkpoint. this means that we try to
                        # remove a file that belongs to an ealier version,
                        # which we don't have in the list
                        if remove_file in self.files:
                            self.files.remove(remove_file)
                # Stop if we have reatched the desired version
                if self.version == version:
                    break

    def _as_newest_version(self):
        # Try to get the latest checkpoint info
        try:
            # get latest checkpoint version
            with self.filesystem.open(f"{self.log_path}/_last_checkpoint") as lst_check:
                checkpoint_info = lst_check.read()
            checkpoint_info = json.loads(checkpoint_info)
            self._apply_from_checkpoint(checkpoint_info["version"])

        except FileNotFoundError:
            pass

        # apply remaining versions. This can be a maximum of 9 versions.
        # we will just break when we don't find any newer logs
        self._apply_partial_logs(version=self.checkpoint + 9)

    def to_table(self, *args, **kwargs):
        """
        Convert to a pyarrow Table.
        Is based on the `to_pandas` function from `pyarrow.Table.to_pandas`,
        so any this will accept the same arguments.
        For more information see https://arrow.apache.org/docs/python/generated/pyarrow.dataset.FileSystemDataset.html#pyarrow.dataset.FileSystemDataset.to_table
        """  # noqa E501
        return self.pyarrow_dataset.to_table(*args, **kwargs)

    def to_pandas(self, *args, **kwargs):
        """
        Convert to a pandas dataframe.
        Is based on the `to_pandas` function from `pyarrow.Table.to_pandas`,
        so any this will accept the same arguments.
        For more information see https://arrow.apache.org/docs/python/generated/pyarrow.Table.html?highlight=to_pandas#pyarrow.Table.to_pandas
        """  # noqa E501
        return self.to_table().to_pandas(*args, **kwargs)

    def as_version(self, version: int, inplace=True):
        """
        Find the files for a specific version of the table.

        Parameters:
        ----------
        version: int
            The table version number that should be loaded

        inplace: Bool
            Specify wether the object should be modified inplace or not.
            If `True`, the current object will be modified.
            if `False`, a new instance of the `DeltaTable` will be returned with the given version.

        Returns:
        -------
        dr : (DeltaTable)
            Delta table that has parsed the log files for the specific version
        """
        nearest_checkpoint = version // 10 * 10
        if inplace:
            self._apply_from_checkpoint(nearest_checkpoint)
            self._apply_partial_logs(version=version)
            self.pyarrow_dataset = self._pyarrow_dataset()
            return self

        deltaTable = deepcopy(self)
        deltaTable._apply_from_checkpoint(nearest_checkpoint)
        deltaTable._apply_partial_logs(version=version)
        deltaTable.pyarrow_dataset = deltaTable._pyarrow_dataset()

        return deltaTable
