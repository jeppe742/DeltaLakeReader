import json
from copy import deepcopy
from os import path

import pyarrow.parquet as pq

from deltalake.base import BaseDeltaReader


class LocalDeltaReader(BaseDeltaReader):
    def __init__(self, path: str):
        super(LocalDeltaReader, self).__init__(path)

    def _parse_path(self):
        # just remove file prefix, since we don't need this
        if "file://" in self.path:
            self.path = self.path.replace("file://", "")

    def _is_delta_table(self):
        return path.exists(self.log_path)

    def _apply_from_checkpoint(self, checkpoint_version: int):

        # reset file set, and checkpoint version
        self.files = set()
        self.checkpoint = checkpoint_version

        if self.checkpoint == 0:
            return

        checkpoint = pq.read_table(
            f"{self.log_path}/{self.checkpoint:020}.checkpoint.parquet"
        ).to_pandas()

        for i, row in checkpoint.iterrows():
            added_file = row["add"]["path"] if row["add"] else None
            if added_file:
                self.files.add(f"{self.path}/{added_file}")

    def _apply_partial_logs(self, version: int):
        for i in range(version - self.checkpoint + 1):
            try:
                with open(f"{self.log_path}/{self.checkpoint+i:020}.json", "r") as f:
                    self.version = self.checkpoint + i
                    for line in f:
                        meta_data = json.loads(line)
                        # Log contains other stuff, but we are only
                        # interested in the add or remove entries
                        if "add" in meta_data.keys():
                            self.files.add(f"{self.path}/{meta_data['add']['path']}")
                        if "remove" in meta_data.keys():
                            remove_file = f"{self.path}/{meta_data['remove']['path']}"
                            # To handle 0 checkpoints, we might read the log file with
                            # same version as checkpoint. this means that we try to
                            # remove a file that belongs to an ealier version,
                            # which we don't have in the list
                            if remove_file in self.files:
                                self.files.remove(remove_file)

            # If the file isn't found it should be because we have reatched
            # the newest log file in last iteration
            except FileNotFoundError:
                break

    def _as_newest_version(self):
        # Check if we have any checkpoints before reading any files
        if path.exists(f"{self.log_path}/_last_checkpoint"):
            with open(f"{self.log_path}/_last_checkpoint", "r") as f:
                checkpoint_info = json.load(f)
                checkpoint = checkpoint_info["version"]
                # apply versions from checkpoint
                self._apply_from_checkpoint(checkpoint)

        # apply remaining versions. This can be a maximum of 9 versions.
        # we will just break when we don't find any newer logs
        self._apply_partial_logs(version=self.checkpoint + 9)

    def to_pyarrow(self, columns=None):
        return pq.ParquetDataset(list(self.files)).read_pandas(columns=columns)

    def as_version(self, version: int):
        nearest_checkpoint = version // 10

        deltaReader = deepcopy(self)
        deltaReader._apply_from_checkpoint(nearest_checkpoint)
        deltaReader._apply_partial_logs(version=version)

        return deltaReader
