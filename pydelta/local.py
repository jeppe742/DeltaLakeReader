import json
from os import path

import pyarrow.parquet as pq

from pydelta.reader import DeltaReader


class LocalDeltaReader(DeltaReader):
    def __init__(self, path):
        super(LocalDeltaReader, self).__init__(path)

    def _is_delta_table(self):
        return path.exists(self.log_path)

    def _get_files(self):
        # Check if we have any checkpoints before reading any files
        if path.exists(f"{self.log_path}/_last_checkpoint"):
            with open(f"{self.log_path}/_last_checkpoint", "r") as f:
                checkpoint_info = json.load(f)
                self.latest_checkpoint = checkpoint_info["version"]
            # Get the files from the checkpoint first
            checkpoint = pq.read_table(
                f"{self.log_path}/{self.latest_checkpoint:020}.checkpoint.parquet"
            ).to_pandas()

            for i, row in checkpoint.iterrows():
                added_file = row["add"]["path"] if row["add"] else None
                if added_file:
                    self.parquet_files.add(f"{self.path}/{added_file}")

        # look at the meta data between newest checkpoint and newest log file.
        # We know that the files are named sequentially,
        # so we can make educated guesses instead of reading all file names.

        # there should maximum be 10 log files between each checkpoint
        for i in range(10):
            try:
                with open(
                    f"{self.log_path}/{self.latest_checkpoint+i:020}.json", "r"
                ) as f:
                    self.latest_version = self.latest_checkpoint + i
                    for line in f:
                        meta_data = json.loads(line)
                        # Log contains other stuff, but we are only
                        # interested in the add or remove entries
                        if "add" in meta_data.keys():
                            self.parquet_files.add(
                                f"{self.path}/{meta_data['add']['path']}"
                            )
                        if "remove" in meta_data.keys():
                            remove_file = f"{self.path}/{meta_data['remove']['path']}"
                            # To handle 0 checkpoints, we might read the log file with
                            # same version as checkpoint. this means that we try to
                            # remove a file that belongs to an ealier version,
                            # which we don't have in the list
                            if remove_file in self.parquet_files:
                                self.parquet_files.remove(remove_file)

            # If the file isn't found it should be because we have reatched
            # the newest log file in last iteration
            except FileNotFoundError:
                break

    def to_pyarrow(self, columns=None):
        return pq.ParquetDataset(list(self.parquet_files)).read_pandas(columns=columns)
