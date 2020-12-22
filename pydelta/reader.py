import json
from os import path

import pandas as pd


class DeltaReader:
    def __init__(self, path):
        self.path = path
        self.log_path = f"{self.path}/_delta_log"
        self.latest_version = 0
        self.latest_checkpoint = 0
        self.parquet_files = set()

        if not self._is_delta_table():
            raise ValueError(f"No delta table found in {self.path}")

    def _is_delta_table(self):
        return path.exists(self.log_path)

    def _get_files(self):
        # Check if we have any checkpoints before reading any files
        if path.exists(f"{self.log_path}/_last_checkpoint"):
            with open(f"{self.log_path}/_last_checkpoint", "r") as f:
                checkpoint_info = json.load(f)
                self.latest_checkpoint = checkpoint_info["version"]
            # Get the files from the checkpoint first
            checkpoint = pd.read_parquet(
                f"{self.log_path}/{self.latest_checkpoint:020}.checkpoint.parquet"
            )
            for i, row in checkpoint.iterrows():
                added_file = row["add"]["path"] if row["add"] else None
                removed_file = row["remove"]["path"] if row["remove"] else None
                if added_file:
                    self.parquet_files.add(f"{self.path}/{added_file}")
                if removed_file:
                    self.parquet_files.remove(f"{self.path}/{removed_file}")
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
                            self.parquet_files.remove(
                                f"{self.path}/{meta_data['remove']['path']}"
                            )

            # If the file isn't found it should be because we have reatched
            # the newest log file in last iteration
            except FileNotFoundError:
                break
