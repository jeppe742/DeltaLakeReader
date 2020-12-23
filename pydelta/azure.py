import json
import re
from io import BytesIO

import pyarrow
import pyarrow.parquet as pq
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import ContainerClient

from pydelta.reader import DeltaReader


class AzureDeltaReader(DeltaReader):
    def __init__(self, path, credential=None):
        super(AzureDeltaReader, self).__init__(path, credential)

    def _parse_path(self):
        if "dfs.core.windows.net" in self.path:
            raise ValueError(
                "use blob.core.windows.net instead of dfs.core.windows.net"
            )

        if "https://" in self.path:
            m = re.search("https://(.+?).blob.core.windows.net/(.+?)/(.+)", self.path)
            self.account_name = m.group(1)
            self.container_name = m.group(2)
            self.folder = m.group(3)

        elif "abfss://" in self.path:
            m = re.search("abfss://(.+?)@(.+?).blob.core.windows.net/(.+)", self.path)
            self.container_name = m.group(1)
            self.account_name = m.group(2)
            self.folder = m.group(3)

    def _authenticate(self):
        self._parse_path()
        self.container_client = ContainerClient(
            account_url=f"https://{self.account_name}.blob.core.windows.net",
            container_name=self.container_name,
            credential=self.credential,
        )

    def _download_blob(self, file):
        bc = self.container_client.get_blob_client(blob=file)
        return bc.download_blob()

    def _is_delta_table(self):
        blobs = self.container_client.list_blobs(
            name_starts_with=f"{self.folder}/_delta_log/{0:020}.json"
        )
        try:
            blobs.next()
            return True

        except StopIteration:
            return False

    def _get_files(self):
        # Try to get the latest checkpoint info
        try:
            # get latest checkpoint version
            checkpoint_info_blob = self._download_blob(
                file=f"{self.folder}/_delta_log/_last_checkpoint"
            )
            checkpoint_info = json.loads(checkpoint_info_blob.readall())
            self.latest_checkpoint = checkpoint_info["version"]

            # read latest checkpoint
            checkpoint_blob = self._download_blob(
                f"{self.folder}/_delta_log/{self.latest_checkpoint:020}.checkpoint.parquet"
            )
            # Read as stream to minimize overhead
            with BytesIO() as checkpoint_stream:
                checkpoint_blob.download_to_stream(checkpoint_stream)
                # Convert stream to pandas table using pyarrow
                checkpoint = pq.read_table(checkpoint_stream).to_pandas()

                for i, row in checkpoint.iterrows():
                    added_file = row["add"]["path"] if row["add"] else None
                    if added_file:
                        self.parquet_files.add(f"{self.path}/{added_file}")

        except ResourceNotFoundError:
            pass

        # Checkpoints are created every 10 transactions,
        # so we need to find all log files with version
        # up to 9 higher than checkpoint.
        # Effectively, this means that we can just create a
        # wild card for the first decimal of the checkpoint version

        log_files = self.container_client.list_blobs(
            name_starts_with=f"{self.folder}/_delta_log/{self.latest_checkpoint//10:019}"
        )
        # sort the log files, so we are sure we get the correct order
        log_files = sorted(log_files, key=lambda log: log.name)
        for log_file in log_files:
            # skip checkpoint files
            if ".json" in log_file.name:
                log = self._download_blob(log_file).readall()
                for line in log.split():
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

        # set latest version.
        # We can get this from the name of the last log file
        m = re.search(r"(\d{20})", log_files[-1].name)
        self.latest_version = int(m.group(1))

    def to_pyarrow(self, columns=None):
        tables = []
        for file in self.parquet_files:

            with BytesIO() as stream:
                # File contains full path, which we cannot use with the container client.
                # If you create a blob client for each blob, it will introduce a lot of overhead
                blob = self._download_blob(
                    file.replace(
                        f"https://{self.account_name}.blob.core.windows.net/{self.container_name}/",
                        "",
                    )
                )
                blob.download_to_stream(stream)
                tables.append(pq.read_table(stream, columns=columns))

        return pyarrow.concat_tables(tables)
