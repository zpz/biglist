import bisect
import collections.abc
import itertools
import logging
import os
import queue
import random
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Union, Sequence

from pyarrow.parquet import ParquetFile
from pyarrow.fs import FileSystem
from upathlib import Upath, LocalUpath, PathType, resolve_path
from ._base import BiglistBase, FileLoaderMode

# If data is in Google Cloud Storage, `pyarrow.fs.GcsFileSystem` accepts "access_token"
# and "credential_token_expiration". These can be obtained via
# a "google.oauth2.service_account.Credentials" object, e.g.
#
#   cred = google.oauth2.service_account.Credentials.from_service_info(
#       info_json, scopes=['https://www.googleapis.com/auth/cloud-platform'])
# or
#   cred = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
#
#   auth_req = google.auth.transport.requests.Request()
#   cred.refresh(auth_req)
#   # now `cred` has `token` and `expiry`; expiration appears to be in a few hours
#
#   gcs = pyarrow.fs.GcsFileSystem(access_token=cred.token, credential_token_expiration=cred.expiry)
#   pfile = pyarrow.parquet.ParquetFile(gcs.open_input_file('bucket-name/path/to/file.parquet'))


logger = logging.getLogger(__name__)


class ParquetBiglist(BiglistBase):
    """
    `ParquetBiglist` defines a kind of "external Biglist", that is,
    it points to pre-existing Parquet files (produced by other code)
    and provides facilities to read the data.

    Data files are read in arbitrary order determined by this class,
    that is, the data sequence produced will most likely differ from
    what is read out by other Parquet utilities. However, if you use
    a saved `ParquetBiglist` object again for reading, it will read
    in the same order.

    As long as you use a `ParquetBiglist` object to read, it is assumed
    the dataset has not changed since the creation of the object.
    """

    @classmethod
    def new(
        cls,
        data_path: Union[PathType, Sequence[PathType]],
        path: PathType = None,
        *,
        suffix: str = ".parquet",
        keep_files: bool = None,
        shuffle: bool = False,
        thread_pool_executor: ThreadPoolExecutor = None,
        **kwargs,
    ):
        """
        `data_path`: Parquet file(s) for folder(s) containing Parquet files;
            folders are traversed recursively. The data files can represent a mix
            of locations, including a mix of local and cloud locations, as long
            as they don't change. However, if any data file is on the local disk,
            you're tied to the particular machine for the use of the `ParquetBiglist`
            object.

        This classmethod gathers info of the data files and saves it to facilitate
        reading the data.

        If the number of data files is small, it's entirely feasible to create a temporary
        object of this class (by leaving `path` at the default `None`) "on-the-fly"
        for one-time use.
        """
        if (
            isinstance(data_path, str)
            or isinstance(data_path, Path)
            or isinstance(data_path, Upath)
        ):
            #  TODO: in py 3.10, we will be able to do `isinstance(data_path, PathType)`
            data_path = [resolve_path(data_path)]
        else:
            data_path = [resolve_path(p) for p in data_path]

        if not path:
            path = cls.get_temp_path()
            if keep_files is None:
                keep_files = False
        else:
            path = resolve_path(path)
            if keep_files is None:
                keep_files = True
        if path.is_dir():
            raise Exception(f'directory "{path}" already exists')
        elif path.is_file():
            raise FileExistsError(path)

        q_datafiles = queue.Queue()

        def get_file_meta(f, p, q):
            meta = f(p).metadata
            q.put(
                {
                    "path": str(p),  # because Upath object does not support JSON
                    "num_rows": meta.num_rows,
                    "row_groups_num_rows": [
                        meta.row_group(k).num_rows for k in range(meta.num_row_groups)
                    ],
                }
            )

        if thread_pool_executor is not None:
            pool = thread_pool_executor
        else:
            pool = ThreadPoolExecutor(min(32, (os.cpu_count() or 1) + 4))
        tasks = []
        read_parquet = cls.read_parquet_file
        for p in data_path:
            if p.is_file():
                if suffix == "*" or p.name.endswith(suffix):
                    tasks.append(
                        pool.submit(get_file_meta, read_parquet, p, q_datafiles)
                    )
            else:
                for q in p.riterdir():
                    if suffix == "*" or q.name.endswith(suffix):
                        tasks.append(
                            pool.submit(get_file_meta, read_parquet, q, q_datafiles)
                        )
        assert tasks
        for k, t in enumerate(tasks):
            _ = t.result()
            if (k + 1) % 1000 == 0:
                logger.info("processed %d files", k + 1)

        if thread_pool_executor is None:
            pool.shutdown()

        datafiles = []
        for _ in range(q_datafiles.qsize()):
            datafiles.append(q_datafiles.get())
        if shuffle:
            random.shuffle(datafiles)

        datafiles_cumlength = list(
            itertools.accumulate(v["num_rows"] for v in datafiles)
        )

        obj = cls(path, require_exists=False, thread_pool_executor=thread_pool_executor, **kwargs)  # type: ignore
        obj.keep_files = keep_files
        obj.info["datapath"] = [str(p) for p in data_path]
        obj.info["datafiles"] = datafiles
        obj.info["datafiles_cumlength"] = datafiles_cumlength
        obj.info["storage_format"] = "parquet"
        obj._info_file.write_json(obj.info)

        return obj

    def __repr__(self):
        return f"<{self.__class__.__name__} at '{self.path}' with {len(self)} records in {self.num_datafiles} data file(s) stored at {self.info['datapath']}>"

    @classmethod
    def read_parquet_file(cls, path: Upath):
        if isinstance(path, LocalUpath):
            return ParquetFile(str(path))
        else:
            # Work around a pyarrow 10.0.0 bug:
            #   ParquetFile does not recognize str cloud path
            # Use may customize this function to pass in cloud
            # credentials to `FileSystem` so that it does not
            # fall back to default ways of finding credentials.
            ff, pp = FileSystem.from_uri(str(path))
            return ParquetFile(ff.open_input_file(pp))

    @classmethod
    def load_data_file(cls, path: Upath, mode: int):
        # This method could be useful by itself. User may want
        # to make a free-standing function as a trivial wrapper of this.
        return ParquetFileData(cls.read_parquet_file(path), mode=mode)

    def get_data_files(self):
        return self.info["datafiles"], self.info["datafiles_cumlength"]

    def get_data_file(self, datafiles, idx):
        return resolve_path(datafiles[idx]["path"])

    def iter_batches(self, batch_size=10000):
        # Yield native Apache Arrow objects for experiments.
        datafiles, _ = self.get_data_files()
        for ifile in range(len(datafiles)):
            filedata = self.load_data_file(
                self.get_data_file(datafiles, ifile), FileLoaderMode.RAND
            )
            yield from filedata.file.iter_batches(batch_size)


class ParquetFileData(collections.abc.Sequence):
    # Represents data of a single Parquet file,
    # with facilities to make it conform to our required APIs.
    #
    # If you want to use the  `arrow.parquet` methods directly,
    # use it via `self.file`, or `self.table` if not None.

    def __init__(
        self,
        file: ParquetFile,
        *,
        mode: int = FileLoaderMode.RAND,
    ):
        self.file = file

        meta = file.metadata
        self.metadata = meta
        self.num_columns = meta.num_columns
        self.num_rows = meta.num_rows
        self.num_row_groups = meta.num_row_groups
        self._row_groups_num_rows = None
        self._row_groups_num_rows_cumsum = None
        self._row_groups = None

        self._columns = None

        self._data = None
        if mode == FileLoaderMode.ITER:
            _ = self.data

    def select_columns(self, cols: Union[str, Sequence[str]]):
        # TODO: use `None` to re-select all?
        if isinstance(cols, str):
            cols = [cols]
        assert len(set(cols)) == len(cols)  # no repeat values

        if self._columns:
            if all(col in self._columns for col in cols):
                if len(cols) == len(self._columns):
                    return self
                if self._data is not None:
                    self._data = self._data.select(cols)
                if self._row_groups is not None:
                    self._row_groups = [
                        None if v is None else v.select(cols) for v in self._row_groups
                    ]
            else:
                # Usually you should not get it in this situation.
                # Warn?
                self._data = None
                self._row_groups = None
        else:
            if self._data is not None:
                self._data = self._data.select(cols)
            if self._row_groups is not None:
                self._row_groups = [
                    None if v is None else v.select(cols) for v in self._row_groups
                ]

        self._columns = cols
        self.num_columns = len(cols)
        return self

    @property
    def data(self):
        if self._data is None:
            self._data = self.file.read(columns=self._columns)
        return self._data

    def __len__(self):
        return self.num_rows

    def __getitem__(self, idx: int):
        # Get one row.
        # If data has a single column, return the value on the specified row.
        # The return is an "arrow scalar" type such as `pyarrow.lib.StringScalar`.
        # If data has multiple columns, return a dict with keys being column names
        # and values being "arrow scalar" types.
        if idx < 0:
            idx = self.num_rows + idx
        if idx < 0 or idx >= self.num_rows:
            raise IndexError(idx)

        if self._data is not None:
            if self._data.num_columns == 1:
                return self._data.column(0)[idx]
            else:
                return {
                    col: self._data.column(col)[idx] for col in self._data.column_names
                }

        if self._row_groups_num_rows is None:
            meta = self.metadata
            self._row_groups_num_rows = [
                meta.row_group(i).num_rows for i in range(self.num_row_groups)
            ]
            self._row_groups_num_rows_cumsum = list(
                itertools.accumulate(self._row_groups_num_rows)
            )

        if self._row_groups is None:
            self._row_groups = [None] * self.num_row_groups

        igrp = bisect.bisect_right(self._row_groups_num_rows_cumsum, idx)
        if self._row_groups[igrp] is None:
            self._row_groups[igrp] = self.file.read_row_group(
                igrp, columns=self._columns
            )
        if igrp == 0:
            idx_in_row_group = idx
        else:
            idx_in_row_group = idx - self._row_groups_num_rows_cumsum[igrp - 1]
        row_group = self._row_groups[igrp]
        if self.num_columns == 1:
            return row_group.column(0)[idx_in_row_group]

        else:
            return {
                col: row_group.column(col)[idx_in_row_group]
                for col in row_group.column_names
            }

    def __iter__(self):
        if self._data is None:
            for batch in self.file.iter_batches(columns=self._columns):
                if batch.num_columns == 1:
                    yield from batch.column(0)
                else:
                    names = batch.schema.names
                    for row in zip(*batch.columns):
                        yield dict(zip(names, row))
        else:
            if self._data.num_columns == 1:
                yield from self._data.column(0)
            else:
                names = self._data.column_names
                for row in zip(*self._data.columns):
                    yield dict(zip(names, row))

    def iter_batches(self, *, batch_size=None):
        if self._data is None:
            yield from self.file.iter_batches(
                batch_size=batch_size, columns=self._columns
            )
        else:
            yield from self._data.to_batches(batch_size)
