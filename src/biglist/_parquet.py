import bisect
import collections.abc
import itertools
import logging
import os
import queue
import random
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Union, Sequence, Iterator, List

import pyarrow
from pyarrow.parquet import ParquetFile
from pyarrow.fs import FileSystem, GcsFileSystem
from upathlib import Upath
from ._base import BiglistBase, FileLoaderMode, PathType

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

    Data files are read in the order of the string value of their paths.
    This order could be different from what another Parquet reader would use.
    The parameter `shuffle` (default `False`) controls whether to
    randomize this file order.

    As long as you use a `ParquetBiglist` object to read, it is assumed
    the dataset has not changed since the creation of the object.
    """

    @classmethod
    def get_gcsfs(cls, *, good_for_hours=1):
        # Import here b/c user may not be on GCP
        from datetime import datetime
        import google.auth

        cred = getattr(cls, "_GCP_CREDENTIALS", None)
        if cred is None:
            cred, _ = google.auth.default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            cls._GCP_CREDENTIALS = cred
        if (
            not cred.token
            or (cred.expiry - datetime.now()).total_seconds() < good_for_hours * 3600
        ):
            cred.refresh(google.auth.transport.requests.Request())
        return GcsFileSystem(
            access_token=cred.token, credential_token_expiration=cred.expiry
        )

    @classmethod
    def read_parquet_file(cls, path: str):
        ff, pp = FileSystem.from_uri(path)
        if isinstance(ff, GcsFileSystem):
            ff = cls.get_gcsfs()
        return ParquetFile(ff.open_input_file(pp))

    @classmethod
    def load_data_file(cls, path: Upath, mode: int):
        # This method or `read_parquet_file` could be useful by themselves.
        # User may want to make free-standing functions as trivial wrappers of them.
        return ParquetFileData(
            cls.read_parquet_file(str(path)), eager_load=(mode == FileLoaderMode.ITER)
        )

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
            data_path = [cls.resolve_path(data_path)]
        else:
            data_path = [cls.resolve_path(p) for p in data_path]

        if not path:
            path = cls.get_temp_path()
            if keep_files is None:
                keep_files = False
        else:
            path = cls.resolve_path(path)
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
                    "path": p,
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
                        pool.submit(get_file_meta, read_parquet, str(p), q_datafiles)
                    )
            else:
                for pp in p.riterdir():
                    if suffix == "*" or pp.name.endswith(suffix):
                        tasks.append(
                            pool.submit(
                                get_file_meta, read_parquet, str(pp), q_datafiles
                            )
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
        else:
            datafiles.sort(key=lambda x: x["path"])

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

    def _get_data_files(self):
        return self.info["datafiles"], self.info["datafiles_cumlength"]

    def _get_data_file(self, datafiles, idx):
        return self.resolve_path(datafiles[idx]["path"])

    def iter_batches(self, batch_size=None):
        for file in self.iter_files():
            yield from file.iter_batches(batch_size)

    @property
    def datafiles(self):
        return [f["path"] for f in self.info["datafiles"]]


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
        eager_load: bool = False,
        scalar_as_py: bool = True,
    ):
        self.file = file

        meta = file.metadata
        self.metadata = meta
        self.num_rows = meta.num_rows
        self.num_row_groups = meta.num_row_groups
        self._row_groups_num_rows = None
        self._row_groups_num_rows_cumsum = None
        self._row_groups: List[pyarrow.Table] = None

        self._column_names = None
        self._scalar_as_py = scalar_as_py

        self._data: pyarrow.Table = None
        if eager_load:
            _ = self.data

        self._getitem_last_row_group = None

    def __repr__(self):
        return "<{} with {} rows, {} columns, {} row-groups>".format(
            self.__class__.__name__,
            self.num_rows,
            self.num_columns,
            self.num_row_groups,
        )

    def __str__(self):
        return self.__repr__()

    @property
    def num_columns(self) -> int:
        if self._column_names:
            return len(self._column_names)
        return self.metadata.num_columns

    @property
    def column_names(self) -> List[str]:
        if self._column_names:
            return self._column_names
        return self.metadata.schema.names

    def select_columns(self, cols: Union[str, Sequence[str]]):
        """
        Specify the columns to downnload. Usually this is called only once,
        and early on in the life of the object; or narrow a previous selection.
        Expanding a previous selection is supported, but that may trigger
        data reload under the hood.

        Examples:

            obj = ParquetFileData('file_path').select_columns(['a', 'b', 'c'])
            print(obj[2])
            obj.select_columns(['b', 'c'])
            print(obj[3])
            obj.select_columns('b')
            for v in obj:
                print(v)
        """
        # TODO: use `None` to re-select all?
        if isinstance(cols, str):
            cols = [cols]
        assert len(set(cols)) == len(cols)  # no repeat values

        if self._column_names:
            if all(col in self._column_names for col in cols):
                if len(cols) == len(self._column_names):
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

        self._column_names = cols
        return self

    @property
    def data(self) -> pyarrow.Table:
        """Eagerly read in the whole file as a table."""
        if self._data is None:
            self._data = self.file.read(columns=self._column_names)
        return self._data

    def __len__(self):
        return self.num_rows

    def __getitem__(self, idx: int):
        """
        Get one record or row.

        `idx`: row index in this file.

        If `self._scalar_as_py` is False,
          If data has a single column, return the value on the specified row.
          The return is a `pyarrow.Scalar`` type such as `pyarrow.lib.StringScalar`.
          If data has multiple columns, return a dict with keys being column names
          and values being `pyarrow.Scalar`` types.
        If `self._scalar_as_py` if True,
          the `pyarrow.Scalar` values are converted to Python native types.
        """
        if idx < 0:
            idx = self.num_rows + idx
        if idx < 0 or idx >= self.num_rows:
            raise IndexError(idx)

        if self._data is not None:
            if self._data.num_columns == 1:
                z = self._data.column(0)[idx]
                if self._scalar_as_py:
                    return z.as_py()
                return z
            else:
                z = {
                    col: self._data.column(col)[idx] for col in self._data.column_names
                }
                if self._scalar_as_py:
                    return {k: v.as_py() for k, v in z.items()}
                return z

        if self._row_groups_num_rows is None:
            meta = self.metadata
            self._row_groups_num_rows = [
                meta.row_group(i).num_rows for i in range(self.num_row_groups)
            ]
            self._row_groups_num_rows_cumsum = list(
                itertools.accumulate(self._row_groups_num_rows)
            )

        # Assuming user is checking neighboring items,
        # then the requested item may be in the same row-group
        # as the item requested last time.
        igrp = None
        if (last_group := self._getitem_last_row_group) is not None:
            if last_group[1] <= idx < last_group[2]:
                igrp = last_group[0]
        if igrp is None:
            igrp = bisect.bisect_right(self._row_groups_num_rows_cumsum, idx)
            if igrp == 0:
                self._getitem_last_row_group = (
                    0,  # row-group index
                    0,  # item index lower bound
                    self._row_groups_num_rows_cumsum[0],  # item index upper bound
                )
            else:
                self._getitem_last_row_group = (
                    igrp,
                    self._row_groups_num_rows_cumsum[igrp - 1],
                    self._row_groups_num_rows_cumsum[igrp],
                )

        if self._row_groups is None:
            self._row_groups = [None] * self.num_row_groups

        if self._row_groups[igrp] is None:
            self._row_groups[igrp] = self.file.read_row_group(
                igrp, columns=self._column_names
            )
        row_group = self._row_groups[igrp]
        if igrp == 0:
            idx_in_row_group = idx
        else:
            idx_in_row_group = idx - self._row_groups_num_rows_cumsum[igrp - 1]
        if row_group.num_columns == 1:
            z = row_group.column(0)[idx_in_row_group]
            if self._scalar_as_py:
                return z.as_py()
            else:
                return z
        else:
            z = {
                col: row_group.column(col)[idx_in_row_group]
                for col in row_group.column_names
            }
            if self._scalar_as_py:
                return {k: v.as_py() for k, v in z.items()}
            return z

    def __iter__(self):
        # Type of yielded individual elements is the same as `__getitem__`.
        if self._data is None:
            for batch in self.file.iter_batches(columns=self._column_names):
                if batch.num_columns == 1:
                    if self._scalar_as_py:
                        yield from (v.as_py() for v in batch.column(0))
                    else:
                        yield from batch.column(0)
                else:
                    names = batch.schema.names
                    if self._scalar_as_py:
                        for row in zip(*batch.columns):
                            yield dict(zip(names, (v.as_py() for v in row)))
                    else:
                        for row in zip(*batch.columns):
                            yield dict(zip(names, row))
        else:
            if self._data.num_columns == 1:
                if self._scalar_as_py:
                    yield from (v.as_py() for v in self._data.column(0))
                else:
                    yield from self._data.column(0)
            else:
                names = self._data.column_names
                if self._scalar_as_py:
                    for row in zip(*self._data.columns):
                        yield dict(zip(names, (v.as_py() for v in row)))
                else:
                    for row in zip(*self._data.columns):
                        yield dict(zip(names, row))

    def iter_batches(self, batch_size=None) -> Iterator[pyarrow.RecordBatch]:
        """
        User often wants to specify `batch_size` b/c the default
        may be too large.
        """
        if self._data is None:
            yield from self.file.iter_batches(
                batch_size=batch_size, columns=self._column_names
            )
        else:
            yield from self._data.to_batches(batch_size)

    def row_group(self, idx: int) -> pyarrow.Table:
        assert 0 <= idx < self.num_row_groups
        if self._row_groups is None:
            self._row_groups = [None] * self.num_row_groups
        if self._row_groups[idx] is None:
            self._row_groups[idx] = self.file.read_row_group(
                idx, columns=self._column_names
            )
        return self._row_groups[idx]
