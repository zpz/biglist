from __future__ import annotations

import collections.abc
import itertools
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Union, Sequence, Iterator, List, Iterable

import pyarrow
from pyarrow.parquet import ParquetFile
from pyarrow.fs import FileSystem, GcsFileSystem
from upathlib import Upath, LocalUpath
from ._base import BiglistBase, FileLoaderMode, PathType, ListView
from ._util import locate_idx_in_chunked_seq


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

    About the order of file reading: the order of the entries in the input
    `data_path` to `ParquetBiglist.new` is preserved; if any entry is a
    directory, the files therein (recursively) are sorted by the string
    value of each file path.

    As long as you use a `ParquetBiglist` object to read, it is assumed that
    the dataset has not changed since the creation of the object.
    """

    @classmethod
    def get_gcsfs(cls, *, good_for_minutes=30):
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
            or (cred.expiry - datetime.now()).total_seconds() < good_for_minutes * 60
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

        `path`: folder to be used by this object is save whatever it needs to persist.
            Id `None`, a temporary location will be used.

        `suffix`: files with this suffix in directories specified in `data_path`
            will be included. To include all files, use `suffix = '*'`.

        `keep_files`: should files persisted by the current object (in `path`)
            be deleted when this object is garbage collected?
            By default, this is `True` if `path` is specified, and `False` otherwise.

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

        def get_file_meta(f, p: str):
            meta = f(p).metadata
            return {
                "path": p,
                "num_rows": meta.num_rows,
                "row_groups_num_rows": [
                    meta.row_group(k).num_rows for k in range(meta.num_row_groups)
                ],
            }

        if thread_pool_executor is not None:
            pool = thread_pool_executor
        else:
            pool = ThreadPoolExecutor(min(32, (os.cpu_count() or 1) + 4))
        tasks = []
        read_parquet = cls.read_parquet_file
        for p in data_path:
            if p.is_file():
                if suffix == "*" or p.name.endswith(suffix):
                    tasks.append(pool.submit(get_file_meta, read_parquet, str(p)))
            else:
                tt = []
                for pp in p.riterdir():
                    if suffix == "*" or pp.name.endswith(suffix):
                        tt.append(
                            (str(pp), pool.submit(get_file_meta, read_parquet, str(pp)))
                        )
                tt.sort()
                for p, t in tt:
                    tasks.append(t)

        assert tasks
        datafiles = []
        for k, t in enumerate(tasks):
            datafiles.append(t.result())
            if (k + 1) % 1000 == 0:
                logger.info("processed %d files", k + 1)

        if thread_pool_executor is None:
            pool.shutdown()

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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.keep_files = True

    def __del__(self) -> None:
        if not self.keep_files:
            self.path.rmrf()

    def __repr__(self):
        return f"<{self.__class__.__name__} at '{self.path}' with {len(self)} records in {self.num_datafiles} data file(s) stored at {self.info['datapath']}>"

    def _get_data_files(self):
        return self.info["datafiles"], self.info["datafiles_cumlength"]

    def _get_data_file(self, datafiles, idx):
        return self.resolve_path(datafiles[idx]["path"])

    def iter_batches(self, batch_size=10_000) -> Iterator[ParquetBatchData]:
        for file in self.iter_files():
            yield from file.iter_batches(batch_size)

    @property
    def datafiles(self) -> List[str]:
        return [f["path"] for f in self.info["datafiles"]]


class ParquetFileData(collections.abc.Sequence):
    """
    Represents data of a single Parquet file,
    with facilities to make it conform to our required APIs.

    If you want to use the  `arrow.parquet` methods directly,
    use it via `self.file`, or `self.data()`.

    Objects of this class can't be pickled.
    """

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
        self._row_groups: List[ParquetBatchData] = None

        self._column_names = None
        self._columns = {}
        self.scalar_as_py = scalar_as_py
        # `self.scalar_as_py` may be toggled anytime
        # and have its effect right away.

        self._data: ParquetBatchData = None
        if eager_load:
            _ = self.data(use_threads=False)

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

    def __len__(self):
        return self.num_rows

    def data(self, *, use_threads=True) -> ParquetBatchData:
        """Eagerly read in the whole file as a table."""
        if self._data is None:
            self._data = ParquetBatchData(
                self.file.read(columns=self._column_names, use_threads=use_threads),
                scalar_as_py=self.scalar_as_py,
            )
            if self.num_row_groups == 1:
                assert self._row_groups is None
                self._row_groups = [self._data]
        return self._data

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

    def _locate_row_group_for_item(self, idx):
        # Assuming user is checking neighboring items,
        # then the requested item may be in the same row-group
        # as the item requested last time.
        if self._row_groups_num_rows is None:
            meta = self.metadata
            self._row_groups_num_rows = [
                meta.row_group(i).num_rows for i in range(self.num_row_groups)
            ]
            self._row_groups_num_rows_cumsum = list(
                itertools.accumulate(self._row_groups_num_rows)
            )

        igrp, idx_in_grp, group_info = locate_idx_in_chunked_seq(
            idx, self._row_groups_num_rows_cumsum, self._getitem_last_row_group
        )
        self._getitem_last_row_group = group_info
        return igrp, idx_in_grp

    def __getitem__(self, idx: int):
        """
        Get one record or row.

        `idx`: row index in this file.

        See `ParquetBatchData.__getitem__`.
        """
        if idx < 0:
            idx = self.num_rows + idx
        if idx < 0 or idx >= self.num_rows:
            raise IndexError(idx)

        if self._data is not None:
            return self._data[idx]

        igrp, idx_in_row_group = self._locate_row_group_for_item(idx)
        row_group = self.row_group(igrp)
        return row_group[idx_in_row_group]

    def __iter__(self):
        """
        Iterate over rows.
        The type of yielded individual elements is the same as `__getitem__`.
        """
        if self._data is None:
            for batch in self.file.iter_batches(columns=self._column_names):
                yield from ParquetBatchData(batch, scalar_as_py=self.scalar_as_py)
        else:
            yield from self._data

    def iter_batches(self, batch_size=10_000) -> Iterator[ParquetBatchData]:
        if self._data is None:
            for batch in self.file.iter_batches(
                batch_size=batch_size, columns=self._column_names
            ):
                yield ParquetBatchData(batch, scalar_as_py=self.scalar_as_py)
        else:
            for batch in self._data.data().to_batches(batch_size):
                yield ParquetBatchData(batch, scalar_as_py=self.scalar_as_py)

    def row_group(self, idx: int) -> ParquetBatchData:
        """
        Return the specified row group.
        """
        assert 0 <= idx < self.num_row_groups
        if self._row_groups is None:
            self._row_groups = [None] * self.num_row_groups
        if self._row_groups[idx] is None:
            self._row_groups[idx] = ParquetBatchData(
                self.file.read_row_group(idx, columns=self._column_names),
                scalar_as_py=self.scalar_as_py,
            )
            if self.num_row_groups == 1:
                assert self._data is None
                self._data = self._row_groups[0]
        return self._row_groups[idx]

    def view(self):
        # The returned object supports row slicing.
        return ListView(self)

    def columns(self, cols: Union[str, Sequence[str]]) -> ParquetFileData:
        """
        Return a new `ParquetFileData` object that will only load
        the specified columns.

        The columns of interest have to be within currently available columns.
        In other words, if a series of calls to this method will incrementally
        narrow down the selection of columns.

        This method "slices" the data by columns, in contrast to most other
        data access methods that are selecting rows.

        Examples:

            obj = ParquetFileData('file_path')
            obj1 = obj.columns(['a', 'b', 'c'])
            print(obj1[2])
            obj2 = obj1.columns(['b', 'c'])
            print(obj2[3])
            obj3 = obj.columns('d')
            for v in obj:
                print(v)
        """
        if isinstance(cols, str):
            cols = [cols]
        assert len(set(cols)) == len(cols)  # no repeat values

        if self._column_names:
            if all(col in self._column_names for col in cols):
                if len(cols) == len(self._column_names):
                    return self
            else:
                cc = [col for col in cols if col not in self._column_names]
                raise ValueError(
                    f"cannot select the columns {cc} because they are not in existing set of columns"
                )

        obj = self.__class__(
            self.file, eager_load=False, scalar_as_py=self.scalar_as_py
        )
        obj._row_groups_num_rows = self._row_groups_num_rows
        obj._row_groups_num_rows_cumsum = self._row_groups_num_rows_cumsum
        if self._row_groups:
            obj._row_groups = [
                None if v is None else v.columns(cols) for v in self._row_groups
            ]
        if self._data is not None:
            obj._data = self._data.columns(cols)
        # TODO: also carry over `self._columns`?
        obj._column_names = cols
        return obj

    def column(self, idx_or_name: Union[int, str]) -> pyarrow.ChunkedArray:
        z = self._columns.get(idx_or_name)
        if z is not None:
            return z
        if self._data is not None:
            return self._data.column(idx_or_name)
        if isinstance(idx_or_name, int):
            idx = idx_or_name
            name = self.column_names[idx]
        else:
            name = idx_or_name
            idx = self.column_names.index(name)
        z = self.file.read(columns=[name]).column(name)
        self._columns[idx] = z
        self._columns[name] = z
        return z


class ParquetBatchData(collections.abc.Sequence):
    """
    Objects of this class can be pickled.
    """

    def __init__(
        self,
        data: Union[pyarrow.Table, pyarrow.RecordBatch],
        *,
        scalar_as_py: bool = True,
    ):
        # `self.scalar_as_py` may be toggled anytime
        # and have its effect right away.
        self._data = data
        self.scalar_as_py = scalar_as_py
        self.num_rows = data.num_rows
        self.num_columns = data.num_columns
        self.column_names = data.schema.names

    def __repr__(self):
        return "<{} with {} rows, {} columns>".format(
            self.__class__.__name__,
            self.num_rows,
            self.num_columns,
        )

    def __str__(self):
        return self.__repr__()

    def data(self):
        return self._data

    def __len__(self):
        return self.num_rows

    def __bool__(self):
        return self.num_rows > 0

    def __getitem__(self, idx: int):
        """
        Get one record or row.

        `idx`: row index in this file.

        If `self.scalar_as_py` is False,
          If data has a single column, return the value on the specified row.
          The return is a `pyarrow.Scalar`` type such as `pyarrow.lib.StringScalar`.
          If data has multiple columns, return a dict with keys being column names
          and values being `pyarrow.Scalar`` types.
        If `self.scalar_as_py` if True,
          the `pyarrow.Scalar` values are converted to Python native types.
        """
        if idx < 0:
            idx = self.num_rows + idx
        if idx < 0 or idx >= self.num_rows:
            raise IndexError(idx)

        if self.num_columns == 1:
            z = self._data.column(0)[idx]
            if self.scalar_as_py:
                return z.as_py()
            return z

        z = {col: self._data.column(col)[idx] for col in self.column_names}
        if self.scalar_as_py:
            return {k: v.as_py() for k, v in z.items()}
        return z

    def __iter__(self):
        # Iterate over rows.
        # The type of yielded individual elements is the same as `__getitem__`.
        if self.num_columns == 1:
            if self.scalar_as_py:
                yield from (v.as_py() for v in self._data.column(0))
            else:
                yield from self._data.column(0)
        else:
            names = self.column_names
            if self.scalar_as_py:
                for row in zip(*self._data.columns):
                    yield dict(zip(names, (v.as_py() for v in row)))
            else:
                for row in zip(*self._data.columns):
                    yield dict(zip(names, row))

    def view(self):
        return ListView(self)

    def columns(self, cols: Union[str, Sequence[str]]) -> ParquetBatchData:
        """
        Return a new `ParquetBatchData` object that will only produce
        the specified columns.

        The columns of interest have to be within currently available columns.
        In other words, a series of calls to this method would incrementally
        narrow down the selection of columns.

        This method "slices" the data by columns, in contrast to most other
        data access methods that are selecting rows.

        Examples:

            obj = ParquetBatchData(parquet_table)
            obj1 = obj.columns(['a', 'b', 'c'])
            print(obj1[2])
            obj2 = obj1.columns(['b', 'c'])
            print(obj2[3])
            obj3 = obj.columns('d')
            for v in obj:
                print(v)
        """
        if isinstance(cols, str):
            cols = [cols]
        assert len(set(cols)) == len(cols)  # no repeat values

        if all(col in self.column_names for col in cols):
            if len(cols) == len(self.column_names):
                return self
        else:
            cc = [col for col in cols if col not in self.column_names]
            raise ValueError(
                f"cannot select the columns {cc} because they are not in existing set of columns"
            )

        return self.__class__(self._data.select(cols), scalar_as_py=self.scalar_as_py)

    def column(
        self, idx_or_name: Union[int, str]
    ) -> Union[pyarrow.Array, pyarrow.ChunkedArray]:
        """
        If `self._data` is `pyarrow.Table`, return `pyarrow.ChunkedArray`.
        If `self._data` is `pyarrow.RecordBatch`, return `pyarrow.Array`.
        """
        return self._data.column(idx_or_name)


def read_parquet_file(path: PathType, **kwargs):
    f = ParquetBiglist.read_parquet_file(str(ParquetBiglist.resolve_path(path)))
    return ParquetFileData(f, **kwargs)


def write_parquet_file(
    path: PathType,
    data: Union[
        pyarrow.Table, Sequence[Union[pyarrow.Array, pyarrow.ChunkedArray, Iterable]]
    ],
    *,
    names: Sequence[str] = None,
    **kwargs,
):
    """
    If the file already exists, it will be overwritten.
    """
    if not isinstance(data, pyarrow.Table):
        assert names
        assert len(names) == len(data)
        arrays = [
            a
            if isinstance(a, (pyarrow.Array, pyarrow.ChunkedArray))
            else pyarrow.array(a)
            for a in data
        ]
        data = pyarrow.Table.from_arrays(arrays, names=names)
    else:
        assert names is None
    path = ParquetBiglist.resolve_path(path)
    if isinstance(path, LocalUpath):
        path.parent.localpath.mkdir(exist_ok=True, parents=True)
    ff, pp = FileSystem.from_uri(str(path))
    if isinstance(ff, GcsFileSystem):
        ff = ParquetBiglist.get_gcsfs()
    pyarrow.parquet.write_table(data, ff.open_output_stream(pp), **kwargs)
