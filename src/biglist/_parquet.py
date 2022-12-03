from __future__ import annotations

import collections.abc
import itertools
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Union, Sequence, Iterator, List, Iterable, Any, Callable, Tuple

import pyarrow
from pyarrow.parquet import ParquetFile, FileMetaData
from pyarrow.fs import FileSystem, GcsFileSystem
from upathlib import LocalUpath
from ._base import BiglistBase, Upath, PathType, ListView, FileReader
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
    ``ParquetBiglist`` defines a kind of "external biglist", that is,
    it points to pre-existing Parquet files and provides facilities to read them.
    As long as you use a ``ParquetBiglist`` object to read, it is assumed that
    the dataset (all the data files) have not changed since the object was created
    by ``new``.
    """

    @classmethod
    def get_gcsfs(cls, *, good_for_seconds=600) -> GcsFileSystem:
        """
        Obtain a ``GcsFileSystem`` object with credentials given so that
        the GCP default process of inferring credentials (which involves
        env vars and file reading etc) will not be triggered.

        This is provided under the (un-verified) assumption that the
        default credential inference process is a high overhead.
        """
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
            or (cred.expiry - datetime.utcnow()).total_seconds() < good_for_seconds
        ):
            cred.refresh(google.auth.transport.requests.Request())
            # One check shows this token expires in one hour.
        return GcsFileSystem(
            access_token=cred.token, credential_token_expiration=cred.expiry
        )

    @classmethod
    def load_data_file(cls, path: Upath) -> ParquetFile:
        """
        Load the data file given by ``path``.

        This function is used as the argument ``loader`` to ``ParquetFileReader.__init__``.
        """
        ff, pp = FileSystem.from_uri(str(path))
        if isinstance(ff, GcsFileSystem):
            ff = cls.get_gcsfs()
        return ParquetFile(ff.open_input_file(pp))

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
    ) -> ParquetBiglist:
        """
        This classmethod gathers info of the specified data files and
        saves the info to facilitate reading the data files.
        The data files remain "external" to the ``ParquetBiglist`` object;
        the "data" persisted and managed by the ``ParquetBiglist`` object
        are the meta info about the Parquet data files.

        If the number of data files is small, it's feasible to create a temporary
        object of this class (by leaving ``path`` at the default value``None``)
        "on-the-fly" for one-time use.

        Parameters
        ----------
        data_path
            Parquet file(s) or folder(s) containing Parquet files.

            If this is a single path, then it's either a Parquet file or a directory.
            If this is a list, each element is either a Parquet file or a directory;
            there can be a mix of files and directories.
            Directories are traversed recursively for Parquet files.
            The paths can be local, or in the cloud, or a mix of both.

            Once the info of all Parquet files are gathered,
            their order is fixed as far as this ``ParquetBiglist`` is concerned.
            The data sequence represented by this ``ParquetBiglist`` follows this
            order of the files. The order is determined as follows:

                The order of the entries in ``data_path`` is preserved; if any entry is a
                directory, the files therein (recursively) are sorted by the string
                value of each file's full path.

        path
            Directory to be used by this object to save whatever it needs to persist
            (i.e. the meta info about the data files).
            This directory must be non-existent at the time of this call.
            If `None``, a temporary location will be determined by ``get_temp_path``.

        suffix
            Only files with this suffix will be included.
            To include all files, use ``suffix='*'``.

        keep_files
            If not specified, the default behavior is the following:

                If ``path`` is ``None``, then this is ``False``---the temporary directory
                will be deleted when this ``ParquetBiglist`` object goes away.

                If ``path`` is not ``None``, i.e. user has deliberately specified a location,
                then this is ``True``---files saved by this ``ParquetBiglist`` object will stay.

            User can pass in ``True`` or ``False`` explicitly to override the default behavior.

        **kwargs
            additional arguments are passed on to ``__init__``.
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

        def get_file_meta(f, p: Upath):
            meta = f(p).metadata
            return {
                "path": str(p),  # str of full path
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
        read_parquet = cls.load_data_file
        for p in data_path:
            if p.is_file():
                if suffix == "*" or p.name.endswith(suffix):
                    tasks.append(pool.submit(get_file_meta, read_parquet, p))
            else:
                tt = []
                for pp in p.riterdir():
                    if suffix == "*" or pp.name.endswith(suffix):
                        tt.append(
                            (str(pp), pool.submit(get_file_meta, read_parquet, pp))
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
        """Please see doc of the base class."""
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

    def file_reader(self, file: Union[Upath, int]) -> ParquetFileReader:
        if isinstance(file, int):
            datafiles, _ = self._get_data_files()
            file = self._get_data_file(datafiles, file)
        return ParquetFileReader(file, self.load_data_file)

    @property
    def datafiles(self) -> List[str]:
        return [f["path"] for f in self.info["datafiles"]]

    @property
    def datafiles_info(self) -> List[Tuple[str, int, int]]:
        files = self.info["datafiles"]
        cumlen = self.info["datafiles_cumlength"]
        return [
            (file["path"], file["num_rows"], cum) for file, cum in zip(files, cumlen)
        ]


class ParquetFileReader(FileReader):
    def __init__(self, path: PathType, loader: Callable[[Upath], Any]):
        """
        Parameters
        ----------
        path
            Path of a Parquet file.
        loader
            Usually this is ``ParquetBiglist.load_data_file``.
            If you customize this, please see the doc of ``FileReader.__init__``.
        """
        self._file: ParquetFile = None
        self._data: ParquetBatchData = None

        self._row_groups_num_rows = None
        self._row_groups_num_rows_cumsum = None
        self._row_groups: List[ParquetBatchData] = None

        self._column_names = None
        self._columns = {}
        self._getitem_last_row_group = None

        super().__init__(path, loader)

        self._scalar_as_py = None
        self.scalar_as_py = True

    @property
    def scalar_as_py(self) -> bool:
        """
        ``scalar_as_py`` controls whether the values returned by ``__getitem__``
        (or indirectly by ``__iter__``) are converted from a ``pyarrow.Scalar`` type
        such as ``pyarrow.lib.StringScalar`` to a Python builtin type such as ``str``.

        This property can be toggled anytime to take effect until it is toggled again.
        """
        if self._scalar_as_py is None:
            self._scalar_as_py = True
        return self._scalar_as_py

    @scalar_as_py.setter
    def scalar_as_py(self, value: bool):
        self._scalar_as_py = bool(value)
        if self._data is not None:
            self._data.scalar_as_py = self._scalar_as_py
        if self._row_groups:
            for r in self._row_groups:
                if r is not None:
                    r.scalar_as_py = self._scalar_as_py

    def __len__(self) -> int:
        return self.num_rows

    def load(self) -> None:
        """Eagerly read in the whole file as a table."""
        if self._data is None:
            self._data = ParquetBatchData(
                self.file.read(columns=self._column_names, use_threads=True),
            )
            self._data.scalar_as_py = (self.scalar_as_py,)
            if self.num_row_groups == 1:
                assert self._row_groups is None
                self._row_groups = [self._data]

    @property
    def file(self) -> ParquetFile:
        """Return a ``pyarrow.parquet.ParquetFile`` object.

        Upon initiation of a ``ParquetFileReader`` object,
        the file is not read at all. When this property is requested,
        the file is accessed to construct a ``ParquetFile`` object,
        but this only reads *meta* info; it does not read the *data*.
        """
        if self._file is None:
            self._file = self.loader(self.path)
        return self._file

    @property
    def metadata(self) -> FileMetaData:
        return self.file.metadata

    @property
    def num_rows(self) -> int:
        return self.metadata.num_rows

    @property
    def num_row_groups(self) -> int:
        return self.metadata.num_row_groups

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

    def data(self) -> ParquetBatchData:
        """Return the entire data in the file."""
        self.load()
        return self._data

    def _locate_row_group_for_item(self, idx: int):
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
        Get one row (or "record").

        If the object has a single column, then return its value in the specified row.
        If the object has multiple columns, return a ``dict`` with column names as keys.
        The values are converted to Python builtin types if ``self.scalar_as_py``
        is ``True``.

        Parameters
        ----------
        idx
            Row index in this file.
            Negative value counts from the end as expected.
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
        The type of yielded individual elements is the same as the return of ``__getitem__``.
        """
        if self._data is None:
            for batch in self.iter_batches():
                yield from batch
        else:
            yield from self._data

    def iter_batches(self, batch_size=10_000) -> Iterator[ParquetBatchData]:
        if self._data is None:
            for batch in self.file.iter_batches(
                batch_size=batch_size,
                columns=self._column_names,
                use_threads=True,
            ):
                z = ParquetBatchData(batch)
                z.scalar_as_py = self.scalar_as_py
                yield z
        else:
            for batch in self._data.data().to_batches(batch_size):
                z = ParquetBatchData(batch)
                z.scalar_as_py = self.scalar_as_py
                yield z

    def row_group(self, idx: int) -> ParquetBatchData:
        """
        Parameters
        ----------
        idx
            Index of the row group of interest.
        """
        assert 0 <= idx < self.num_row_groups
        if self._row_groups is None:
            self._row_groups = [None] * self.num_row_groups
        if self._row_groups[idx] is None:
            z = ParquetBatchData(
                self.file.read_row_group(idx, columns=self._column_names),
            )
            z.scalar_as_py = self.scalar_as_py
            self._row_groups[idx] = z
            if self.num_row_groups == 1:
                assert self._data is None
                self._data = self._row_groups[0]
        return self._row_groups[idx]

    def columns(self, cols: Sequence[str]) -> ParquetFileReader:
        """
        Return a new ``ParquetFileReader`` object that will only load
        the specified columns.

        The columns of interest have to be within currently available columns.
        In other words, a series of calls to this method would incrementally
        narrow down the selection of columns. (Note this returns a new
        ``ParquetFileReader``, hence one can call ``columns`` again on the
        returned object.)

        This method "slices" the data by columns, in contrast to other
        data access methods that select rows.

        Parameters
        ----------
        cols
            Names of the columns to select.

        Examples
        --------
        >>> obj = ParquetFileReader('file_path')
        >>> obj1 = obj.columns(['a', 'b', 'c'])
        >>> print(obj1[2])
        >>> obj2 = obj1.columns(['b', 'c'])
        >>> print(obj2[3])
        >>> obj3 = obj.columns(['d'])
        >>> for v in obj:
        >>>     print(v)
        """
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

        obj = self.__class__(self.path, self.loader)
        obj.scalar_as_py = self.scalar_as_py
        obj._file = self._file
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
        """Select a single column.

        Note: while ``columns`` returns a new ``ParquetFileReader``,
        ``column`` returns a ``pyarrow.ChunkedArray``.
        """
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
    ``ParquetBatchData`` wraps a ``pyarrow.Table`` or ``pyarrow.RecordBatch``.
    The data is already in memory; this class does not involve file reading.

    ``ParquetFileReader.data`` and ``ParquetFileReader.iter_batches`` both
    return or yield ``ParquetBatchData``.
    In addition, the method ``columns`` of this class returns a new object
    of this class.

    Objects of this class can be pickled.
    """

    def __init__(
        self,
        data: Union[pyarrow.Table, pyarrow.RecordBatch],
    ):
        # `self.scalar_as_py` may be toggled anytime
        # and have its effect right away.
        self._data = data
        self.scalar_as_py = True
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

    def data(self) -> Union[pyarrow.Table, pyarrow.RecordBatch]:
        """Return the underlying ``pyarrow`` data."""
        return self._data

    def __len__(self) -> int:
        return self.num_rows

    def __bool__(self) -> bool:
        return self.num_rows > 0

    def __getitem__(self, idx: int):
        """
        Get one row (or "record").

        If the object has a single column, then return its value in the specified row.
        If the object has multiple columns, return a ``dict`` with column names as keys.
        The values are converted to Python builtin types if ``self.scalar_as_py``
        is ``True``.

        Parameters
        ----------
        idx
            Row index in this batch.
            Negative value counts from the end as expected.
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
        """
        Iterate over rows.
        The type of yielded individual elements is the same as ``__getitem__``.
        """
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

    def view(self) -> ListView:
        """Return a ``ListView`` to gain slicing capabilities."""
        return ListView(self)

    def columns(self, cols: Sequence[str]) -> ParquetBatchData:
        """
        Return a new ``ParquetBatchData`` object that only contains
        the specified columns.

        The columns of interest have to be within currently available columns.
        In other words, a series of calls to this method would incrementally
        narrow down the selection of columns.
        (Note this returns a new ``ParquetBatchData``,
        hence one can call ``columns`` again on the returned object.)

        This method "slices" the data by columns, in contrast to other
        data access methods that select rows.

        Parameters
        ----------
        cols
            Names of the columns to select.

        Examples
        --------
        >>> obj = ParquetBatchData(parquet_table)
        >>> obj1 = obj.columns(['a', 'b', 'c'])
        >>> print(obj1[2])
        >>> obj2 = obj1.columns(['b', 'c'])
        >>> print(obj2[3])
        >>> obj3 = obj.columns(['d'])
        >>> for v in obj:
        >>>     print(v)
        """
        assert len(set(cols)) == len(cols)  # no repeat values

        if all(col in self.column_names for col in cols):
            if len(cols) == len(self.column_names):
                return self
        else:
            cc = [col for col in cols if col not in self.column_names]
            raise ValueError(
                f"cannot select the columns {cc} because they are not in existing set of columns"
            )

        z = self.__class__(self._data.select(cols))
        z.scalar_as_py = self.scalar_as_py
        return z

    def column(
        self, idx_or_name: Union[int, str]
    ) -> Union[pyarrow.Array, pyarrow.ChunkedArray]:
        """
        Select a single column specified by name or index.

        If ``self._data`` is ``pyarrow.Table``, return ``pyarrow.ChunkedArray``.
        If ``self._data`` is ``pyarrow.RecordBatch``, return ``pyarrow.Array``.
        """
        return self._data.column(idx_or_name)


def read_parquet_file(path: PathType, **kwargs) -> ParquetFileReader:
    return ParquetFileReader(path, ParquetBiglist.load_data_file, **kwargs)


def write_parquet_file(
    path: PathType,
    data: Union[
        pyarrow.Table, Sequence[Union[pyarrow.Array, pyarrow.ChunkedArray, Iterable]]
    ],
    *,
    names: Sequence[str] = None,
    **kwargs,
) -> None:
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
