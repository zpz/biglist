from __future__ import annotations

import itertools
import logging
from collections.abc import Iterable, Iterator, Sequence
from multiprocessing.util import Finalize
from pathlib import Path

import pyarrow
from pyarrow.fs import FileSystem, GcsFileSystem
from pyarrow.parquet import FileMetaData, ParquetFile
from upathlib import LocalUpath, PathType, Upath, resolve_path

try:
    from upathlib.gcs import get_google_auth
except ImportError:
    pass

from ._base import (
    BiglistBase,
    FileReader,
    FileSeq,
    Seq,
)
from ._util import get_global_thread_pool, locate_idx_in_chunked_seq, lock_to_use

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


class ParquetFileReader(FileReader):
    @classmethod
    def get_gcsfs(cls, *, good_for_seconds=600) -> GcsFileSystem:
        """
        Obtain a `pyarrow.fs.GcsFileSystem`_ object with credentials given so that
        the GCP default process of inferring credentials (which involves
        env vars and file reading etc) will not be triggered.

        This is provided under the (un-verified) assumption that the
        default credential inference process is a high overhead.
        """
        cls._GCP_PROJECT_ID, cls._GCP_CREDENTIALS, renewed = get_google_auth(
            project_id=getattr(cls, '_GCP_PROJECT_ID', None),
            credentials=getattr(cls, "_GCP_CREDENTIALS", None),
            valid_for_seconds=good_for_seconds,
        )
        if renewed or getattr(cls, '_GCSFS', None) is None:
            fs = GcsFileSystem(
                access_token=cls._GCP_CREDENTIALS.token,
                credential_token_expiration=cls._GCP_CREDENTIALS.expiry,
            )
            cls._GCSFS = fs
        return cls._GCSFS

    @classmethod
    def load_file(cls, path: Upath) -> ParquetFile:
        '''
        This reads *meta* info and constructs a ``pyarrow.parquet.ParquetFile`` object.
        This does not load the entire file.
        See :meth:`load` for eager loading.

        Parameters
        ----------
        path
            Path of the file.
        '''
        ff, pp = FileSystem.from_uri(str(path))
        if isinstance(ff, GcsFileSystem):
            ff = cls.get_gcsfs()
        file = ParquetFile(pp, filesystem=ff)
        Finalize(file, file.reader.close)
        # NOTE: can not use
        #
        #   Finalize(file, file.close, kwargs={'force': True})
        #
        # because the instance method `file.close` can't be used as the callback---the
        # object `file` is no long available at that time.
        #
        # See https://github.com/apache/arrow/issues/35318
        return file

    def __init__(self, path: PathType):
        """
        Parameters
        ----------
        path
            Path of a Parquet file.
        """
        self.path: Upath = resolve_path(path)
        self._reset()

    def _reset(self):
        self._file: ParquetFile | None = None
        self._data: ParquetBatchData | None = None

        self._row_groups_num_rows = None
        self._row_groups_num_rows_cumsum = None
        self._row_groups: None | list[ParquetBatchData] = None

        self._column_names = None
        self._columns = {}
        self._getitem_last_row_group = None

        self._scalar_as_py = None
        self.scalar_as_py = True

    def __getstate__(self):
        return (self.path,)

    def __setstate__(self, data):
        self.path = data[0]
        self._reset()

    @property
    def scalar_as_py(self) -> bool:
        """
        ``scalar_as_py`` controls whether the values returned by :meth:`__getitem__`
        (or indirectly by :meth:`__iter__`) are converted from a `pyarrow.Scalar`_ type
        such as `pyarrow.lib.StringScalar`_ to a Python builtin type such as ``str``.

        This property can be toggled anytime to take effect until it is toggled again.

        :getter: Returns this property's value.
        :setter: Sets this property's value.
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
        """Eagerly read the whole file into memory as a table."""
        if self._data is None:
            self._data = ParquetBatchData(
                self.file.read(columns=self._column_names, use_threads=True),
            )
            self._data.scalar_as_py = self.scalar_as_py
            if self.num_row_groups == 1:
                assert self._row_groups is None
                self._row_groups = [self._data]

    @property
    def file(self) -> ParquetFile:
        """Return a `pyarrow.parquet.ParquetFile`_ object.

        Upon initiation of a :class:`ParquetFileReader` object,
        the file is not read at all. When this property is requested,
        the file is accessed to construct a `pyarrow.parquet.ParquetFile`_ object.

        """
        if self._file is None:
            self._file = self.load_file(self.path)
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
    def column_names(self) -> list[str]:
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
        If the object has multiple columns, return a dict with column names as keys.
        The values are converted to Python builtin types if :data:`scalar_as_py`
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
        The type of yielded individual elements is the same as the return of :meth:`__getitem__`.
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
        Return a new :class:`ParquetFileReader` object that will only load
        the specified columns.

        The columns of interest have to be within currently available columns.
        In other words, a series of calls to this method would incrementally
        narrow down the selection of columns. (Note this returns a new
        :class:`ParquetFileReader`, hence one can call :meth:`columns` again on the
        returned object.)

        This method "slices" the data by columns, in contrast to other
        data access methods that select rows.

        Parameters
        ----------
        cols
            Names of the columns to select.

        Examples
        --------
        >>> obj = ParquetFileReader('file_path', ParquetBiglist.load_data_file)  # doctest: +SKIP
        >>> obj1 = obj.columns(['a', 'b', 'c'])  # doctest: +SKIP
        >>> print(obj1[2])  # doctest: +SKIP
        >>> obj2 = obj1.columns(['b', 'c'])  # doctest: +SKIP
        >>> print(obj2[3])  # doctest: +SKIP
        >>> obj3 = obj.columns(['d'])  # doctest: +SKIP
        >>> for v in obj:  # doctest: +SKIP
        >>>     print(v)  # doctest: +SKIP
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

        obj = self.__class__(self.path)
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

    def column(self, idx_or_name: int | str) -> pyarrow.ChunkedArray:
        """Select a single column.

        Note: while :meth:`columns` returns a new :class:`ParquetFileReader`,
        :meth:`column` returns a `pyarrow.ChunkedArray`_.
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


class ParquetBatchData(Seq):
    """
    ``ParquetBatchData`` wraps a `pyarrow.Table`_ or `pyarrow.RecordBatch`_.
    The data is already in memory; this class does not involve file reading.

    :meth:`ParquetFileReader.data` and :meth:`ParquetFileReader.iter_batches` both
    return or yield ParquetBatchData.
    In addition, the method :meth:`columns` of this class returns a new object
    of this class.

    Objects of this class can be pickled.
    """

    def __init__(
        self,
        data: pyarrow.Table | pyarrow.RecordBatch,
    ):
        # `self.scalar_as_py` may be toggled anytime
        # and have its effect right away.
        self._data = data
        self.scalar_as_py = True
        """Indicate whether scalar values should be converted to Python types from `pyarrow`_ types."""
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

    def data(self) -> pyarrow.Table | pyarrow.RecordBatch:
        """Return the underlying `pyarrow`_ data."""
        return self._data

    def __len__(self) -> int:
        return self.num_rows

    def __getitem__(self, idx: int):
        """
        Get one row (or "record").

        If the object has a single column, then return its value in the specified row.
        If the object has multiple columns, return a dict with column names as keys.
        The values are converted to Python builtin types if :data:`scalar_as_py`
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
        The type of yielded individual elements is the same as :meth:`__getitem__`.
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

    def columns(self, cols: Sequence[str]) -> ParquetBatchData:
        """
        Return a new :class:`ParquetBatchData` object that only contains
        the specified columns.

        The columns of interest have to be within currently available columns.
        In other words, a series of calls to this method would incrementally
        narrow down the selection of columns.
        (Note this returns a new :class:`ParquetBatchData`,
        hence one can call :meth:`columns` again on the returned object.)

        This method "slices" the data by columns, in contrast to other
        data access methods that select rows.

        Parameters
        ----------
        cols
            Names of the columns to select.

        Examples
        --------
        >>> obj = ParquetBatchData(parquet_table)  # doctest: +SKIP
        >>> obj1 = obj.columns(['a', 'b', 'c'])  # doctest: +SKIP
        >>> print(obj1[2])  # doctest: +SKIP
        >>> obj2 = obj1.columns(['b', 'c'])  # doctest: +SKIP
        >>> print(obj2[3])  # doctest: +SKIP
        >>> obj3 = obj.columns(['d'])  # doctest: +SKIP
        >>> for v in obj:  # doctest: +SKIP
        >>>     print(v)  # doctest: +SKIP
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

    def column(self, idx_or_name: int | str) -> pyarrow.Array | pyarrow.ChunkedArray:
        """
        Select a single column specified by name or index.

        If ``self._data`` is `pyarrow.Table`_, return `pyarrow.ChunkedArray`_.
        If ``self._data`` is `pyarrow.RecordBatch`_, return `pyarrow.Array`_.
        """
        return self._data.column(idx_or_name)


def read_parquet_file(path: PathType) -> ParquetFileReader:
    """
    Parameters
    ----------
    path
        Path of the file.
    """
    return ParquetFileReader(path)


class ParquetBiglist(BiglistBase):
    """
    ``ParquetBiglist`` defines a kind of "external biglist", that is,
    it points to pre-existing Parquet files and provides facilities to read them.
    As long as you use a ParquetBiglist object to read, it is assumed that
    the dataset (all the data files) have not changed since the object was created
    by :meth:`new`.
    """

    @classmethod
    def new(
        cls,
        data_path: PathType | Sequence[PathType],
        path: PathType | None = None,
        *,
        suffix: str = ".parquet",
        **kwargs,
    ) -> ParquetBiglist:
        """
        This classmethod gathers info of the specified data files and
        saves the info to facilitate reading the data files.
        The data files remain "external" to the :class:`ParquetBiglist` object;
        the "data" persisted and managed by the ParquetBiglist object
        are the meta info about the Parquet data files.

        If the number of data files is small, it's feasible to create a temporary
        object of this class (by leaving ``path`` at the default value ``None``)
        "on-the-fly" for one-time use.

        Parameters
        ----------
        path
            Passed on to :meth:`BiglistBase.new` of :class:`BiglistBase`.
        data_path
            Parquet file(s) or folder(s) containing Parquet files.

            If this is a single path, then it's either a Parquet file or a directory.
            If this is a list, each element is either a Parquet file or a directory;
            there can be a mix of files and directories.
            Directories are traversed recursively for Parquet files.
            The paths can be local, or in the cloud, or a mix of both.

            Once the info of all Parquet files are gathered,
            their order is fixed as far as this :class:`ParquetBiglist` is concerned.
            The data sequence represented by this ParquetBiglist follows this
            order of the files. The order is determined as follows:

                The order of the entries in ``data_path`` is preserved; if any entry is a
                directory, the files therein (recursively) are sorted by the string
                value of each file's full path.

        suffix
            Only files with this suffix will be included.
            To include all files, use ``suffix='*'``.

        **kwargs
            additional arguments are passed on to :meth:`__init__`.
        """
        if isinstance(data_path, (str, Path, Upath)):
            #  TODO: in py 3.10, we will be able to do `isinstance(data_path, PathType)`
            data_path = [resolve_path(data_path)]
        else:
            data_path = [resolve_path(p) for p in data_path]

        def get_file_meta(p: Upath):
            ff = ParquetFileReader.load_file(p)
            meta = ff.metadata
            return {
                "path": str(p),  # str of full path
                "num_rows": meta.num_rows,
                # "row_groups_num_rows": [
                #     meta.row_group(k).num_rows for k in range(meta.num_row_groups)
                # ],
            }

        pool = get_global_thread_pool()
        tasks = []
        for p in data_path:
            if p.is_file():
                if suffix == "*" or p.name.endswith(suffix):
                    tasks.append(pool.submit(get_file_meta, p))
            else:
                tt = []
                for pp in p.riterdir():
                    if suffix == "*" or pp.name.endswith(suffix):
                        tt.append((str(pp), pool.submit(get_file_meta, pp)))
                tt.sort()
                for p, t in tt:
                    tasks.append(t)

        assert tasks
        datafiles = []
        for k, t in enumerate(tasks):
            datafiles.append(t.result())
            if (k + 1) % 1000 == 0:
                logger.info("processed %d files", k + 1)

        datafiles_cumlength = list(
            itertools.accumulate(v["num_rows"] for v in datafiles)
        )

        obj = super().new(path, **kwargs)  # type: ignore
        obj.info["datapath"] = [str(p) for p in data_path]

        # Removed in 0.7.4
        # obj.info["datafiles"] = datafiles
        # obj.info["datafiles_cumlength"] = datafiles_cumlength

        # Added in 0.7.4
        data_files_info = [
            (a["path"], a["num_rows"], b)
            for a, b in zip(datafiles, datafiles_cumlength)
        ]
        obj.info["data_files_info"] = data_files_info

        obj.info["storage_format"] = "parquet"
        obj.info["storage_version"] = 1
        # `storage_version` is a flag for certain breaking changes in the implementation,
        # such that certain parts of the code (mainly concerning I/O) need to
        # branch into different treatments according to the version.
        # This has little relation to `storage_format`.
        # version 1 designator introduced in version 0.7.4.
        # prior to 0.7.4 it is absent, and considered 0.

        obj._info_file.write_json(obj.info, overwrite=True)

        return obj

    def __init__(self, *args, **kwargs):
        """Please see doc of the base class."""
        super().__init__(*args, **kwargs)
        self.keep_files: bool = True
        """Indicates whether the meta info persisted by this object should be kept or deleted when this object is garbage-collected.

        This does *not* affect the external Parquet data files.
        """

        # For back compat. Added in 0.7.4.
        if self.info and "data_files_info" not in self.info:
            # This is not called by ``new``, instead is opening an existing dataset
            assert self.storage_version == 0
            data_files_info = [
                (a["path"], a["num_rows"], b)
                for a, b in zip(
                    self.info["datafiles"], self.info["datafiles_cumlength"]
                )
            ]
            self.info["data_files_info"] = data_files_info
            with lock_to_use(self._info_file) as ff:
                ff.write_json(self.info, overwrite=True)

    def __repr__(self):
        return f"<{self.__class__.__name__} at '{self.path}' with {len(self)} records in {len(self.files)} data file(s) stored at {self.info['datapath']}>"

    @property
    def storage_version(self) -> int:
        return self.info.get("storage_version", 0)

    @property
    def files(self):
        # This method should be cheap to call.
        return ParquetFileSeq(
            self.path,
            self.info["data_files_info"],
        )


class ParquetFileSeq(FileSeq[ParquetFileReader]):
    def __init__(
        self,
        root_dir: Upath,
        data_files_info: list[tuple[str, int, int]],
    ):
        """
        Parameters
        ----------
        root_dir
            Root directory for storage of meta info.
        data_files_info
            A list of data files that constitute the file sequence.
            Each tuple in the list is comprised of a file path (relative to
            ``root_dir``), number of data items in the file, and cumulative
            number of data items in the files up to the one at hand.
            Therefore, the order of the files in the list is significant.
        """
        self._root_dir = root_dir
        self._data_files_info = data_files_info

    @property
    def path(self):
        return self._root_dir

    @property
    def data_files_info(self):
        return self._data_files_info

    def __getitem__(self, idx: int):
        return ParquetFileReader(
            self._data_files_info[idx][0],
        )


def make_parquet_type(type_spec: str | Sequence):
    """
    ``type_spec`` is a spec of arguments to one of pyarrow's data type
    `factory functions <https://arrow.apache.org/docs/python/api/datatypes.html#factory-functions>`_.

    For simple types, this may be just the type name (or function name), e.g. ``'bool_'``, ``'string'``, ``'float64'``.

    For type functions expecting arguments, this is a list or tuple with the type name followed by other arguments,
    for example,

    ::

        ('time32', 's')
        ('decimal128', 5, -3)

    For compound types (types constructed by other types), this is a "recursive" structure, such as

    ::

        ('list_', 'int64')
        ('list_', ('time32', 's'), 5)

    where the second element is the spec for the member type, or

    ::

        ('map_', 'string', ('list_', 'int64'), True)

    where the second and third elements are specs for the key type and value type, respectively,
    and the fourth element is the optional argument ``keys_sorted`` to
    `pyarrow.map_() <https://arrow.apache.org/docs/python/generated/pyarrow.map_.html#pyarrow.map_>`_.
    Below is an example of a struct type::

        ('struct', [('name', 'string', False), ('age', 'uint8', True), ('income', ('struct', (('currency', 'string'), ('amount', 'uint64'))), False)])

    Here, the second element is the list of fields in the struct.
    Each field is expressed by a spec that is taken by :meth:`make_parquet_field`.
    """
    if isinstance(type_spec, str):
        type_name = type_spec
        args = ()
    else:
        type_name = type_spec[0]
        args = type_spec[1:]

    if type_name in ("string", "float64", "bool_", "int8", "int64", "uint8", "uint64"):
        assert not args
        return getattr(pyarrow, type_name)()

    if type_name == "list_":
        if len(args) > 2:
            raise ValueError(f"'pyarrow.list_' expects 1 or 2 args, got `{args}`")
        return pyarrow.list_(make_parquet_type(args[0]), *args[1:])

    if type_name in ("map_", "dictionary"):
        if len(args) > 3:
            raise ValueError(f"'pyarrow.{type_name}' expects 2 or 3 args, got `{args}`")
        return getattr(pyarrow, type_name)(
            make_parquet_type(args[0]),
            make_parquet_type(args[1]),
            *args[2:],
        )

    if type_name == "struct":
        assert len(args) == 1
        return pyarrow.struct((make_parquet_field(v) for v in args[0]))

    if type_name == "large_list":
        assert len(args) == 1
        return pyarrow.large_list(make_parquet_type(args[0]))

    if type_name in (
        "int16",
        "int32",
        "uint16",
        "uint32",
        "float32",
        "date32",
        "date64",
        "month_day_nano_interval",
        "utf8",
        "large_binary",
        "large_string",
        "large_utf8",
        "null",
    ):
        assert not args
        return getattr(pyarrow, type_name)()

    if type_name in ("time32", "time64", "duration"):
        assert len(args) == 1
    elif type_name in ("timestamp", "decimal128"):
        assert len(args) in (1, 2)
    elif type_name in ("binary",):
        assert len(args) <= 1
    else:
        raise ValueError(f"unknown pyarrow type '{type_name}'")
    return getattr(pyarrow, type_name)(*args)


def make_parquet_field(field_spec: Sequence):
    """
    ``filed_spec`` is a list or tuple with 2, 3, or 4 elements.
    The first element is the name of the field.
    The second element is the spec of the type, to be passed to function :func:`make_parquet_type`.
    Additional elements are the optional ``nullable`` and ``metadata`` to the function
    `pyarrow.field() <https://arrow.apache.org/docs/python/generated/pyarrow.field.html#pyarrow.field>`_.
    """
    field_name = field_spec[0]
    type_spec = field_spec[1]
    assert len(field_spec) <= 4  # two optional elements are `nullable` and `metadata`.
    return pyarrow.field(field_name, make_parquet_type(type_spec), *field_spec[3:])


def make_parquet_schema(fields_spec: Iterable[Sequence]):
    """
    This function constructs a pyarrow schema that is expressed by simple Python types
    that can be json-serialized.

    ``fields_spec`` is a list or tuple, each of its elements accepted by :func:`make_parquet_field`.

    This function is motivated by the need of :class:`~biglist._biglist.ParquetSerializer`.
    When :class:`biglist.Biglist` uses a "storage-format" that takes options (such as 'parquet'),
    these options can be passed into :func:`biglist.Biglist.new` (via ``serialize_kwargs`` and ``deserialize_kwargs``) and saved in "info.json".
    However, this requires the options to be json-serializable.
    Therefore, the argument ``schema`` to :meth:`ParquetSerializer.serialize() <biglist._biglist.ParquetSerializer.serialize>`
    can not be used by this mechanism.
    As an alternative, user can use the argument ``schema_spec``;
    this argument can be saved in "info.json", and it is handled by this function.
    """
    return pyarrow.schema((make_parquet_field(v) for v in fields_spec))


def write_parquet_table(
    table: pyarrow.Table,
    path: PathType,
    **kwargs,
) -> None:
    """
    If the file already exists, it will be overwritten.

    Parameters
    ----------
    path
        Path of the file to create and write to.
    table
        pyarrow Table object.
    **kwargs
        Passed on to `pyarrow.parquet.write_table() <https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html>`_.
    """
    path = resolve_path(path)
    if isinstance(path, LocalUpath):
        path.parent.path.mkdir(exist_ok=True, parents=True)
    ff, pp = FileSystem.from_uri(str(path))
    if isinstance(ff, GcsFileSystem):
        ff = ParquetFileReader.get_gcsfs()
    pyarrow.parquet.write_table(table, ff.open_output_stream(pp), **kwargs)


def write_arrays_to_parquet(
    data: Sequence[pyarrow.Array | pyarrow.ChunkedArray | Iterable],
    path: PathType,
    *,
    names: Sequence[str],
    **kwargs,
) -> None:
    """
    Parameters
    ----------
    path
        Path of the file to create and write to.
    data
        A list of data arrays.
    names
        List of names for the arrays in ``data``.
    **kwargs
        Passed on to `pyarrow.parquet.write_table() <https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html>`_.
    """
    assert len(names) == len(data)
    arrays = [
        a if isinstance(a, (pyarrow.Array, pyarrow.ChunkedArray)) else pyarrow.array(a)
        for a in data
    ]
    table = pyarrow.Table.from_arrays(arrays, names=names)
    return write_parquet_table(table, path, **kwargs)


def write_pylist_to_parquet(
    data: Sequence,
    path: PathType,
    *,
    schema=None,
    schema_spec=None,
    metadata=None,
    **kwargs,
):
    if schema is not None:
        assert schema_spec is None
    elif schema_spec is not None:
        assert schema is None
        schema = make_parquet_schema(schema_spec)
    table = pyarrow.Table.from_pylist(data, schema=schema, metadata=metadata)
    return write_parquet_table(table, path, **kwargs)
