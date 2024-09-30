from __future__ import annotations

import atexit
import bisect
import concurrent.futures
import copy
import functools
import io
import itertools
import logging
import os
import queue
import string
import tempfile
import threading
import uuid
import warnings
import weakref
from abc import abstractmethod
from collections.abc import Iterable, Iterator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Callable, TypeVar
from uuid import uuid4

import pyarrow
from typing_extensions import Self
from upathlib import LocalUpath, PathType, Upath, resolve_path, serializer

from ._parquet import ParquetFileReader, make_parquet_schema
from ._util import Element, FileReader, Seq

logger = logging.getLogger(__name__)


_biglist_objs = weakref.WeakSet()


# TODO: rethink this.
def _cleanup():
    # Use this to guarantee proper execution of ``Biglist.__del__``.
    # An error case was observed in this scenario:
    #
    #   1. The ``self.path.rmrf()`` line executes;
    #   2. ``rmrf`` involves a background thread, in which task is submitted to a thread pool;
    #   3. ``RuntimeError: cannot schedule new futures after interpreter shutdown`` was raised.
    #
    # The module ``concurrent.futures.thread`` registers a ``atexit`` function, which sets a flag
    # saying "interpreter is shutdown". This module contains the ``ThreadPoolExecutor`` class and
    # of course its ``submit`` method. In ``submit``, if it sees the global "interpreter is shutdown"
    # flag is set, it raises the error above.
    #
    # Here, we register this function with ``atexit`` **after** the registration of the cleanup
    # function in the standard module, because at this time that standard module has been imported.
    # As a result, this cleanup function runs before the standard one, hence the call to ``rmrf``
    # finishes before the "interpreter is shutdown" is set.
    global _biglist_objs
    for x in _biglist_objs:
        x.__del__()


atexit.register(_cleanup)


_global_thread_pool_ = weakref.WeakValueDictionary()
_global_thread_pool_lock_ = threading.Lock()


def get_global_thread_pool():
    # Refer to ``get_shared_thread_pool`` in package ``mpservice.concurrent.futures``.

    with _global_thread_pool_lock_:
        executor = _global_thread_pool_.get('_biglist_')
        if executor is None or executor._shutdown:
            executor = concurrent.futures.ThreadPoolExecutor()
            _global_thread_pool_['_biglist_'] = executor
    return executor


if hasattr(os, 'register_at_fork'):  # this is not available on Windows

    def _clear_global_state():
        executor = _global_thread_pool_.get('_biglist_')
        if executor is not None:
            executor.shutdown(wait=False)
            _global_thread_pool_.pop('_biglist_', None)

        global _global_thread_pool_lock_
        try:
            _global_thread_pool_lock_.release()
        except RuntimeError:  # 'release unlocked lock'
            pass
        _global_thread_pool_lock_ = threading.Lock()

    os.register_at_fork(after_in_child=_clear_global_state)


FileReaderType = TypeVar('FileReaderType', bound=FileReader)
"""This type variable indicates the class :class:`FileReader` or a subclass thereof."""


class FileSeq(Seq[FileReaderType]):
    """
    A ``FileSeq`` is a :class:`Seq` of :class:`FileReader` objects.

    Since this class represents a sequence of data files,
    methods such as :meth:`__len__` and :meth:`__iter__` are in terms of data *files*
    rather than data *elements* in the files.
    (One data file contains a sequence of data elements.)
    """

    def __repr__(self):
        return f"<{self.__class__.__name__} at '{self.path}' with {self.num_data_items} elements in {self.num_data_files} data file(s)>"

    def __str__(self):
        return self.__repr__()

    @property
    @abstractmethod
    def data_files_info(self) -> list[tuple[str, int, int]]:
        """
        Return a list of tuples for the data files.
        Each tuple, representing one data file, consists of
        "file path", "element count in the file",
        and "cumulative element count in the data files so far".

        Implementation in a subclass should consider caching the value
        so that repeated calls are cheap.
        """
        raise NotImplementedError

    @property
    def num_data_files(self) -> int:
        """Number of data files."""
        return len(self.data_files_info)

    @property
    def num_data_items(self) -> int:
        """Total number of data items in the data files."""
        z = self.data_files_info
        if not z:
            return 0
        return z[-1][-1]

    def __len__(self) -> int:
        """Number of data files."""
        return self.num_data_files

    @abstractmethod
    def __getitem__(self, idx: int) -> FileReaderType:
        """
        Return the :class:`FileReader` for the data file at the specified
        (0-based) index. The returned FileReader object has not loaded data yet,
        and is guaranteed to be pickle-able.

        Parameters
        ----------
        idx
            Index of the file (0-based) in the list of data files as returned
            by :meth:`data_files_info`.
        """
        raise NotImplementedError

    def __iter__(self) -> Iterator[FileReaderType]:
        """
        Yield one data file at a time.

        .. seealso:: :meth:`__getitem__`
        """
        for i in range(self.__len__()):
            yield self.__getitem__(i)

    @property
    @abstractmethod
    def path(self) -> Upath:
        """
        Return the location (a "directory") where this object
        saves info about the data files, and any other info the implementation chooses
        to save.

        Note that this location does not need to be related to the location of the data files.
        """
        raise NotImplementedError


class BiglistBase(Seq[Element]):
    """
    This base class contains code mainly concerning *reading*.
    The subclass :class:`~biglist.Biglist` adds functionalities for writing.
    Another subclass :class:`~biglist.ParquetBiglist` is read-only.
    Here, "reading" and "read-only" is talking about the *data files*.
    This class always needs to write meta info *about* the data files.
    In addition, the subclass :class:`~biglist.Biglist` also creates and manages
    the data files, whereas :class:`~biglist.ParquetBiglist` provides methods
    to read existing data files, treating them as read-only.

    This class is generic with a parameter indicating the type of the data items,
    but this is useful only for the subclass :class:`~biglist.Biglist`.
    For the subclass :class:`~biglist.ParquetBiglist`, this parameter is essentially ``Any``
    because the data items (or rows) in Parquet files are composite and flexible.
    """

    @classmethod
    def get_temp_path(cls) -> Upath:
        """
        If user does not specify ``path`` when calling :meth:`new` (in a subclass),
        this method is used to determine a temporary directory.

        This implementation returns a temporary location in the local file system.

        Subclasses may want to customize this if they prefer other ways
        to find temporary locations. For example, they may want
        to use a temporary location in a cloud storage.
        """
        path = LocalUpath(os.path.abspath(tempfile.gettempdir()), str(uuid.uuid4()))  # type: ignore
        return path  # type: ignore

    @classmethod
    def new(
        cls,
        path: PathType | None = None,
        *,
        init_info: dict = None,
        **kwargs,
    ) -> BiglistBase:
        """
        Create a new object of this class (of a subclass, to be precise) and then add data to it.

        Parameters
        ----------
        path
            A directory in which this :class:`BiglistBase` will save whatever it needs to save.

            The directory must be non-existent.
            It is not necessary to pre-create the parent directory of this path.

            This path can be either on local disk or in a cloud storage.

            If not specified, :meth:`BiglistBase.get_temp_path`
            is called to determine a temporary path.

            The subclass :class:`~biglist.Biglist` saves both data and meta-info in this path.
            The subclass :class:`~biglist.ParquetBiglist` saves meta-info only.

        init_info
            Initial info that should be written into the *info* file before ``__init__`` is called.
            This is in addition to whatever this method internally decides to write.

            The info file `info.json` is written before :meth:`__init__` is called.
            In :meth:`__init__`, this file is read into ``self.info``.

            This parameter can be used to write some high-level info that ``__init__``
            needs.

            If the info is not needed in ``__init__``, then user can always add it
            to ``self.info`` after the object has been instantiated, hence saving general info
            in ``info.json`` is not the intended use of this parameter.

            User rarely needs to use this parameter. It is mainly used by the internals
            of the method ``new`` of subclasses.
        **kwargs
            additional arguments are passed on to :meth:`__init__`.

        Notes
        -----
        A :class:`BiglistBase` object construction is in either of the two modes
        below:

        a) create a new :class:`BiglistBase` to store new data.

        b) create a :class:`BiglistBase` object pointing to storage of
           existing data, which was created by a previous call to :meth:`new`.

        In case (a), one has called :meth:`new`. In case (b), one has called
        ``BiglistBase(..)`` (i.e. :meth:`__init__`).

        Some settings are applicable only in mode (a), b/c in
        mode (b) they can't be changed and, if needed, should only
        use the value already set in mode (a).
        Such settings can be parameters to :meth:`new`
        but should not be parameters to :meth:`__init__`.
        Examples include ``storage_format`` and ``batch_size`` for the subclass :class:`~biglist.Biglist`.
        These settings typically should be taken care of in :meth:`new`,
        before and/or after the object has been created by a call to :meth:`__init__`
        within :meth:`new`.

        :meth:`__init__` should be defined in such a way that it works for
        both a barebone object that is created in this :meth:`new`, as well as a
        fleshed out object that already has data in persistence.

        Some settings may be applicable to an existing :class:`BiglistBase` object, e.g.,
        they control styles of display and not intrinsic attributes persisted along
        with the BiglistBase.
        Such settings should be parameters to :meth:`__init__` but not to :meth:`new`.
        If provided in a call to :meth:`new`, these parameters will be passed on to :meth:`__init__`.

        Subclass authors should keep these considerations in mind.
        """

        if not path:
            path = cls.get_temp_path()
        path = resolve_path(path)
        if path.is_dir():
            raise Exception(f'directory "{path}" already exists')
        if path.is_file():
            raise FileExistsError(path)
        (path / 'info.json').write_json(init_info or {}, overwrite=False)
        obj = cls(path, **kwargs)
        return obj

    def __init__(
        self,
        path: PathType,
    ):
        """
        Parameters
        ----------
        path
            Directory that contains files written by an instance
            of this class.
        """
        self.path: Upath = resolve_path(path)
        """Root directory of the storage space for this object."""

        self._read_buffer: Seq[Element] | None = None
        self._read_buffer_file_idx = None
        self._read_buffer_item_range: tuple[int, int] | None = None
        # `self._read_buffer` contains the content of the file
        # indicated by `self._read_buffer_file_idx`.

        self._thread_pool_ = None

        self.info: dict
        """Various meta info."""

        self._info_file = self.path / 'info.json'
        self.info = self._info_file.read_json()
        self._n_read_threads = 3

    def __repr__(self):
        return f"<{self.__class__.__name__} at '{self.path}' with {self.num_data_items} elements in {self.num_data_files} data file(s)>"

    def __str__(self):
        return self.__repr__()

    def __len__(self) -> int:
        """
        Number of data items in this biglist.

        This is an alias to :meth:`num_data_items`.
        """
        return self.num_data_items

    def __getstate__(self):
        return (self.path,)

    def __setstate__(self, data):
        self.__init__(data[0])

    def _get_thread_pool(self):
        if self._thread_pool_ is None:
            self._thread_pool_ = get_global_thread_pool()
        return self._thread_pool_

    def destroy(self, *, concurrent=True) -> None:
        self.path.rmrf(concurrent=concurrent)

    def __getitem__(self, idx: int) -> Element:
        """
        Access a data item by its index; negative index works as expected.
        """
        # This is not optimized for speed.
        # For better speed, use ``__iter__``.

        if not isinstance(idx, int):
            raise TypeError(
                f'{self.__class__.__name__} indices must be integers, not {type(idx).__name__}'
            )

        if idx >= 0 and self._read_buffer_file_idx is not None:
            n1, n2 = self._read_buffer_item_range  # type: ignore
            if n1 <= idx < n2:
                return self._read_buffer[idx - n1]  # type: ignore

        files = self.files
        if files:
            data_files_cumlength = [v[-1] for v in files.data_files_info]
            length = data_files_cumlength[-1]
            nfiles = len(files)
        else:
            data_files_cumlength = []
            length = 0
            nfiles = 0
        idx = range(length)[idx]

        if idx >= length:
            raise IndexError(idx)

        ifile0 = 0
        ifile1 = nfiles
        if self._read_buffer_file_idx is not None:
            n1, n2 = self._read_buffer_item_range  # type: ignore
            if idx < n1:
                ifile1 = self._read_buffer_file_idx  # pylint: disable=access-member-before-definition
            elif idx < n2:
                return self._read_buffer[idx - n1]  # type: ignore
            else:
                ifile0 = self._read_buffer_file_idx + 1  # pylint: disable=access-member-before-definition

        # Now find the data file that contains the target item.
        ifile = bisect.bisect_right(data_files_cumlength, idx, lo=ifile0, hi=ifile1)
        # `ifile`: index of data file that contains the target element.
        # `n`: total length before `ifile`.
        if ifile == 0:
            n = 0
        else:
            n = data_files_cumlength[ifile - 1]
        self._read_buffer_item_range = (n, data_files_cumlength[ifile])

        data = files[ifile]
        self._read_buffer_file_idx = ifile
        self._read_buffer = data
        return data[idx - n]

    def __iter__(self) -> Iterator[Element]:
        """
        Iterate over all the elements.

        When there are multiple data files, as the data in one file is being yielded,
        the next file(s) may be pre-loaded in background threads.
        For this reason, although the following is equivalent in the final result::

            for file in self.files:
                for item in file:
                    ... use item ...

        it could be less efficient than iterating over `self` directly, as in

        ::

            for item in self:
                ... use item ...
        """
        files = self.files
        if not files:
            return

        if len(files) == 1:
            z = files[0]
            z.load()
            yield from z
        else:
            ndatafiles = len(files)

            max_workers = min(self._n_read_threads, ndatafiles)
            tasks = queue.Queue(max_workers)
            executor = self._get_thread_pool()

            def _read_file(idx):
                z = files[idx]
                z.load()
                return z

            for i in range(max_workers):
                t = executor.submit(_read_file, i)
                tasks.put(t)
            nfiles_queued = max_workers

            for _ in range(ndatafiles):
                t = tasks.get()
                file_reader = t.result()

                # Before starting to yield data, take care of the
                # downloading queue to keep it busy.
                if nfiles_queued < ndatafiles:
                    # `nfiles_queued` is the index of the next file to download.
                    t = executor.submit(_read_file, nfiles_queued)
                    tasks.put(t)
                    nfiles_queued += 1

                yield from file_reader

    @property
    def num_data_files(self) -> int:
        return len(self.files)

    @property
    def num_data_items(self) -> int:
        # This assumes the current object is the only one
        # that may be appending to the biglist.
        # In other words, if the current object is one of
        # of a number of workers that are concurrently using
        # this biglist, then all the other workers are reading only.
        files = self.files
        if files:
            return files.num_data_items
        return 0

    @property
    @abstractmethod
    def files(self) -> FileSeq[FileReader[Element]]:
        raise NotImplementedError


class Biglist(BiglistBase[Element]):
    registered_storage_formats = {
        'json': serializer.JsonSerializer,
        'pickle': serializer.PickleSerializer,
        'pickle-zstd': serializer.ZstdPickleSerializer,
    }

    DEFAULT_STORAGE_FORMAT = 'pickle-zstd'

    @classmethod
    def register_storage_format(
        cls,
        name: str,
        serializer: type[serializer.Serializer],
    ) -> None:
        """
        Register a new serializer to handle data file dumping and loading.

        This class has a few serializers registered out of the box.
        They should be adequate for most applications.

        Parameters
        ----------
        name
            Name of the format to be associated with the new serializer.

            After registering the new serializer with name "xyz", one can use
            ``storage_format='xyz'`` in calls to :meth:`new`.
            When reading the object back from persistence,
            make sure this registry is also in place so that the correct
            deserializer can be found.

        serializer
            A subclass of `upathlib.serializer.Serializer <https://github.com/zpz/upathlib/blob/main/src/upathlib/serializer.py>`_.

            Although this class needs to provide the ``Serializer`` API, it is possible to write data files in text mode.
            The registered 'json' format does that.
        """
        good = string.ascii_letters + string.digits + '-_'
        assert all(n in good for n in name)
        if name.replace('_', '-') in cls.registered_storage_formats:
            raise ValueError(f"serializer '{name}' is already registered")
        name = name.replace('_', '-')
        cls.registered_storage_formats[name] = serializer

    @classmethod
    def new(
        cls,
        path: PathType | None = None,
        *,
        batch_size: int | None = None,
        storage_format: str | None = None,
        serialize_kwargs: dict | None = None,
        deserialize_kwargs: dict | None = None,
        init_info: dict = None,
        **kwargs,
    ) -> Self:
        """
        Parameters
        ----------
        path
            Passed on to :meth:`BiglistBase.new`.
        batch_size
            Max number of data elements in each persisted data file.

            There's no good default value for this parameter, although one is
            provided (currently the default is 1000),
            because the code of :meth:`new` doesn't know
            the typical size of the data elements. User is recommended to
            specify the value of this parameter.

            In choosing a value for ``batch_size``, the most important
            consideration is the size of each data file, which is determined
            by the typical size of the data elements as well as ``batch_size``,
            which is the upper bound of the the number of elements in each file.

            There are several considerations about the data file sizes:

            - It should not be so small that the file reading/writing is a large
              overhead relative to actual processing of the data.
              This is especially important when ``path`` is cloud storage.

            - It should not be so large that it is "unwieldy", e.g. approaching 1GB.

            - When :meth:`__iter__`\\ating over a :class:`Biglist` object, there can be up to (by default) 4
              files-worth of data in memory at any time, where 4 is ``self._n_read_threads`` plus 1.

            - When :meth:`append`\\ing or :meth:`extend`\\ing to a :class:`Biglist` object at high speed,
              there can be up to (by default) 9 times ``batch_size`` data elements in memory at any time,
              where 9 is ``self._n_write_threads`` plus 1.
              See :meth:`_flush` and :class:`~biglist._biglist.Dumper`.

            Another consideration is access pattern of elements in the :class:`Biglist`. If
            there are many "jumping around" with random element access, large data files
            will lead to very wasteful file loading, because to read any element,
            its hosting file must be read into memory. (After all, if the application is
            heavy on random access, then :class:`Biglist` is **not** the right tool.)

            The performance of iteration is not expected to be highly sensitive to the value
            of ``batch_size``, as long as it is in a reasonable range.

            A rule of thumb: it is recommended to keep the persisted files between 32-128MB
            in size. (Note: no benchmark was performed to back this recommendation.)
        storage_format
            This should be a key in :data:`registered_storage_formats`.
            If not specified, :data:`DEFAULT_STORAGE_FORMAT` is used.
        serialize_kwargs
            Additional keyword arguments to the serialization function.
        deserialize_kwargs
            Additional keyword arguments to the deserialization function.

            ``serialize_kwargs`` and ``deserialize_kwargs`` are rarely needed.
            One use case is ``schema`` when storage format is "parquet".
            See :class:`~biglist._biglist.ParquetSerializer`.

            ``serialize_kwargs`` and ``deserialize_kwargs``, if not ``None``,
            will be saved in the "info.json" file, hence they must be JSON
            serializable, meaning they need to be the few simple native Python
            types that are supported by the standard ``json`` library.
            (However, the few formats "natively" supported by Biglist may get special treatment
            to relax this requirement.)
            If this is not possible, the solution is to define a custom serialization class and
            register it with :meth:`register_storage_format`.
        **kwargs
            additional arguments are passed on to :meth:`BiglistBase.new`.

        Returns
        -------
        Biglist
            A new :class:`Biglist` object.
        """
        if not batch_size:
            batch_size = 1000
            warnings.warn(
                'The default batch-size, 1000, may not be optimal for your use case; consider using the parameter ``batch_size``.'
            )
        else:
            assert batch_size > 0

        if storage_format is None:
            storage_format = cls.DEFAULT_STORAGE_FORMAT
        if storage_format.replace('_', '-') not in cls.registered_storage_formats:
            raise ValueError(f"invalid value of `storage_format`: '{storage_format}'")

        init_info = {
            **(init_info or {}),
            'storage_format': storage_format.replace('_', '-'),
            'storage_version': 3,
            # `storage_version` is a flag for certain breaking changes in the implementation,
            # such that certain parts of the code (mainly concerning I/O) need to
            # branch into different treatments according to the version.
            # This has little relation to `storage_format`.
            # version 0 designator introduced on 2022/3/8
            # version 1 designator introduced on 2022/7/25
            # version 2 designator introduced in version 0.7.4.
            # version 3 designator introduced in version 0.7.7.
            'batch_size': batch_size,
            'data_files_info': [],
        }
        if serialize_kwargs:
            init_info['serialize_kwargs'] = serialize_kwargs
        if deserialize_kwargs:
            init_info['deserialize_kwargs'] = deserialize_kwargs

        obj = super().new(path, init_info=init_info, **kwargs)  # type: ignore
        return obj

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        """
        Please see the base class for additional documentation.
        """
        super().__init__(*args, **kwargs)

        self._append_buffer: list = []
        self._append_files_buffer: list = []
        self._file_dumper = None

        self._n_write_threads = 4
        """This value affects memory demand during quick "appending" (and flushing/dumping in the background).
        If the memory consumption of each batch is large, you could manually set this to a lower value, like::

            lst = Biglist(path)
            lst._n_write_threads = 4
        """

        self._serialize_kwargs = self.info.get('serialize_kwargs', {})
        self._deserialize_kwargs = self.info.get('deserialize_kwargs', {})
        if self.storage_format == 'parquet' and 'schema_spec' in self._serialize_kwargs:
            # Build the schema so that it does not need to be done each time the function
            # ``ParquetSerializer.serialize`` is called. Maybe this does not matter.
            assert 'schema' not in self._serialize_kwargs
            kk = copy.deepcopy(self._serialize_kwargs)
            kk['schema'] = make_parquet_schema(kk['schema_spec'])
            del kk['schema_spec']
            self._serialize_kwargs = kk

        _biglist_objs.add(self)

        # For back compat.
        if self.info.get('storage_version', 0) < 3:
            # This is not called by ``new``, instead is opening an existing dataset.
            # Usually these legacy datasets are in a "read-only" mode, i.e., you should
            # not append more data to them. If you do, the back-compat code below
            # may not be totally reliable if the dataset is being used by multiple workers
            # concurrently.
            if 'data_files_info' not in self.info:  # added in 0.7.4
                if self.storage_version == 0:
                    # This may not be totally reliable in every scenario.
                    # The older version had a parameter `lazy`, which is gone now.
                    # After some time we may stop supporting this storage version. (7/27/2022)
                    # However, as long as older datasets are in a "read-only" status,
                    # this is fine.
                    try:
                        data_info_file = self.path / 'datafiles_info.json'
                        data_files = data_info_file.read_json()
                        # A list of tuples, (file_name, item_count)
                    except FileNotFoundError:
                        data_files = []
                elif self.storage_version == 1:
                    # Starting with storage_version 1, data file name is
                    #   <timestamp>_<uuid>_<itemcount>.<ext>
                    # <timestamp> contains a '.', no '_';
                    # <uuid> contains '-', no '_';
                    # <itemcount> contains no '-' nor '_';
                    # <ext> may contain '_'.
                    files0 = (v.name for v in self.data_path.iterdir())
                    files1 = (v.split('_') + [v] for v in files0)
                    files2 = (
                        (float(v[0]), v[-1], int(v[2].partition('.')[0]))
                        # timestamp, file name, item count
                        for v in files1
                    )
                    files = sorted(files2)  # sort by timestamp

                    if files:
                        data_files = [
                            (v[1], v[2]) for v in files
                        ]  # file name, item count
                    else:
                        data_files = []
                else:
                    pass
                    # `storage_version == 2 already has `data_files_info` in `self.info`,
                    # but its format may be bad, hence it's included below.

                if data_files:
                    data_files_cumlength = list(
                        itertools.accumulate(v[1] for v in data_files)
                    )
                    data_files_info = [
                        (filename, count, cumcount)
                        for (filename, count), cumcount in zip(
                            data_files, data_files_cumlength
                        )
                    ]
                    # Each element of the list is a tuple containing file name, item count in file, and cumsum of item counts.
                else:
                    data_files_info = []

                self.info['data_files_info'] = data_files_info
                with self._info_file.lock() as ff:
                    ff.write_json(self.info, overwrite=True)

            else:
                # Added in 0.7.5: check for a bug introduced in 0.7.4.
                # Convert full path to file name.
                # Version 0.7.4 was used very briefly, hence very few datasets
                # were created by that version.
                data_files_info = self.info['data_files_info']
                if data_files_info:
                    new_info = None
                    if os.name == 'nt' and '\\' in data_files_info[0][0]:
                        new_info = [
                            (f[(f.rfind('\\') + 1) :], *_) for f, *_ in data_files_info
                        ]
                    elif '/' in data_files_info[0][0]:
                        new_info = [
                            (f[(f.rfind('/') + 1) :], *_) for f, *_ in data_files_info
                        ]
                    if new_info:
                        self.info['data_files_info'] = new_info
                        with self._info_file.lock() as ff:
                            ff.write_json(self.info, overwrite=True)

        self._info_backup = copy.deepcopy(self.info)

    def __del__(self) -> None:
        if self._info_file.is_file():  # otherwise `destroy()` may have been called
            if self._warn_flush('__del__'):
                self.flush()

    @property
    def batch_size(self) -> int:
        """The max number of data items in one data file."""
        return self.info['batch_size']

    @property
    def data_path(self) -> Upath:
        return self.path / 'store'

    @property
    def storage_format(self) -> str:
        """The value of ``storage_format`` used in :meth:`new`, either user-specified or the default value."""
        return self.info['storage_format'].replace('_', '-')

    @property
    def storage_version(self) -> int:
        """The internal format used in persistence. This is a read-only attribute for information only."""
        return self.info.get('storage_version', 0)

    def _warn_flush(self, source: str):
        if (
            self._append_buffer
            or self._append_files_buffer
            or self._info_backup != self.info
        ):
            # This warning fires if changed made by this object is not yet
            # fully flushed. This does not consider changed made by other objects
            # pointing to the same underlying dataset.
            #
            # Unless you know what you are doing, don't use `flush(eager=True)`.

            warnings.warn(
                f"did you forget to flush {self.__class__.__name__} at '{self.path}' (about to call `{source}`)?"
            )
            return True
        return False

    def __len__(self) -> int:
        """
        Number of data items in this biglist.

        If data is being appended to this biglist, then this method only includes the items
        that have been "flushed" to storage. Data items in the internal memory buffer
        are not counted. The buffer is empty upon calling :meth:`_flush` (internally and automatically)
        or :meth:`flush` (explicitly by user).

        .. versionchanged:: 0.7.4
            In previous versions, this count includes items that are not yet flushed.
        """
        self._warn_flush('__len__')
        return super().__len__()

    def __getitem__(self, idx: int) -> Element:
        """
        Access a data item by its index; negative index works as expected.

        Items not yet "flushed" are not accessible by this method.
        They are considered "invisible" to this method.
        Similarly, negative ``idx`` operates in the range of flushed items only.

        .. versionchanged:: 0.7.4
            In previous versions, the accessible items include those that are not yet flushed.
        """
        return super().__getitem__(idx)

    def __iter__(self) -> Iterator[Element]:
        """
        Iterate over all the elements.

        Items that are not yet "flushed" are invisible to this iteration.

        .. versionchanged:: 0.7.4
            In previous versions, this iteration includes those items that are not yet flushed.
        """
        self._warn_flush('__iter__')
        return super().__iter__()

    def append(self, x: Element) -> None:
        """
        Append a single element to the :class:`Biglist`.

        In implementation, this appends to an in-memory buffer.
        Once the buffer size reaches :data:`batch_size`, the buffer's content
        will be persisted as a new data file, and the buffer will re-start empty.
        In other words, whenever the buffer is non-empty,
        its content is not yet persisted.

        You can append data to a common biglist from multiple processes.
        In the processes, use independent ``Biglist`` objects that point to the same "path".
        Each of the objects will maintain its own in-memory buffer and save its own files once the buffer
        fills up. Remember to :meth:`flush` at the end of work in each process.
        """
        self._append_buffer.append(x)
        if len(self._append_buffer) >= self.batch_size:
            self._flush()

    def extend(self, x: Iterable[Element]) -> None:
        """This simply calls :meth:`append` repeatedly."""
        for v in x:
            self.append(v)

    def make_file_name(self, buffer_len: int, extra: str = '') -> str:
        """
        This method constructs the file name of a data file.
        If you need to customize this method for any reason, you should do it via ``extra``
        and keep the other patterns unchanged.
        The string ``extra`` will appear between other fixed patterns in the file name.

        One possible usecase is this: in distributed writing, you want files written by different workers
        to be distinguishable by the file names. Do something like this::

              def worker(datapath: str, worker_id: str, ...):
                  out = Biglist(datapath)
                  _make_file_name = out.make_file_name
                  out.make_file_name = lambda buffer_len: _make_file_name(buffer_len, worker_id)
                  ...
        """
        if extra:
            extra = extra.lstrip('_').rstrip('_') + '_'
        return f"{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S.%f')}_{extra}{str(uuid4()).replace('-', '')[:16]}_{buffer_len}"
        # File name pattern introduced on 7/25/2022.
        # This should guarantee the file name is unique, hence
        # we do not need to verify that this file name is not already used.
        # Also include timestamp and item count in the file name, in case
        # later we decide to use these pieces of info.
        # Changes in 0.7.4: the time part changes from epoch to datetime, with guaranteed fixed length.
        # Change in 0.8.4: the uuid part has dash removed and length reduced to 10; add ``extra``.
        # Change in 0.9.5: keep 16 digits of the uuid4 str, instead of the previous 10.

    def _flush(self) -> None:
        """
        Persist the content of the in-memory buffer to a file,
        reset the buffer, and update relevant book-keeping variables.

        This method is called any time the size of the in-memory buffer
        reaches ``self.batch_size``. This happens w/o the user's intervention.
        """
        if not self._append_buffer:
            return

        buffer = self._append_buffer
        buffer_len = len(buffer)
        self._append_buffer = []

        datafile_ext = self.storage_format.replace('-', '_')
        filename = f'{self.make_file_name(buffer_len)}.{datafile_ext}'

        data_file = self.data_path / filename

        if self._file_dumper is None:
            self._file_dumper = Dumper(self._get_thread_pool(), self._n_write_threads)
        self._file_dumper.dump_file(
            self.registered_storage_formats[self.storage_format].dump,
            buffer,
            data_file,
            **self._serialize_kwargs,
        )
        # This call will return quickly if the dumper has queue
        # capacity for the file. The file meta data below
        # will be updated as if the saving has completed, although
        # it hasn't (it is only queued). If dumping failed, the entry
        # will be deleted in `flush()`.

        self._append_files_buffer.append((filename, buffer_len))

    def flush(
        self,
        *,
        lock_timeout=300,
        raise_on_write_error: bool = True,
        eager: bool = False,
    ) -> None:
        """
        The persisted biglist consists of a number of data files and an overall meta info file
        (``info.json`` in the root of ``self.path``).
        The latter contains, among others, a listing of the data files so that the data elements
        have a defined order. Only data files listed in the info file are visible to reading.

        When user adds data via :meth:`append` or :meth:`extend`,
        :meth:`_flush` is called automatically whenever the "append buffer"
        is full, so to persist the data and empty the buffer.
        (The capacity of this buffer is equal to ``self.batch_size``.)
        If multiple Biglist objects in threads, processes, or machines add data concurrently,
        each object has its own append buffer and does `_flush` independent of other objects.
        A data file has a random name (comprised of datetime accurate to sub-seconds,
        plus a random string, plus other things);
        there is essentially no risk of name clash when multiple Biglist objects save data files independent of
        each other.

        However, there are two things that the automatic `_flush` does not do:

        - First, if the append buffer is only partially filled when the user (of one Biglist object)
        is done adding elements to the biglist, the data in the buffer will not be persisted.

        - Second, `_flush` does not add new data files it has created into the meta info file.
        It does not do so because doing it would need to lock the info file, which adds overhead
        and harms concurrent independent writing.

        These two things are left to the user to do via explicit calls to :meth:`flush`.

        A Biglist object should call :meth:`flush` at the end of its data writing session,
        regardless of whether all the new data have been persisted in data files.
        (They would be if their count happens to be a multiple of ``self.batch_size``.)
        This will flush the append buffer.

        By default, `flush` also adds newly created data files to the meta info file.
        (Until that point, all the new data files created by the particular Biglist object
        are recorded in memory.) This operation locks the info file so that concurrent
        writers will not corrupt it.

        The parameter `eager` (default `False`) gives this op a twist. If `eager` is `True`,
        the list of new data files created by `self`--that is, this calling Biglist object--is written to a small "interim" file,
        and the meta info file is *not* updated. The interim file has a random name with no risk
        of name clash between multiple writers. In sum, `flush(eager=True)` persists all data and info
        but puts the data structure in an "interim" state. Importantly, this op does *not*
        involve locking, because it does not update the meta info file.
        The parameter `eager` is provided to manage the lock overhead when we write to a
        cloud-persisted biglist using many concurrent, distributed writers.

        A call to `flush()` (i.e., `flush(eager=False)`) does all that `flush(eager=True)` does, plus
        it integrates the content of all interim files, if any, into the meta info file,
        and deletes the interim files. This op does lock the info file.
        Multiple interim files may have been created by multiple writers.
        One call to `flush()` will take care of all the interim files in existence.
        This call can be made from any Biglist object as long as it points to the same path.

        Unless you know what you are doing, don't use `flush(eager=True)`.

        User should assume that data not yet fully persisted via `flush`
        are not visible to data reading via :meth:`__getitem__` or :meth:`__iter__`,
        and are not included in :meth:`__len__`, even to the same Biglist object that has performed writing.
        In common use cases, we do not start reading data until we're done adding data
        to the biglist (at least "for now"), hence this is not a big inconvenience.

        In summary, call :meth:`flush` when

        - You are done adding data (for this "session"),
        - or you need to start reading data.

        After a call to ``flush()``, there's no problem to add more elements again by
        :meth:`append` or :meth:`extend`. Data files created by ``flush()`` with less than
        :data:`batch_size` elements will stay as is among larger files.
        This is a legitimate case in parallel or distributed writing, or writing in
        multiple sessions.

        User is strongly recommended to explicitly call `flush` at the end of their writing session.
        (See :meth:`_warn_flush`.)

        On the other hand, you should **not** call `flush` frequently "just to be safe".
        It has I/O overhead, and it may create small data files because it flushes the append buffer
        regardless of whether it is full.
        """
        self._flush()
        if self._file_dumper is not None:
            errors = self._file_dumper.wait(raise_on_error=raise_on_write_error)
            if errors:
                for file, e in errors:
                    logger.error('failed to write file %s: %r', file, e)
                    fname = file.name
                    for i, (f, _) in enumerate(self._append_files_buffer):
                        if f == fname:
                            self._append_files_buffer.pop(i)
                            break
                    if file.exists():
                        try:
                            file.remove_file()
                        except Exception as e:
                            logger.error('failed to delete file %s: %r', file, e)

        # Other workers in other threads, processes, or machines may have appended data
        # to the list. This block merges the appends by the current worker with
        # appends by other workers. The last call to ``flush`` across all workers
        # will get the final meta info right.

        def _merge_data_file_info(info, additions):
            z = sorted(set((*(tuple(v[:2]) for v in info), *map(tuple, additions))))
            # TODO: maybe a merge sort can be more efficient.
            cum = list(itertools.accumulate(v[1] for v in z))
            z = [(a, b, c) for (a, b), c in zip(z, cum)]
            return z

        if eager:
            if self._append_files_buffer:
                # Saving file meta data without merging it into `info.json`.
                # This puts the on-disk data structure in a transitional state.
                filename = getattr(self, '_flush_eager_file', None)
                if not filename:
                    filename = f"{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S.%f')}_{str(uuid4()).replace('-', '')}"
                    (self.path / '_flush_eager' / filename).write_json(
                        self._append_files_buffer, overwrite=False
                    )
                    self._flush_eager_file = filename
                else:
                    try:
                        z = (self.path / '_flush_eager' / filename).read_json()
                    except FileNotFoundError:
                        (self.path / '_flush_eager' / filename).write_json(
                            self._append_files_buffer, overwrite=False
                        )
                    else:
                        # Accumulate the existing file with new file info.
                        z.extend(self._append_files_buffer)
                        (self.path / '_flush_eager' / filename).write_json(
                            z, overwrite=True
                        )

                    # This file contains info of all files written by this object so far.
                    # Although this file has been written previously by this object,
                    # the file may not exist. Another object for the same biglist could have
                    # called `flush`, which would have incorporated all these files into meta info
                    # and deleted these files.
                self.info['data_files_info'] = _merge_data_file_info(
                    self.info['data_files_info'], self._append_files_buffer
                )
                # Update the info to reflect the data writings by this object.
                self._append_files_buffer.clear()
                self._info_backup['data_files_info'] = copy.deepcopy(
                    self.info['data_files_info']
                )
            return

        data = []
        if self._append_files_buffer:
            # Do not update this object's eager file, which contains info of files written by this object
            # previously (not including the content of `self._append_files_buffer`).
            # Take care of `self._append_files_buffer` directly in the meta info file.
            data.extend(self._append_files_buffer)
            self._append_files_buffer.clear()

        # Merge data-file meta data into `info.json`, finalizing the persisted data structure.
        with self._info_file.lock(timeout=lock_timeout) as ff:
            # The info file may have been updated by another object for the same biglist.
            self.info.update(ff.read_json())

            for f in (self.path / '_flush_eager').iterdir():
                z = f.read_json()
                data.extend(z)
                f.remove_file()
            if data:
                self.info['data_files_info'] = _merge_data_file_info(
                    self.info['data_files_info'],
                    data,
                )
                ff.write_json(self.info, overwrite=True)
            self._info_backup = copy.deepcopy(self.info)

    def reload(self) -> None:
        """
        Reload the meta info.

        This is used in this scenario: suppose we have this object pointing to a biglist
        on the local disk; another object in another process is appending data to the same biglist
        (that is, it points to the same storage location); then after a while, the meta info file
        on the disk has been modified by the other process, hence the current object is out-dated;
        calling this method will bring it up to date. The same idea applies if the storage is
        in the cloud, and another machine is appending data to the same remote biglist.

        Creating a new object pointing to the same storage location would achieve the same effect.
        """
        self.info = self._info_file.read_json()
        self._info_backup = copy.deepcopy(self.info)

    @property
    def files(self):
        # This method should be cheap to call.
        # TODO: call `reload`?
        self._warn_flush('files')
        serde = self.registered_storage_formats[self.storage_format]
        fun = serde.load
        if self._deserialize_kwargs:
            fun = functools.partial(fun, **self._deserialize_kwargs)
        return BiglistFileSeq(
            self.path,
            [
                (str(self.data_path / row[0]), *row[1:])
                for row in self.info['data_files_info']
            ],
            fun,
        )


class Dumper:
    """
    This class performs file-saving in a thread pool.

    Parameters
    ----------
    n_threads
        Max number of threads to use. There are at most
        this many submitted and unfinished file-dumping tasks
        at any time.
    """

    def __init__(self, executor: ThreadPoolExecutor, n_threads: int):
        self._executor: executor = executor
        self._n_threads = n_threads
        self._sem: threading.Semaphore | None = None  # type: ignore
        self._tasks: set[Future] = set()

    def _callback(self, t):
        self._sem.release()
        if not t.exception():
            self._tasks.remove(t)
        # If `t` raised exception, keep it in `self._tasks`
        # so that the exception can be re-raised in `self.wait`.

    def dump_file(
        self,
        file_dumper: Callable[[list, Upath], None],
        data: list,
        data_file: Upath,
        **kwargs,
    ):
        """
        Parameters
        ----------
        file_dumper
            This function takes the data as a list and a file path, and saves
            the data in the file named by the path.

        data_file, data
            Parameters to ``file_dumper``.
        """
        if self._sem is None:
            self._sem = threading.Semaphore(
                min(self._n_threads, self._executor._max_workers)
            )
        self._sem.acquire()
        # Wait here if the executor is busy at capacity.
        # The `Dumper` object runs in the same thread as the `Biglist` object,
        # hence if it's waiting for the semaphore here, it is blocking
        # further actions of the `Biglist` and waiting for one file-dumping
        # to finish.

        task = self._executor.submit(file_dumper, data, data_file, **kwargs)
        task._file = data_file
        self._tasks.add(task)
        task.add_done_callback(self._callback)
        # If task is already finished when this callback is being added,
        # then it is called immediately.

    def wait(self, *, raise_on_error: bool = True):
        """
        Wait to finish all the submitted dumping tasks.
        """
        if self._tasks:
            concurrent.futures.wait(self._tasks)
            if raise_on_error:
                for t in self._tasks:
                    _ = t.result()
            else:
                errors = []
                for t in self._tasks:
                    try:
                        _ = t.result()
                    except Exception as e:
                        errors.append((t._file, e))
                return errors


class BiglistFileReader(FileReader[Element]):
    def __init__(self, path: PathType, loader: Callable[[Upath], Any]):
        """
        Parameters
        ----------
        path
            Path of a data file.
        loader
            A function that will be used to load the data file.
            This must be pickle-able.
            Usually this is the bound method ``load`` of  a subclass of :class:`upathlib.serializer.Serializer`.
            If you customize this, please see the doc of :class:`~biglist.FileReader`.
        """
        super().__init__()
        self.path: Upath = resolve_path(path)
        self.loader = loader
        self._data: list | None = None

    def __getstate__(self):
        return self.path, self.loader

    def __setstate__(self, data):
        self.path, self.loader = data
        self._data = None

    def load(self) -> None:
        if self._data is None:
            self._data = self.loader(self.path)

    def data(self) -> list[Element]:
        """Return the data loaded from the file."""
        self.load()
        return self._data

    def __len__(self) -> int:
        return len(self.data())

    def __getitem__(self, idx: int) -> Element:
        return self.data()[idx]

    def __iter__(self) -> Iterator[Element]:
        return iter(self.data())


class BiglistFileSeq(FileSeq[BiglistFileReader]):
    def __init__(
        self,
        path: Upath,
        data_files_info: list[tuple[str, int, int]],
        file_loader: Callable[[Upath], Any],
    ):
        """
        Parameters
        ----------
        path
            Root directory for storage of meta info.
        data_files_info
            A list of data files that constitute the file sequence.
            Each tuple in the list is comprised of a file path,
            number of data items in the file, and cumulative
            number of data items in the files up to the one at hand.
            Therefore, the order of the files in the list is significant.
        file_loader
            Function that will be used to load a data file.
        """
        self._root_dir = path
        self._data_files_info = data_files_info
        self._file_loader = file_loader

    @property
    def path(self):
        return self._root_dir

    @property
    def data_files_info(self):
        return self._data_files_info

    def __getitem__(self, idx: int):
        file = self._data_files_info[idx][0]
        return BiglistFileReader(file, self._file_loader)


class ParquetSerializer(serializer.Serializer):
    @classmethod
    def serialize(
        cls,
        x: list[dict],
        schema: pyarrow.Schema = None,
        schema_spec: Sequence = None,
        metadata=None,
        **kwargs,
    ):
        """
        `x` is a list of data items. Each item is a dict. In the output Parquet file,
        each item is a "row".

        The content of the item dict should follow a regular pattern.
        Not every structure is supported. The data `x` must be acceptable to
        `pyarrow.Table.from_pylist <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.from_pylist>`_. If unsure, use a list with a couple data elements
        and experiment with ``pyarrow.Table.from_pylist`` directly.

        When using ``storage_format='parquet'`` for :class:`Biglist`, each data element is a dict
        with a consistent structure that is acceptable to ``pyarrow.Table.from_pylist``.
        When reading the Biglist, the original Python data elements are returned.
        (A record read out may not be exactly equal to the original that was written, in that
        elements that were missing in a record when written may have been filled in with ``None``
        when read back out.)
        In other words, the reading is *not* like that of :class:`~biglist.ParquetBiglist`.
        You can always create a separate ParquetBiglist for the data files of the Biglist
        in order to use Parquet-style data reading. The data files are valid Parquet files.

        If neither ``schema`` nor ``schema_spec`` is specified, then the data schema is auto-inferred
        based on the first element of ``x``. If this does not work, you can specify either ``schema`` or ``schema_spec``.
        The advantage of ``schema_spec`` is that it is json-serializable Python types, hence can be passed into
        :meth:`Biglist.new() <biglist.Biglist.new>` via ``serialize_kwargs`` and saved in "info.json" of the biglist.

        If ``schema_spec`` is not flexible or powerful enough for your usecase, then you may have to use ``schema``.
        """
        if schema is not None:
            assert schema_spec is None
        elif schema_spec is not None:
            assert schema is None
            schema = make_parquet_schema(schema_spec)
        table = pyarrow.Table.from_pylist(x, schema=schema, metadata=metadata)
        sink = io.BytesIO()
        writer = pyarrow.parquet.ParquetWriter(sink, table.schema, **kwargs)
        writer.write_table(table)
        writer.close()
        sink.seek(0)
        # return sink.getvalue()  # bytes
        # return sink.getbuffer()  # memoryview
        return sink
        # this is a file-like object; `upathlib.LocalUpath.write_bytes` and `upathlib.GcsBlobUpath.write_bytes` accept this.
        # We do not returnn the bytes because `upathlib.GcsBlobUpath.write_bytes` can take file-like objects directly.

    @classmethod
    def deserialize(cls, y: bytes, **kwargs):
        try:
            memoryview(y)
        except TypeError:
            pass  # `y` is a file-like object
        else:
            # `y` is a bytes-like object
            y = io.BytesIO(y)
        table = pyarrow.parquet.ParquetFile(y, **kwargs).read()
        return table.to_pylist()


Biglist.register_storage_format('parquet', ParquetSerializer)


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
        suffix: str = '.parquet',
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
        if isinstance(data_path, PathType):
            data_path = [resolve_path(data_path)]
        else:
            data_path = [resolve_path(p) for p in data_path]

        def get_file_meta(p: Upath):
            ff = ParquetFileReader.load_file(p)
            meta = ff.metadata
            return {
                'path': str(p),  # str of full path
                'num_rows': meta.num_rows,
                # "row_groups_num_rows": [
                #     meta.row_group(k).num_rows for k in range(meta.num_row_groups)
                # ],
            }

        pool = get_global_thread_pool()
        tasks = []
        for p in data_path:
            if p.is_file():
                if suffix == '*' or p.name.endswith(suffix):
                    tasks.append(pool.submit(get_file_meta, p))
            else:
                tt = []
                for pp in p.riterdir():
                    if suffix == '*' or pp.name.endswith(suffix):
                        tt.append((str(pp), pool.submit(get_file_meta, pp)))
                tt.sort()
                for p, t in tt:
                    tasks.append(t)

        assert tasks
        datafiles = []
        for k, t in enumerate(tasks):
            datafiles.append(t.result())
            if (k + 1) % 1000 == 0:
                logger.info('processed %d files', k + 1)

        datafiles_cumlength = list(
            itertools.accumulate(v['num_rows'] for v in datafiles)
        )

        obj = super().new(path, **kwargs)  # type: ignore
        obj.info['datapath'] = [str(p) for p in data_path]

        # Removed in 0.7.4
        # obj.info["datafiles"] = datafiles
        # obj.info["datafiles_cumlength"] = datafiles_cumlength

        # Added in 0.7.4
        data_files_info = [
            (a['path'], a['num_rows'], b)
            for a, b in zip(datafiles, datafiles_cumlength)
        ]
        obj.info['data_files_info'] = data_files_info

        obj.info['storage_format'] = 'parquet'
        obj.info['storage_version'] = 1
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

        # For back compat. Added in 0.7.4.
        if self.info and 'data_files_info' not in self.info:
            # This is not called by ``new``, instead is opening an existing dataset
            assert self.storage_version == 0
            data_files_info = [
                (a['path'], a['num_rows'], b)
                for a, b in zip(
                    self.info['datafiles'], self.info['datafiles_cumlength']
                )
            ]
            self.info['data_files_info'] = data_files_info
            with self._info_file.lock() as ff:
                ff.write_json(self.info, overwrite=True)

    def __repr__(self):
        return f"<{self.__class__.__name__} at '{self.path}' with {len(self)} records in {len(self.files)} data file(s) stored at {self.info['datapath']}>"

    @property
    def storage_version(self) -> int:
        return self.info.get('storage_version', 0)

    @property
    def files(self):
        # This method should be cheap to call.
        return ParquetFileSeq(
            self.path,
            self.info['data_files_info'],
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
