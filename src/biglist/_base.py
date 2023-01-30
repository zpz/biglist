from __future__ import annotations

import bisect
import logging
import os
import queue
import tempfile
import uuid
import warnings
import weakref
from abc import abstractmethod
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import (
    Any,
    Callable,
    Optional,
    TypeVar,
)

from deprecation import deprecated
from upathlib import LocalUpath, PathType, Upath, resolve_path

from ._util import Element, Seq, Slicer

logger = logging.getLogger(__name__)


_thread_pool = None


def _get_thread_pool():
    """
    If this function is used in multiple processes, **you must use the "spawn" method
    to start the processes**. This is related to data copying in "fork" processes
    (the default on Linux), and a ``ThreadPoolExecutor`` copied into
    the new processes is malfunctional.

    I do not know whether it is possible to detect such corrupt thread pools.
    If there is a way to do that, then it can be leveraged in this function to make
    things work with forked processes.
    """
    global _thread_pool
    if _thread_pool is None or _thread_pool() is None:
        pool = ThreadPoolExecutor(min(32, (os.cpu_count() or 1) + 4))
        pool.__enter__()
        _thread_pool = weakref.ref(pool, lambda ref: pool.shutdown())
    return _thread_pool()


class FileReader(Seq[Element]):
    """
    A FileReader is a "lazy" loader of a data file.
    It keeps track of the path of a data file along with a loader function,
    but performs the loading only when needed.
    In particular, upon initiation of a FileReader object,
    file loading has not happened, and the object
    is light weight and friendly to pickling.

    Once data have been loaded, this class provides various ways to navigate
    the data. At a minimum, the :class:`Seq` API is implemented.

    With loaded data and associated facilities, this object may no longer
    be pickle-able, depending on the specifics of the subclass.

    One use case of this class is to pass around FileReader objects
    (that are initiated but not loaded) in
    `multiprocessing <https://docs.python.org/3/library/multiprocessing.html>`_ code for concurrent data processing.

    This class is generic with a parameter indicating the type of the elements in the data sequence
    contained in the file.
    For example you can write::

        def func(file_reader: FileReader[int]):
            ...
    """

    def __init__(self, path: PathType, loader: Callable[[Upath], Any]):
        """
        Parameters
        ----------
        path
            Path of the data file.
        loader
            A function that will be used to load the data file.
            This must be pickle-able.
        """
        self.path: Upath = resolve_path(path)
        """Path of the file."""
        self.loader: Callable[[Upath], Any] = loader
        """A function that will be used to read the data file."""

    def __repr__(self):
        return f"<{self.__class__.__name__} for '{self.path}'>"

    def __str__(self):
        return self.__repr__()

    @abstractmethod
    def load(self) -> None:
        """
        This method *eagerly* loads all the data from the file.

        Once this method has been called, typically the entire data file is loaded
        into memory, and subsequent data consumption should all draw upon this
        in-memory copy. However, if the data file is large, and especially
        if only part of the data is of interest, calling this method may not be
        the best approach. This all depends on the specifics of the subclass.

        A subclass may allow consuming the data and load parts of data
        in a "as-needed" or "streaming" fashion.
        """
        raise NotImplementedError

    @deprecated(
        deprecated_in="0.7.4",
        removed_in="0.7.6",
        details="Use ``Slicer`` instead.",
    )
    def view(self):
        return Slicer(self)


FileReaderType = TypeVar("FileReaderType", bound=FileReader)


class FileSeq(Seq[FileReaderType]):
    """
    A FileSeq is a :class:`Seq` of :class:`FileReader` objects.

    Since this class represents a sequence of data files,
    methods such as ``__len__`` and ``__iter__`` are in terms of data *files*
    rather than data *items*. (One data file contains a sequence of data items.)
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

    def _concurrent_iter_info_file(self, task_id: str) -> Upath:
        """
        ``task_id``: returned by :meth:`new_concurrent_iter`.
        """
        return self.path / ".concurrent_file_iter" / task_id / "info.json"

    def new_concurrent_iter(self) -> str:
        """
        Initiate a concurrent iteration of the data files by multiple workers.

        One worker, such as a "coordinator", calls this method and obtains a task-ID.
        After that, one or more workers independently call :meth:`concurrent_iter`,
        providing the task-ID, which they have received from the coordinator.
        Each data file will be obtained by exactly one worker.

        During this iteration, the data files should stay unchanged.
        """
        task_id = datetime.utcnow().isoformat()
        self._concurrent_iter_info_file(task_id).write_json(
            {"n_files_claimed": 0}, overwrite=False
        )
        return task_id

    def concurrent_iter(self, task_id: str) -> Iterator[FileReaderType]:
        """
        Parameters
        ----------
        task_id
            The string returned by :meth:`new_concurrent_iter`.
            All workers that call this method using the same ``task_id``
            will consume the data files collectively.

        .. seealso:: :meth:`__getitem__`, :meth:`new_concurrent_iter`
        """
        nfiles = self.__len__()
        while True:
            ff = self._concurrent_iter_info_file(task_id)
            with ff.with_suffix(".json.lock").lock(timeout=120):
                iter_info = ff.read_json()
                n_files_claimed = iter_info["n_files_claimed"]
                if n_files_claimed >= nfiles:
                    # No more date files to process.
                    break

                iter_info["n_files_claimed"] = n_files_claimed + 1
                ff.write_json(iter_info, overwrite=True)

            logger.debug('yielding file #"%d"', n_files_claimed)
            yield self.__getitem__(n_files_claimed)

    def concurrent_iter_stat(self, task_id: str) -> dict:
        """Return status info for an ongoing "concurrent file iter"
        identified by the task ID.

        .. seealso: :meth:`new_concurrent_iter`.
        """
        info = self._concurrent_iter_info_file(task_id).read_json()
        return {**info, "n_files": self.__len__()}

    def concurrent_iter_done(self, task_id: str) -> bool:
        """Return whether the "concurrent file iter" identified by ``task_id`` is finished."""
        zz = self.concurrent_iter_stat(task_id)
        return zz["n_files_claimed"] >= zz["n_files"]


_unspecified = object()


class BiglistBase(Seq[Element]):
    """
    This base class contains code mainly concerning *reading*.
    The subclass :class:`~biglist.Biglist` adds functionalities for writing.
    Another subclass :class:`~biglist.ParquetBiglist` is read-only.
    Here, "reading" and "read-only" is talking about the *data files*.
    This class always needs to write meta info *about* the data files.
    In addition, the subclass :class:`~biglist.Biglist` also creates and manages
    the data files, whereas :class:`_biglist.ParquetBiglist` provides methods
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
        path = LocalUpath(
            os.path.abspath(tempfile.gettempdir()), str(uuid.uuid4())
        )  # type: ignore
        return path  # type: ignore

    @classmethod
    def new(
        cls,
        path: Optional[PathType] = None,
        *,
        keep_files: Optional[bool] = None,
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

        keep_files
            If not specified, the default behavior is the following:

            - If ``path`` is ``None``, then this is ``False``---the temporary directory
              will be deleted when this :class:`BiglistBase` object goes away.
            - If ``path`` is not ``None``, i.e. user has deliberately specified a location,
              then this is ``True``---files saved by this :class:`BiglistBase` object will stay.

            User can pass in ``True`` or ``False`` explicitly to override the default behavior.

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
            if keep_files is None:
                keep_files = False
        else:
            path = resolve_path(path)
            if keep_files is None:
                keep_files = True
        if path.is_dir():
            raise Exception(f'directory "{path}" already exists')
        if path.is_file():
            raise FileExistsError(path)
        obj = cls(path, require_exists=False, **kwargs)
        obj.keep_files = keep_files
        return obj

    def __init__(
        self,
        path: PathType,
        *,
        require_exists: bool = True,
        thread_pool_executor: Optional[ThreadPoolExecutor] = _unspecified,
    ):
        """
        Parameters
        ----------
        path
            Directory that contains files written by an instance
            of this class.

        thread_pool_executor
            Methods for reading and writing
            use worker threads. If this parameter is specified, then
            the provided thread pool will be used. This is useful
            when a large number of Biglist instances are active
            at the same time, because the provided thread pool
            controls the max number of threads.

            .. deprecated:: 0.7.4
                The input is ignored. Will be removed in 0.7.6.

        require_exists
            When initializing an object of this class,
            contents of the directory ``path`` should be already in place.
            This is indicated by ``require_exists = True``. In the
            classmethod :meth:`new` of a subclass, when creating an instance
            before any file is written, ``require_exists=False`` is used.
            User should usually leave this parameter at its default value.
        """
        self.path: Upath = resolve_path(path)
        """Root directory of the storage space for this object."""

        self._read_buffer: Optional[Seq[Element]] = None
        self._read_buffer_file_idx = None
        self._read_buffer_item_range: Optional[tuple[int, int]] = None
        # `self._read_buffer` contains the content of the file
        # indicated by `self._read_buffer_file_idx`.

        if thread_pool_executor is not _unspecified:
            warnings.warn(
                "`thread_pool_executor` is deprecated and ignored; will be removed in 0.7.6",
                DeprecationWarning,
                stacklevel=2,
            )

        self.info: dict
        """Various meta info."""

        try:
            # Instantiate a Biglist object pointing to
            # existing data.
            with self._info_file.with_suffix(".lock").lock(timeout=120):
                # Lock it because there could be multiple distributed workers
                # accessing this file.
                self.info = self._info_file.read_json()
        except FileNotFoundError as e:
            if require_exists:
                raise RuntimeError(
                    f"Cat not find {self.__class__.__name__} at path '{self.path}'"
                ) from e
            self.info = {}

        self._n_read_threads = 3
        self._n_write_threads = 3

    def __repr__(self):
        return f"<{self.__class__.__name__} at '{self.path}' with {self.num_data_items} elements in {self.num_data_files} data file(s)>"

    def __str__(self):
        return self.__repr__()

    @property
    def _info_file(self) -> Upath:
        return self.path / "info.json"

    def __len__(self) -> int:
        """
        Number of data items in this biglist.

        This is an alias to :meth:`num_data_items`.
        """
        # This assumes the current object is the only one
        # that may be appending to the biglist.
        # In other words, if the current object is one of
        # of a number of workers that are concurrently using
        # this biglist, then all the other workers are reading only.
        files = self.files
        if files:
            return files.num_data_items
        return 0

    def __getitem__(self, idx: int) -> Element:
        """
        Access a data item by its index; negative index works as expected.
        """
        # This is not optimized for speed. For example, ``self._get_data_files``
        # could be expensive involving directory crawl, maybe even in the cloud.
        # For better speed, use ``__iter__``.

        if not isinstance(idx, int):
            raise TypeError(
                f"{self.__class__.__name__} indices must be integers, not {type(idx).__name__}"
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
                ifile1 = (
                    self._read_buffer_file_idx
                )  # pylint: disable=access-member-before-definition
            elif idx < n2:
                return self._read_buffer[idx - n1]  # type: ignore
            else:
                ifile0 = (
                    self._read_buffer_file_idx + 1
                )  # pylint: disable=access-member-before-definition

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
        if files:
            if len(files) == 1:
                z = files[0]
                z.load()
                yield from z
            else:
                ndatafiles = len(files)

                max_workers = min(self._n_read_threads, ndatafiles)
                tasks = queue.Queue(max_workers)
                executor = _get_thread_pool()

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
        return self.__len__()

    @property
    @abstractmethod
    def files(self) -> FileSeq[FileReader[Element]]:
        raise NotImplementedError

    @deprecated(
        deprecated_in="0.7.4",
        removed_in="0.7.6",
        details="Use ``Slicer`` instead.",
    )
    def view(self):
        """
        By convention, a "slicing" method should return an object of the same class
        as the original object. This is not possible for :class:`BiglistBase` (or its subclasses),
        hence its :meth:`BiglistBase.__getitem__` does not support slicing. Slicing is supported
        by the object returned from this method, e.g.,

        ::

            biglist = Biglist(...)
            s = biglist.view()
            print(s[2:8].collect())
            print(s[3::2].collect())

        During the use of this slicer, the underlying biglist should not change.
        Multiple slicers may be used to view diff parts
        of the biglist; they open and read files independent of other slicers.
        """
        return Slicer(self)

    @deprecated(
        deprecated_in="0.7.4",
        removed_in="0.7.6",
        details="Use ``files`` instead.",
    )
    def iter_files(self) -> Iterator[FileReader[Element]]:
        """
        Yield one data file at a time, in contrast to :meth:`__iter__`,
        which yields one element at a time.
        """
        yield from self.files

    @deprecated(
        deprecated_in="0.7.4",
        removed_in="0.7.6",
        details="Use ``.files.new_concurrent_iter`` instead.",
    )
    def new_concurrent_file_iter(self) -> str:
        return self.files.new_concurrent_iter()

    @deprecated(
        deprecated_in="0.7.4",
        removed_in="0.7.6",
        details="Use ``.files.concurrent_iter`` instead.",
    )
    def concurrent_iter_files(self, task_id: str) -> Iterator[FileReader[Element]]:
        yield from self.files.concurrent_iter(task_id)

    @deprecated(
        deprecated_in="0.7.4",
        removed_in="0.7.6",
        details="Use ``.files.concurrent_iter_stat`` instead.",
    )
    def concurrent_file_iter_stat(self, task_id: str) -> dict:
        return self.files.concurrent_iter_stat(task_id)

    @deprecated(
        deprecated_in="0.7.4",
        removed_in="0.7.6",
        details="Use ``.files.concurrent_iter_done`` instead.",
    )
    def concurrent_file_iter_done(self, task_id: str) -> bool:
        return self.files.concurrent_iter_done(task_id)

    @deprecated(
        deprecated_in="0.7.1",
        removed_in="0.7.6",
        details="Use ``.files.__getitem__`` instead.",
    )
    def file_view(self, file: int):
        return self.files[file]

    @deprecated(
        deprecated_in="0.7.4",
        removed_in="0.7.6",
        details="Use ``.files.__getitem__`` instead.",
    )
    def file_reader(self, file: int) -> FileReader[Element]:
        return self.files[file]

    @deprecated(
        deprecated_in="0.7.1",
        removed_in="0.7.6",
        details="Use ``list(self.files)`` instead.",
    )
    def file_views(self):
        return list(self.files)

    @deprecated(
        deprecated_in="0.7.4",
        removed_in="0.7.6",
        details="Use ``list(self.files)`` instead.",
    )
    def file_readers(self) -> list[FileReader[Element]]:
        return list(self.files)

    @property
    @deprecated(
        deprecated_in="0.7.4",
        removed_in="0.7.6",
        details="Use ``.num_data_files`` instead.",
    )
    def num_datafiles(self) -> int:
        """Number of data files."""
        return len(self.files)

    @property
    @deprecated(
        deprecated_in="0.7.4",
        removed_in="0.7.6",
        details="Use ``self.files`` instead.",
    )
    def datafiles(self) -> list[str]:
        """
        Return the list of data file paths.
        """
        return [v[0] for v in self.files.data_files_info]

    @property
    @deprecated(
        deprecated_in="0.7.4",
        removed_in="0.7.6",
        details="Use ``self.files.data_files_info`` instead.",
    )
    def datafiles_info(self) -> list[tuple[str, int, int]]:
        """
        Return a list of tuples for the data files.
        Each tuple, representing one data file, consists of
        "file path", "element count in the file",
        and "cumulative element count in the data files so far".
        """
        return self.files.data_files_info
