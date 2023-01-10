from __future__ import annotations

import bisect
import logging
import os
import queue
import tempfile
import uuid
import warnings
from abc import abstractmethod
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from typing import (
    Any,
    Callable,
    Generic,
    Optional,
    TypeVar,
)

from deprecation import deprecated
from upathlib import LocalUpath, PathType, Upath, resolve_path

from ._util import Element, ListView, Seq, SeqType

logger = logging.getLogger(__name__)


class FileReader(Seq[Element]):
    """
    A FileReader is a "lazy" loader of a data file.
    It keeps track of the path of a data file along with a loader function,
    but performs the loading only when needed.
    In particular, upon initiation, file loading has not happened, and the object
    is light weight and friendly to pickling.

    One use case of this class is to pass these objects around in
    `multiprocessing <https://docs.python.org/3/library/multiprocessing.html>`_ code for concurrent data processing.

    Once data have been loaded, this class provides various ways to navigate
    the data. At a minimum, a subclass must implement the
    |Sequence|_ API, mainly random access and iteration.

    With loaded data and associated facilities, this object may no longer
    be pickle-able, depending on the specifics of the subclass.

    This class is generic with a parameter indicating the type of the data elements.
    For example you can write::

        def func(file_reader: FileReader[int]):
            ...
    """

    def __init__(self, path: PathType, loader: Callable[[Upath], Any]):
        """
        Parameters
        ----------
        path
            Path of a data file.
        loader
            A function that will load the data file.
            This could be a classmethod, staticmethod,
            or a standalone function, but can't be a lambda
            function, because a FileReader object is often used
            with `multiprocessing <https://docs.python.org/3/library/multiprocessing.html>`_, hence must be pickle-friendly.
        """
        self.path: Upath = BiglistBase.resolve_path(path)
        """Path of the file."""
        self.loader: Callable[[Upath], Any] = loader
        """A function that will be used to read the file."""

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.path}', {self.loader})"

    def __str__(self):
        return self.__repr__()

    def load(self) -> None:
        """
        This method *eagerly* loads all the data from the file.
        """
        raise NotImplementedError

    def view(self) -> ListView[FileReader[Element]]:
        """Return a :class:`ListView` object to facilitate slicing this biglist."""
        return ListView(self)


FileReaderType = TypeVar("FileReaderType", bound=FileReader)


"""
Return a list of all the data files wrapped in :class:`FileReader` objects,
which are light weight, have not loaded data yet, and are friendly
to pickling.

This is intended to facilitate concurrent processing,
e.g. one may send the :class:`FileReader` objects to different processes or threads.
"""


class FileSeq(Generic[FileReaderType]):
    @classmethod
    @contextmanager
    def lockfile(cls, file: Upath):
        """
        All this method does is to guarantee that the code block identified
        by ``file`` (essentially the name) is *not* executed concurrently
        by two "workers".

        This method is used by several "distributed reading" methods.
        The scope of the lock is for reading/writing a tiny "control info" file.

        See `Upath.lock <https://github.com/zpz/upathlib/blob/main/src/upathlib/_upath.py>`_.
        """
        with file.lock(timeout=120):
            yield

    @property
    @abstractmethod
    def info(self) -> dict:
        """
        Return a dict with at least an element called 'data_files', which
        is list of tuples for the data files.
        Each tuple, representing one data file, consists of
        "file path", "element count in the file",
        and "cumulative element count in the data files so far".

        Implementation in a subclass should consider caching the value
        so that repeated calls are cheap.
        """
        raise NotImplementedError

    def __len__(self) -> int:
        return len(self.info["data_files"])

    @abstractmethod
    def __getitem__(self, idx: int) -> FileReaderType:
        """
        Parameters
        ----------
        idx
            Index of the file (0-based) in the list of data files as returned
            in :meth:`info`.
        """
        raise NotImplementedError

    def __iter__(self) -> Iterator[FileReaderType]:
        """
        Yield one data file at a time.
        """
        for i in range(self.__len__()):
            yield self.__getitem__(i)

    def _concurrent_iter_info_file(self, task_id: str) -> Upath:
        """
        ``task_id``: returned by :meth:`new_concurrent_iter`.
        """
        raise NotImplementedError

    def new_concurrent_iter(self) -> str:
        """
        One worker, such as a "coordinator", calls this method once.
        After that, one or more workers independently call :meth:`concurrent_iter`,
        providing the task-ID returned by this method.
        Each data file will be obtained by exactly one worker,
        thus content of the biglist is split between the workers.

        During this iteration, the biglist object should stay unchanged.
        """
        task_id = datetime.utcnow().isoformat()
        self._concurrent_file_iter_info_file(task_id).write_json(
            {"n_files_claimed": 0}, overwrite=False
        )
        return task_id

    def concurrent_iter(self, task_id: str) -> Iterator[FileReader[Element]]:
        """
        Parameters
        ----------
        task_id
            The string returned by :meth:`new_concurrent_iter`.
            All workers that call this method using the same ``task_id``
            will consume the biglist collectively.
        """
        nfiles = self.__len__()
        while True:
            ff = self._concurrent_file_iter_info_file(task_id)
            with self.lockfile(ff.with_suffix(".json.lock")):
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
        """Return status info of an ongoing "concurrent file iter"."""
        info = self._concurrent_file_iter_info_file(task_id).read_json()
        return {**info, "n_files": self._len__()}

    def concurrent_iter_done(self, task_id: str) -> bool:
        """Return whether the "concurrent file iter" identified by ``task_id`` is finished."""
        zz = self.concurrent_iter_stat(task_id)
        return zz["n_files_claimed"] >= zz["n_files"]


_unspecified = object()


class BiglistBase(Seq[Element]):
    """
    This base class contains code mainly concerning *reading* only.
    The subclass :class:`~biglist.Biglist` adds functionalities for writing.
    Other subclasses, such as :class:`~biglist.ParquetBiglist`, may be read-only.

    This class is generic with a parameter indicating the type of the data elements,
    but this is useful only for the subclass :class:`~biglist.Biglist`.
    For the subclass :class:`~biglist.ParquetBiglist`, this parameter is essentially ``Any``.
    """

    _thread_pool: ThreadPoolExecutor = None

    @staticmethod
    def resolve_path(path: PathType) -> Upath:
        """
        Resolve ``path`` to a `upathlib.Upath`_ object.

        User may want to customize this method to provide
        credentials for cloud storages, if their application involves
        them, so that the code does not resort to default credential
        retrieval mechanisms, which may be slow.
        """
        return resolve_path(path)

    @classmethod
    def _get_thread_pool(cls):
        """
        This method is called in three places:

        - :meth:`iter_files` when there are more than one data files.
        - :meth:`biglist.Biglist.append` (and :meth:`~biglist.Biglist.extend`).
        - :meth:`biglist.ParquetBiglist.new`.

        Once any of these has been called, the class object :class:`BiglistBase`
        gets a ``concurrent.futures.ThreadPoolExecutor`` as an attribute.
        After that, if you start using other processes, and in other processes
        any of these methods is called again, **you must use the "spawn" method
        to start the processes**. This is related to data copying in "fork" processes
        (the default on Linux), and a ``ThreadPoolExecutor`` copied into
        the new processes is malfunctional.

        I do not know whether it is possible to detect such corrupt thread pools.
        If there is a way to do that, then it can be used in this method to make
        things work with forked processes.
        """
        if cls._thread_pool is None:
            cls._thread_pool = ThreadPoolExecutor(min(32, (os.cpu_count() or 1) + 4))
        return cls._thread_pool

    @classmethod
    def get_temp_path(cls) -> Upath:
        """
        If user does not specify ``path`` when calling :meth:`new` (in a subclass),
        this method is used to determine a temporary directory.

        Subclass may want to customize this if they prefer other ways
        to find temporary locations. For example, they may want
        to use a temporary location in a cloud storage.
        """
        path = LocalUpath(
            os.path.abspath(tempfile.gettempdir()), str(uuid.uuid4())
        )  # type: ignore
        return path  # type: ignore

    @classmethod
    @abstractmethod
    def load_data_file(cls, path: Upath):
        """
        Load the data file given by ``path``.

        This function is used as the argument ``loader`` to :meth:`biglist.FileReader.__init__`.
        Its return type depends on the subclass.
        The value it returns is contained in :class:`FileReader` for subsequent use.
        """
        raise NotImplementedError

    @classmethod
    @contextmanager
    def lockfile(cls, file: Upath):
        """
        All this method does is to guarantee that the code block identified
        by ``file`` (essentially the name) is *not* executed concurrently
        by two "workers".

        This method is used by several "distributed reading" methods.
        The scope of the lock is for reading/writing a tiny "control info" file.

        See `Upath.lock <https://github.com/zpz/upathlib/blob/main/src/upathlib/_upath.py>`_.
        """
        with file.lock(timeout=120):
            yield

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
            (The subclass :class:`~biglist.Biglist` saves both data and meta-info.
            The subclass :class:`~biglist.ParquetBiglist` saves meta-info only.)
            The directory must be non-existent.
            It is not necessary to pre-create the parent directory of this path.

            This path can be either on local disk or in a cloud storage.

            If not specified, :meth:`~biglist._base.BiglistBase.get_temp_path`
            will be called to determine a temporary path.

        keep_files
            If not specified, the default behavior is the following:

            - If ``path`` is ``None``, then this is ``False``---the temporary directory
              will be deleted when this :class:`~biglist._base.BiglistBase` object goes away.
            - If ``path`` is not ``None``, i.e. user has deliberately specified a location,
              then this is ``True``---files saved by this :class:`~biglist._base.BiglistBase` object will stay.

            User can pass in ``True`` or ``False`` explicitly to override the default behavior.

        **kwargs
            additional arguments are passed on to :meth:`__init__`.

        Notes
        -----
        A :class:`~biglist._base.BiglistBase` object construction is in either of the two modes
        below:

        a) create a new :class:`~biglist._base.BiglistBase` to store new data.

        b) create a :class:`~biglist._base.BiglistBase` object pointing to storage of
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

        Some settings may be applicable to an existing :class:`~biglist._base.BiglistBase` object, e.g.,
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
            path = cls.resolve_path(path)
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
        self.path: Upath = self.resolve_path(path)
        """Root directory of the storage space for this object."""

        self._read_buffer: Optional[Seq[Element]] = None
        self._read_buffer_file_idx = None
        self._read_buffer_item_range: Optional[tuple[int, int]] = None
        # `self._read_buffer` contains the content of the file
        # indicated by `self._read_buffer_file_idx`.

        self._append_buffer: list = []
        self._file_dumper = None
        # These are for writing, but are needed in some code for reading.
        # In a read-only subclass, they remain these default values and
        # behave correctly.

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
        return f"<{self.__class__.__name__} at '{self.path}' with {len(self)} elements in {self.num_datafiles} data file(s)>"

    def __str__(self):
        return self.__repr__()

    @property
    def _info_file(self) -> Upath:
        return self.path / "info.json"

    @property
    @abstractmethod
    def files(self) -> FileSeq:
        raise NotImplementedError

    def __len__(self) -> int:
        """
        Number of elements in this biglist.
        """
        # This assumes the current object is the only one
        # that may be appending to the biglist.
        # In other words, if the current object is one of
        # of a number of workers that are concurrently using
        # this biglist, then all the other workers are reading only.
        files = self.files
        if files:
            return files.info["data_files"][-1][-1] + len(self._append_buffer)
        return len(self._append_buffer)

    def __getitem__(self, idx: int) -> Element:
        """
        Element access by single index; negative index works as expected.

        This does not support slicing. For slicing, see method :meth:`view`.
        The object returned by :meth:`view` eventually also calls this method
        to access elements.
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

        if idx < 0 and (-idx) <= len(self._append_buffer):
            return self._append_buffer[idx]

        # TODO: this is reliable only if there is no other "worker node"
        # appending elements to this object.
        files = self.files
        if files:
            data_files_cumlength = [v[-1] for v in files.info["data_files"]]
            length = data_files_cumlength[-1]
            nfiles = len(files)
        else:
            data_files_cumlength = []
            length = 0
            nfiles = 0
        idx = range(length + len(self._append_buffer))[idx]

        if idx >= length:
            if idx - length >= len(self._append_buffer):
                raise IndexError(idx)
            return self._append_buffer[idx - length]  # type: ignore

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

        file = files.info["data_files"][ifile][0]
        # If the file is in a writing queue and has not finished writing yet:
        if self._file_dumper is None:
            data = None
        else:
            data = self._file_dumper.get_file_data(file)
        if data is None:
            data = files[ifile]

        self._read_buffer_file_idx = ifile
        self._read_buffer = data
        return data[idx - n]

    def __iter__(self) -> Iterator[Element]:
        """
        Iterate over all the elements.
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

        if self._append_buffer:
            yield from self._append_buffer

    def view(self) -> ListView[BiglistBase[Element]]:
        """
        By convention, a "slicing" method should return an object of the same class
        as the original object. This is not possible for :class:`~biglist._base.BiglistBase` (or its subclasses),
        hence its :meth:`~biglist._base.BiglistBase.__getitem__` does not support slicing. Slicing is supported
        by the object returned from this method, e.g.,

        ::

            biglist = Biglist(...)
            v = biglist.view()
            print(v[2:8].collect())
            print(v[3::2].collect())

        During the use of this view, the underlying biglist should not change.
        Multiple views may be used to view diff parts
        of the biglist; they open and read files independent of other views.
        """
        return ListView(self)

    @deprecated(
        deprecated_in="0.7.4", removed_in="0.8.0", details="Use `.files` instead."
    )
    def iter_files(self) -> Iterator[FileReader[Element]]:
        """
        Yield one data file at a time, in contrast to :meth:`__iter__`,
        which yields one element at a time.

        See Also
        --------
        :meth:`concurrent_iter_files`: collectively iterate between multiple workers.
        """
        yield from self.files

    @deprecated(
        deprecated_in="0.7.4",
        removed_in="0.8.0",
        details="Use ``.files.new_concurrent_iter`` instead.",
    )
    def new_concurrent_file_iter(self) -> str:
        return self.files.new_concurrent_iter()

    @deprecated(
        deprecated_in="0.7.4",
        removed_in="0.8.0",
        details="Use ``.files.concurrent_iter`` instead.",
    )
    def concurrent_iter_files(self, task_id: str) -> Iterator[FileReader[Element]]:
        yield from self.files.concurrent_iter(task_id)

    @deprecated(
        deprecated_in="0.7.4",
        removed_in="0.8.0",
        details="Use ``.files.concurrent_iter_stat`` instead.",
    )
    def concurrent_file_iter_stat(self, task_id: str) -> dict:
        return self.files.concurrent_iter_stat(task_id)

    @deprecated(
        deprecated_in="0.7.4",
        removed_in="0.8.0",
        details="Use ``.files.concurrent_iter_done`` instead.",
    )
    def concurrent_file_iter_done(self, task_id: str) -> bool:
        return self.files.concurrent_iter_done(task_id)

    @deprecated(
        deprecated_in="0.7.1",
        removed_in="0.8.0",
        details="Use ``.files.__getitem__`` instead.",
    )
    def file_view(self, file: int):
        return self.files[file]

    @deprecated(
        deprecated_in="0.7.4",
        removed_in="0.8.0",
        details="Use ``.files.__getitem__`` instead.",
    )
    def file_reader(self, file: int) -> FileReader[Element]:
        return self.files[file]

    @deprecated(
        deprecated_in="0.7.1",
        removed_in="0.8.0",
        details="Use ``list(self.files)`` instead.",
    )
    def file_views(self):
        return list(self.files)

    @deprecated(
        deprecated_in="0.7.4",
        removed_in="0.8.0",
        details="Use ``list(self.files)`` instead.",
    )
    def file_readers(self) -> list[FileReader[Element]]:
        return list(self.files)

    @property
    @deprecated(
        deprecated_in="0.7.4",
        removed_in="0.8.0",
        details="Use ``len(self.files)`` instead.",
    )
    def num_datafiles(self) -> int:
        """Number of data files."""
        return len(self.files)
