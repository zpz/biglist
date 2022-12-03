from __future__ import annotations

import bisect
import collections.abc
import itertools
import logging
import os
import queue
import tempfile
import uuid
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from typing import (
    Any,
    Callable,
    Iterator,
    Union,
    Sequence,
    List,
    Optional,
    Tuple,
    TypeVar,
)

from deprecation import deprecated
from upathlib import LocalUpath, Upath, PathType, resolve_path
from ._util import locate_idx_in_chunked_seq


logger = logging.getLogger(__name__)


T = TypeVar("T")


class FileLoaderMode:
    ITER = 0
    RAND = 1


class FileReader(collections.abc.Sequence):
    """
    A ``FileReader`` is a "lazy" loader of a data file.
    It keeps track of the path of a data file along with a loader function,
    but performs the loading only when needed.
    In particular, upon initiation, file loading has not happened, and the object
    is light weight and friendly to pickling.

    One use case of this class is to pass these objects around in
    ``multiprocessing`` code for concurrent data processing.

    Once data have been loaded, this class provides various ways to navigate
    the data. At a minimum, a subclass must implement the
    ``Sequence`` API, mainly random access and iteration.

    With loaded data and associated facilities, this object may no longer
    be pickle-able, depending on the specifics of the subclass.
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
            function, because a ``FileReader`` object is often used
            with ``multiprocessing``, hence must be pickle-friendly.
        """
        self.path: Upath = BiglistBase.resolve_path(path)
        self.loader = loader

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.path}', {self.loader})"

    def __str__(self):
        return self.__repr__()

    def load(self) -> None:
        """
        This method *eagerly* loads all the data from the file.
        """
        raise NotImplementedError

    def __len__(self) -> int:
        """Number of data elements in the file."""
        raise NotImplementedError

    def __bool__(self) -> bool:
        return self.__len__() > 0

    def view(self) -> ListView:
        """Return a ``ListView`` object to facilitate slicing this biglist."""
        return ListView(self)


class ListView(Sequence[T]):
    """
    This class wraps a sequence and enables access by slice or index array,
    in addition to single-index access.

    A ``ListView`` object does "zero-copy"---it keeps track of
    indices of selected elements along with a reference to
    the underlying sequence. This object may be sliced again in a repeated "zoom in" fashion.
    Only when a single-element access or an iteration is performed, the relevant elements
    are retrieved from the underlying sequence.
    """

    def __init__(self, list_: Sequence[T], range_: Union[range, Sequence[int]] = None):
        """
        This provides a "window" into the sequence ``list_``,
        which may be another ``ListView`` (which *is* a sequence, hence
        no special treatment is needed).

        During the use of this object, the underlying ``list_`` must remain unchanged.

        If ``range_`` is ``None``, the "window" covers the entire ``list_``.
        """
        self._list = list_
        self._range = range_

    @property
    def raw(self) -> Sequence[T]:
        """The underlying data ``Sequence``."""
        return self._list

    @property
    def range(self) -> Union[range, Sequence[int]]:
        """The current "window" represented by a ``range`` or a ``list`` of indices."""
        return self._range

    def __repr__(self):
        return f"<{self.__class__.__name__} into {self.__len__()}/{len(self._list)} of {self._list}>"

    def __str__(self):
        return self.__repr__()

    def __len__(self) -> int:
        """Number of elements in the current window."""
        if self._range is None:
            return len(self._list)
        return len(self._range)

    def __bool__(self) -> bool:
        return len(self) > 0

    def __getitem__(self, idx: Union[int, slice, Sequence[int]]):
        """
        Element access by a single index, slice, or an index array.
        Negative index and standard slice syntax work as expected.

        Single-index access returns the requested data element.
        Slice and index-array access return a new ``ListView`` object.
        """
        if isinstance(idx, int):
            # Return a single element.
            if self._range is None:
                return self._list[idx]
            return self._list[self._range[idx]]

        # Return a new `ListView` object below.

        if isinstance(idx, slice):
            if self._range is None:
                range_ = range(len(self._list))[idx]
            else:
                range_ = self._range[idx]
            return self.__class__(self._list, range_)

        # `idx` is a list of indices.
        if self._range is None:
            return self.__class__(self._list, idx)
        return self.__class__(self._list, [self._range[i] for i in idx])

    def __iter__(self):
        """Iterate over the elements in the current window."""
        if self._range is None:
            yield from self._list
        else:
            # This could be inefficient, depending on
            # the random-access performance of `self._list`.
            for i in self._range:
                yield self._list[i]

    def collect(self) -> List[T]:
        """
        Return a ``list`` containing the elements in the current window.
        This is equivalent to using the object to initialize a ``list``.

        Warning: don't do this on "big" data!
        """
        return list(self)


class ChainedList(Sequence):
    """
    This class tracks a series of ``Sequence`` to provide
    random element access and iteration on the series as a whole.
    A call to the method ``view`` further returns an ``ListView`` that
    supports slicing.

    This class operates with zero-copy.

    Note that ``ListView`` and ``ChainedList`` are ``Sequence``, hence could be
    members of the series.
    """

    def __init__(self, *lists: Sequence):
        self._lists = lists
        self._lists_len: List[int] = None
        self._lists_len_cumsum: List[int] = None
        self._len: int = None

        # Records info about the last call to `__getitem__`
        # to hopefully speed up the next call, under the assumption
        # that user tends to access consecutive or neighboring
        # elements.
        self._get_item_last_list = None

    def __repr__(self):
        return "<{} with {} elements in {} member lists>".format(
            self.__class__.__name__,
            self._len,
            len(self._lists),
        )

    def __str__(self):
        return self.__repr__()

    def __len__(self) -> int:
        if self._len is None:
            if self._lists_len is None:
                self._lists_len = [len(v) for v in self._lists]
            self._len = sum(self._lists_len)
        return self._len

    def __bool__(self) -> bool:
        return len(self) > 0

    def __getitem__(self, idx: int):
        if self._lists_len_cumsum is None:
            if self._lists_len is None:
                self._lists_len = [len(v) for v in self._lists]
            self._lists_len_cumsum = list(itertools.accumulate(self._lists_len))
        ilist, idx_in_list, list_info = locate_idx_in_chunked_seq(
            idx, self._lists_len_cumsum, self._get_item_last_list
        )
        self._get_item_last_list = list_info
        return self._lists[ilist][idx_in_list]

    def __iter__(self):
        for v in self._lists:
            yield from v

    def view(self) -> ListView:
        # The returned object supports slicing.
        return ListView(self)

    @property
    def raw(self) -> List[Sequence]:
        """
        Return the underlying list of ``Sequence``'s.

        A member ``Sequence`` could be a ``ListView```. The current method
        does not follow a ``ListView`` to its "raw" component, b/c
        that could represent a different set of elements than the ``ListView``
        object.
        """
        return self._lists


class BiglistBase(Sequence[T], ABC):
    """
    This base class contains code mainly concerning *reading* only.
    The subclass ``Biglist`` adds functionalities for writing.
    Other subclasses, such as ``ParquetBiglist``, may be read-only.
    """

    @staticmethod
    def resolve_path(path: PathType) -> Upath:
        """
        Resolve ``path`` to a ``upathlib.Upath`` object.

        User may want to customize this method to provide
        credentials for cloud storages, if their application involves
        them, so that the code does not resort to default credential
        retrieval mechanisms, which may be slow.
        """
        return resolve_path(path)

    @classmethod
    def get_temp_path(cls) -> Upath:
        """
        If user does not specify ``path`` when calling ``new``,
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

        This function is used as the argument ``loader`` to ``FileReader.__init__``.
        Its return type depends on the subclass.
        The value it returns is contained in ``FileReader`` for subsequent use.
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

        See ``Upath.lock``.
        """
        with file.lock(timeout=120):
            yield

    def __init__(
        self,
        path: PathType,
        *,
        thread_pool_executor: ThreadPoolExecutor = None,
        require_exists: bool = True,
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

        require_exists
            When initializing an object of this class,
            contents of the directory ``path`` should be already in place.
            This is indicated by ``require_exists = True``. In the
            classmethod ``new`` of a subclass, when creating an instance
            before any file is written, ``require_exists=False`` is used.
            User should usually leave this parameter at its default value.
        """
        self.path = self.resolve_path(path)

        self._read_buffer: Optional[Sequence[T]] = None
        self._read_buffer_file_idx = None
        self._read_buffer_item_range: Optional[Tuple[int, int]] = None
        # `self._read_buffer` contains the content of the file
        # indicated by `self._read_buffer_file_idx`.

        self._append_buffer: List = []
        self._file_dumper = None
        # These are for writing, but are needed in some code for reading.
        # In a read-only subclass, they remain these default values and
        # behave correctly.

        self._thread_pool_ = thread_pool_executor

        try:
            # Instantiate a Biglist object pointing to
            # existing data.
            self.info = self._info_file.read_json()
        except FileNotFoundError:
            if require_exists:
                raise RuntimeError(
                    f"Cat not find {self.__class__.__name__} at path '{self.path}'"
                )
            self.info = {}

        self._n_read_threads = 3
        self._n_write_threads = 3
        # You may assign these to other values right upon creation
        # of the `Biglist` object.

    def __repr__(self):
        return f"<{self.__class__.__name__} at '{self.path}' with {len(self)} elements in {self.num_datafiles} data file(s)>"

    def __str__(self):
        return self.__repr__()

    @property
    def _info_file(self) -> Upath:
        return self.path / "info.json"

    @property
    def _thread_pool(self):
        if self._thread_pool_ is None:
            executor = ThreadPoolExecutor(
                max(self._n_read_threads, self._n_write_threads)
            )
            self._thread_pool_ = executor

        return self._thread_pool_

    def __bool__(self) -> bool:
        return len(self) > 0

    def _get_data_files(self) -> tuple:
        """
        Returns
        -------
        tuple
            First element is "data_files"; second element is "data_files_cumlength".
            Subclass may choose to cache these results in instance attributes.
        """
        raise NotImplementedError

    def _get_data_file(self, datafiles, idx):
        """
        Parameters
        ----------
        datafiles
            The first element returned by ``_get_data_files``.
        idx
            Index of the data file in the list of data files.
        """
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
        _, data_files_cumlength = self._get_data_files()
        if data_files_cumlength:
            return data_files_cumlength[-1] + len(self._append_buffer)
        return len(self._append_buffer)

    def __getitem__(self, idx: int) -> T:
        """
        Element access by single index; negative index works as expected.

        This does not support slicing. For slicing, see method ``view``.
        The object returned by ``view`` eventually also calls this method
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
        datafiles, data_files_cumlength = self._get_data_files()
        if data_files_cumlength:
            length = data_files_cumlength[-1]
        else:
            length = 0
        idx = range(length + len(self._append_buffer))[idx]

        if idx >= length:
            if idx - length >= len(self._append_buffer):
                raise IndexError(idx)
            return self._append_buffer[idx - length]  # type: ignore

        ifile0 = 0
        ifile1 = len(datafiles)
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

        file = self._get_data_file(datafiles, ifile)
        # If the file is in a writing queue and has not finished writing yet:
        if self._file_dumper is None:
            data = None
        else:
            data = self._file_dumper.get_file_data(file)
        if data is None:
            data = self.file_reader(file)

        self._read_buffer_file_idx = ifile
        self._read_buffer = data
        return data[idx - n]

    def __iter__(self) -> Iterator[T]:
        """
        Iterate over all the elements.
        """
        for f in self.iter_files():
            yield from f

        if self._append_buffer:
            yield from self._append_buffer

    def iter_files(self) -> Iterator[FileReader]:
        """
        Yield one data file at a time, in contrast to ``__iter__``,
        which yields one element at a time.

        See Also
        --------
        concurrent_iter_files: collectively iterate between multiple workers.
        """
        # Assuming the biglist will not change (not being appended to)
        # during iteration.

        datafiles, _ = self._get_data_files()
        ndatafiles = len(datafiles)

        if ndatafiles == 1:
            z = self.file_reader(self._get_data_file(datafiles, 0))
            z.load()
            yield z
        elif ndatafiles > 1:
            max_workers = min(self._n_read_threads, ndatafiles)
            tasks = queue.Queue(max_workers)
            executor = self._thread_pool

            def _read_file(idx):
                z = self.file_reader(self._get_data_file(datafiles, idx))
                z.load()
                return z

            for i in range(max_workers):
                t = executor.submit(_read_file, i)
                tasks.put(t)
            nfiles_queued = max_workers

            for _ in range(ndatafiles):
                t = tasks.get()
                data = t.result()

                # Before starting to yield data, take care of the
                # downloading queue to keep it busy.
                if nfiles_queued < ndatafiles:
                    # `nfiles_queued` is the index of the next file to download.
                    t = executor.submit(_read_file, nfiles_queued)
                    tasks.put(t)
                    nfiles_queued += 1

                yield data

    def _concurrent_file_iter_info_file(self, task_id: str) -> Upath:
        """
        `task_id`: returned by `new_concurrent_file_iter`.
        """
        return self.path / ".concurrent_file_iter" / task_id / "info.json"

    def new_concurrent_file_iter(self) -> str:
        """
        One worker, such as a "coordinator", calls this method once.
        After that, one or more workers independently call ``concurrent_iter_files``,
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

    def concurrent_iter_files(self, task_id: str) -> Iterator[FileReader]:
        """
        Parameters
        ----------
        task_id
            The string returned by ``new_concurrent_file_iter``.
            All workers that call this method using the same ``task_id``
            will consume the biglist collectively.
        """
        datafiles, _ = self._get_data_files()
        while True:
            ff = self._concurrent_file_iter_info_file(task_id)
            with self.lockfile(ff.with_suffix(".json.lock")):
                iter_info = ff.read_json()
                n_files_claimed = iter_info["n_files_claimed"]
                if n_files_claimed >= len(datafiles):
                    # No more date files to process.
                    break

                iter_info["n_files_claimed"] = n_files_claimed + 1
                ff.write_json(iter_info, overwrite=True)

            file = self._get_data_file(datafiles, n_files_claimed)
            logger.debug('yielding data of file "%s"', file)
            yield self.file_reader(file)

    def concurrent_file_iter_stat(self, task_id: str) -> dict:
        """Return status info of an ongoing "concurrent file iter"."""
        info = self._concurrent_file_iter_info_file(task_id).read_json()
        return {**info, "n_files": len(self._get_data_files()[0])}

    def concurrent_file_iter_done(self, task_id: str) -> bool:
        """Return whether the "concurrent file iter" identified by ``task_id`` is finished."""
        zz = self.concurrent_file_iter_stat(task_id)
        return zz["n_files_claimed"] >= zz["n_files"]

    @deprecated(
        deprecated_in="0.7.1",
        removed_in="0.8.0",
        details="Use ``file_reader`` instead.",
    )
    def file_view(self, file):
        return self.file_reader(file)

    def file_reader(self, file: Union[Upath, int]) -> FileReader:
        """
        Parameters
        ----------
        file
            The data file path or the index of the file (0-based)
            in the list of data files.
        """
        raise NotImplementedError

    @deprecated(
        deprecated_in="0.7.1",
        removed_in="0.8.0",
        details="Use ``file_readers`` instead.",
    )
    def file_views(self):
        return self.file_readers()

    def file_readers(self) -> List[FileReader]:
        """
        Return a list of all the data files wrapped in ``FileReader`` objects,
        which are light weight, have not loaded data yet, and are friendly
        to pickling.

        This is intended to facilitate concurrent processing,
        e.g. one may send the ``FileReader`` objects to different processes or threads.
        """
        datafiles, _ = self._get_data_files()
        return [
            self.file_reader(self._get_data_file(datafiles, i))
            for i in range(len(datafiles))
        ]

    def view(self) -> ListView[T]:
        """
        By convention, a "slicing" method should return an object of the same class
        as the original object. This is not possible for ``BiglistBase`` (or its subclasses),
        hence its ``__getitem__`` does not support slicing. Slicing is supported
        by the object returned from ``view``, e.g.,

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

    @property
    def num_datafiles(self) -> int:
        """Number of data files."""
        return len(self._get_data_files()[0])

    @property
    def datafiles(self) -> List[str]:
        """
        Return the list of data file paths.
        """
        raise NotImplementedError

    @property
    def datafiles_info(self) -> List[Tuple[str, int, int]]:
        """
        Return a list of tuples for the data files.
        Each tuple, representing one data file, consists of
        "file path", "element count in the file",
        and "cumulative element count in the data files so far".
        """
