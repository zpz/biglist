import bisect
import collections.abc
import itertools
import logging
import os
import queue
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from typing import (
    Callable,
    Iterator,
    Union,
    Sequence,
    List,
    Optional,
    Tuple,
    TypeVar,
)

from upathlib import LocalUpath, Upath, PathType, resolve_path
from ._util import locate_idx_in_chunked_seq


logger = logging.getLogger(__name__)


T = TypeVar("T")


class FileLoaderMode:
    ITER = 0
    RAND = 1


class FileView(collections.abc.Sequence):
    """
    Given a function ``loader`` that would load ``path`` and return
    a Sequence, a ``FileView`` object keeps ``loader`` and ``path``
    but does not call ``loader`` until the resultant ``Sequence``
    is needed. In other words, it does "lazy" loading.

    This makes a ``FileView`` object light weight and, more importantly,
    lend itself to pickling.
    One use case of FileView is to pass these objects around in
    ``multiprocessing`` code for concurrent data processing.

    The method ``BiglistBase.file_view`` returns a ``FileView`` object.
    """

    def __init__(self, path: PathType, loader: Callable):
        """
        Parameters
        ----------
        loader:
            A function that will load the data file and return
            a Sequence. This could be a classmethod, static method,
            and standing alone function, but can't be a lambda
            function, because a ``FileView`` object often undergoes
            pickling when it is passed between processes.
        """
        self._path: Upath = BiglistBase.resolve_path(path)
        self._loader = loader

    def __repr__(self):
        return f"{self.__class__.__name__}('{self._path}', {self._loader})"

    def __str__(self):
        return self.__repr__()

    def load(self) -> None:
        """
        This method *eagerly* loads data.
        """
        raise NotImplementedError

    def bool(self):
        return self.__len__() > 0

    def view(self):
        return ListView(self)


class ListView(Sequence[T]):
    """
    This class wraps a sequence and gives it capabilities of
    element access by slice or index array.

    One use case is to provide slicing capabilities
    to ``Biglist`` via a call to ``Biglist.view``.

    A ``ListView`` object does "zero-copy"---it keeps track of
    indices of elements in the window along with a reference to
    the underlying sequence. One may slice a ``ListView`` object,
    and this index-tracking continues. Only when a single-element
    access or an iteration is performed, the relevant elements
    are retrieved from the underlying sequence.
    """

    def __init__(self, list_: Sequence[T], range_: Union[range, Sequence[int]] = None):
        """
        This provides a "window" into the sequence ``list_``,
        which may be another ``ListView`` (which *is* a sequence, hence
        no special treatment).

        During the use of this object, it is assumed that the underlying
        ``list_`` is not changing. Otherwise the results may be incorrect.

        As a sequence, ``list_`` must support random access by index.
        It does not need to support slicing.
        """
        self._list = list_
        self._range = range_

    @property
    def raw(self) -> Sequence[T]:
        return self._list

    @property
    def range(self):
        return self._range

    def __repr__(self):
        return f"<{self.__class__.__name__} into {self.__len__()}/{len(self._list)} of {self._list}>"

    def __str__(self):
        return self.__repr__()

    def __len__(self) -> int:
        if self._range is None:
            return len(self._list)
        return len(self._range)

    def __bool__(self) -> bool:
        return len(self) > 0

    def __getitem__(self, idx: Union[int, slice, Sequence[int]]):
        """
        Element access by a single index, slice, or an index array.
        Negative index and standard slice syntax both work as expected.

        Slice and index-array access returns a new ``ListView`` object.
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
        if self._range is None:
            yield from self._list
        else:
            # This could be inefficient, depending on
            # the random-access performance of `self._list`.
            for i in self._range:
                yield self._list[i]

    def collect(self) -> List[T]:
        # Warning: don't do this on "big" data!
        return list(self)


class ChainedList(Sequence[T]):
    """
    This class tracks a series of ``Sequence``'s to provide
    random element access and iteration on the series as a whole.
    A call to the method ``view`` further returns an object that
    supports slicing.

    Note that ``ListView`` is also a ``Sequence``, hence could be
    a member of the series.
    """

    def __init__(self, *lists: Sequence[T]):
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

    def __len__(self):
        if self._len is None:
            if self._lists_len is None:
                self._lists_len = [len(v) for v in self._lists]
            self._len = sum(self._lists_len)
        return self._len

    def __bool__(self):
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

    def view(self):
        # The returned object supports slicing.
        return ListView(self)

    @property
    def raw(self) -> Sequence[T]:
        """
        A member list could be a ``ListView```. The current method
        does not follow a ``ListView`` to its "raw" component, b/c
        that might not have the same elements as the view, hence
        losing info.
        """
        return self._lists


class BiglistBase(Sequence[T]):
    """
    This base class contains code mainly concerning *read* only.
    The subclass ``Biglist`` adds functionalities for writing,
    whereas other subclasses, such as ``ParquetBiglist``, may be read-only.

    Data access is optimized for iteration, whereas random access
    (via index or slice) is less efficient, and assumed to be rare.
    """

    @staticmethod
    def resolve_path(path: PathType):
        # User may want to customize this method to provide
        # credentials for cloud storages, if their application involves
        # them, so that the code does not resort to default credential
        # retrieval mechanisms, which may be slow.
        return resolve_path(path)

    @classmethod
    def get_temp_path(cls) -> Upath:
        """
        Return a temporary directory to host a new Biglist object,
        if user does not specify a location for it.
        Subclass may want to customize this if they prefer other ways
        to find temporary locations. For example, they may want
        to use a temporary location in a cloud storage.
        """
        path = LocalUpath(
            os.path.abspath(tempfile.gettempdir()), str(uuid.uuid4())
        )  # type: ignore
        return path  # type: ignore

    @classmethod
    def load_data_file(cls, path: Upath):
        raise NotImplementedError

    @classmethod
    @contextmanager
    def lockfile(cls, file: Upath):
        """
        Although by default this uses ``file.lock()``, it doesn't have to be.
        All this method needs is to guarantee that the code block identified
        by ``file`` (essentially the name) is NOT executed concurrently
        by two "workers". It by no means has to be "locking that file".

        See ``Upath.lock``.

        Locking is used by several "concurrent distributed reading" methods.
        The scope of the lock is for read/write a tiny "control info" file.
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
        path:
            Directory that contains files written by an instance
            of this class.

        thread_pool_executor:
            Methods for reading and writing
            use worker threads. If this parameter is specified, then
            the provided thread pool will be used. This is useful
            when a large number of Biglist instances are active
            at the same time, because the provided thread pool
            controls the max number of threads.

        require_exists:
            When initializing an object of this class,
            contents of the directory ``path`` should be already in place.
            This is indicated by ``require_exists = True``. In the
            classmethod ``new`` of a subclass, when creating an instance
            before any file is written, ``require_exists=False`` is used.
            User should always leave this parameter at its default value.
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
        # Return "data_files" and "data_files_cumlength".
        # Subclass may choose to cache these results in instance attributes.
        raise NotImplementedError

    def _get_data_file(self, datafiles, idx):
        # `datafiles` is the return of `get_datafiles`.
        # `idx` is the index of the file of interest in `datafiles`.
        raise NotImplementedError

    def __len__(self) -> int:
        # This assumes the current object is the only one
        # that may be appending to the biglist (hence it has `append_buffer`).
        # In other words, if the current object is one of
        # of a number of workers that are concurrently using
        # the biglist, then all the other workers are reading only.
        _, data_files_cumlength = self._get_data_files()
        if data_files_cumlength:
            return data_files_cumlength[-1] + len(self._append_buffer)
        return len(self._append_buffer)

    def __getitem__(self, idx: int) -> T:
        """
        Element access by single index; negative index works as expected.

        This is not optimized for speed. For example, ``self._get_data_files``
        could be expensive involving directory crawl, maybe even in the cloud.
        For better speed, use ``__iter__``.

        This does not support slicing. For slicing, see method ``view``.
        The object returned by ``view`` eventually also calls this method
        to access elements.
        """
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
            data = self.file_view(file)

        self._read_buffer_file_idx = ifile
        self._read_buffer = data
        return data[idx - n]

    def __iter__(self) -> Iterator[T]:
        for f in self.iter_files():
            yield from f

        if self._append_buffer:
            yield from self._append_buffer

    def iter_files(self) -> Iterator[FileView]:
        """
        This is "eager" and not distributed, that is,
        this call consumes the entire data. To distribute the iteration
        to multiple workers, see ``concurrent_iter_files``.

        This yields the content of one file at a time.
        Specifically, it yields ``FileView`` objects.

        This exists mainly as the _engine_ for ``__iter__``.
        """
        # Assuming the biglist will not change (not being appended to)
        # during iteration.

        datafiles, _ = self._get_data_files()
        ndatafiles = len(datafiles)

        if ndatafiles == 1:
            z = self.file_view(self._get_data_file(datafiles, 0))
            z.load()
            yield z
        elif ndatafiles > 1:
            max_workers = min(self._n_read_threads, ndatafiles)
            tasks = queue.Queue(max_workers)
            executor = self._thread_pool

            def _read_file(idx):
                z = self.file_view(self._get_data_file(datafiles, idx))
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
        The content of the Biglist is split between the workers because
        each data file will be obtained by exactly one worker.

        During this iteration, the Biglist object should stay unchanged.
        """
        task_id = datetime.utcnow().isoformat()
        self._concurrent_file_iter_info_file(task_id).write_json(
            {"n_files_claimed": 0}, overwrite=False
        )
        return task_id

    def concurrent_iter_files(self, task_id: str) -> Iterator[FileView]:
        """
        Parameters
        ----------
        task_id:
            Returned by ``new_concurrent_file_iter``.
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
            yield self.file_view(file)

    def concurrent_file_iter_stat(self, task_id: str) -> dict:
        info = self._concurrent_file_iter_info_file(task_id).read_json()
        return {**info, "n_files": len(self._get_data_files()[0])}

    def concurrent_file_iter_done(self, task_id: str) -> bool:
        zz = self.concurrent_file_iter_stat(task_id)
        return zz["n_files_claimed"] >= zz["n_files"]

    def file_view(self, file: Union[Upath, int]) -> FileView:
        """
        Parameters
        ----------
        file:
            The data file path or the index of the file
            in the list of data files.
        """
        raise NotImplementedError

    def file_views(self) -> List[FileView]:
        # This is intended to facilitate parallel processing,
        # e.g. send views on diff files to diff processes.
        datafiles, _ = self._get_data_files()
        return [
            self.file_view(self._get_data_file(datafiles, i))
            for i in range(len(datafiles))
        ]

    def view(self) -> ListView[T]:
        """
        By convention, "slicing" should return an object of the same class
        as the original object. This is not possible for the ``Biglist`` class,
        hence its ``__getitem__`` does not support slicing. Slicing is supported
        by this "view" method---the object returned by this method can be
        sliced, e.g.,

        ::

            biglist = Biglist(...)
            v = biglist.view()
            print(v[2:8])
            print(v[3::2])

        During the use of this view, the underlying Biglist should not change.
        Multiple views may be used to view diff parts
        of the ``Biglist``; they open and read files independent of other views.
        """
        return ListView(self)

    @property
    def num_datafiles(self) -> int:
        return len(self._get_data_files()[0])

    @property
    def datafiles(self) -> List[str]:
        """
        Return the list of file paths.
        """
        raise NotImplementedError

    @property
    def datafiles_info(self) -> List[Tuple[str, int, int]]:
        """
        Return the list of (file_path, item_count, cum_count)
        """
