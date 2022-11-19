import bisect
import itertools
import logging
import os
import queue
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from multiprocessing.util import Finalize
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

from upathlib import LocalUpath, Upath, PathType, resolve_path  # type: ignore
from ._util import locate_idx_in_chunked_seq


logger = logging.getLogger(__name__)


T = TypeVar("T")


class FileLoaderMode:
    ITER = 0
    RAND = 1


class FileView(Sequence[T]):
    """
    Given a function `loader` that would load `file` and return
    a Sequence. A `FileView` object keeps `loader` and `file`
    but does not call `loader` until the resultant Sequence
    is needed. In other words, it does "lazy" loading.

    This makes a `FileView` object light weight and, more importantly,
    lend itself to pickling.
    One use case of FileView is to pass these objects around in
    `multiprocessing` code for concurrent data processing.
    """

    def __init__(self, file: Upath, loader: Callable):
        """
        `loader`: a function that will load the data file and return
            a Sequence. This could be a classmethod, static method,
            and standing alone function, but can't be a lambda
            function, because a `FileView` object often undergoes
            pickling when it is passed between processes.
        """
        self._file = file
        self._loader = loader
        self._data = None

    def __repr__(self):
        return f"<{self.__class__.__name__} into '{self._file}', with loader {self._loader}>"

    def __str__(self):
        return self.__repr__()

    @property
    def data(self) -> Sequence[T]:
        if self._data is None:
            self._data = self._loader(self._file, FileLoaderMode.RAND)
        return self._data

    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, idx: Union[int, slice]) -> T:
        return self.data[idx]

    def __iter__(self):
        return iter(self.data)


class ListView(Sequence[T]):
    """
    This class wraps a sequence and gives it capabilities of
    element access by slice or index array.

    One use case is to provide slicing capabilities
    to `Biglist` via a call to `Biglist.view()`.

    A `ListView` object does "zero-copy"---it keeps track of
    indices of elements in the window as well as a reference to
    the underlying sequence. One may slice a `ListView` object,
    and this index-tracking continues. Only when a single-element
    access or an iteration is performed, the relevant elements
    are retrieved from the underlying sequence.
    """

    def __init__(self, list_: Sequence[T], range_: Union[range, Sequence[int]] = None):
        """
        This provides a "window" into the sequence `list_`,
        which may be another `ListView` (which *is* a sequence, hence
        no special treatment).

        During the use of this object, it is assumed that the underlying
        `list_` is not changing. Otherwise the results may be incorrect.

        As a sequence, `list_` must support random access by index.
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

        Slice and index-array access returns a new `ListView` object.
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
    def __init__(self, *lists: Sequence[T]):
        self._lists = lists
        self._lists_len = None
        self._lists_len_cumsum = None
        self._len = None
        self._get_item_last_list = None

    def __repr__(self):
        return "<{} with {} member lists, total length {}>".format(
            self.__class__.__name__,
            len(self._lists),
            self._len,
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
        return self._lists


class BiglistBase(Sequence[T]):
    """
    This base class contains code mainly concerning *read* only.
    The subclass `Biglist` adds functionalities for writing,
    whereas other subclasses, such as `ParquetBiglist`, may be read-only.
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
    def load_data_file(cls, path: Upath, mode: int) -> Sequence[T]:
        """
        `mode`: take values defined in `FileLoaderMode`.
        """
        raise NotImplementedError

    @classmethod
    @contextmanager
    def lockfile(cls, file: Upath):
        # Although by default this uses `file.lock()`, it doesn't have to be.
        # All this method needs is to guarantee that the code block identified
        # by `file` (essentially the name) is NOT executed concurrently
        # by two "workers". It by no means has to be "locking that file".
        #
        # Refer to `Upath.lock`.
        #
        # Locking is used by several "concurrent distributed reading" methods.
        # The scope of the lock is for read/write a tiny "control info" file.
        with file.lock(timeout=120):
            yield

    def __init__(
        self,
        path: PathType,
        thread_pool_executor: ThreadPoolExecutor = None,
        require_exists: bool = True,
    ):
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

    def __repr__(self):
        return f"<{self.__class__.__name__} at '{self.path}' with {len(self)} records in {self.num_datafiles} data file(s)>"

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
            Finalize(self, executor.shutdown)

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

        This is not optimized for speed. For example, `self._get_data_files`
        could be expensive involving directory crawl, maybe even in the cloud.
        For better speed, use `__iter__`.

        This does not support slicing. For slicing, see method `view`.

        This is called in these cases:

            - Access a single item of a Biglist object.
            - Access a single item of a Biglist.view()
            - Access a single item of a slice on a Biglist.view()
            - Iterate a slice on a Biglist.view()
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
            data = self.load_data_file(file, FileLoaderMode.RAND)

        self._read_buffer_file_idx = ifile
        self._read_buffer = data
        return data[idx - n]

    def __iter__(self) -> Iterator[T]:
        for f in self.iter_files():
            yield from f

        if self._append_buffer:
            yield from self._append_buffer

    def iter_files(self) -> Iterator[Sequence[T]]:
        """
        This is "eager", and not distributed, that is,
        it consumes the entire data. To distribute the iteration
        to multiple workers, use `concurrent_iter` or `concurrent_iter_files`.

        This exists mainly as the _engine_ for `__iter__`.
        """
        # Assuming the biglist will not change (not being appended to)
        # during iteration.

        datafiles, _ = self._get_data_files()
        ndatafiles = len(datafiles)

        if ndatafiles == 1:
            yield self.load_data_file(
                self._get_data_file(datafiles, 0), FileLoaderMode.ITER
            )
        elif ndatafiles > 1:
            max_workers = min(self._n_read_threads, ndatafiles)
            tasks = queue.Queue(max_workers)
            executor = self._thread_pool

            for i in range(max_workers):
                t = executor.submit(
                    self.load_data_file,
                    self._get_data_file(datafiles, i),
                    FileLoaderMode.ITER,
                )
                tasks.put(t)
            nfiles_queued = max_workers

            for _ in range(ndatafiles):
                t = tasks.get()
                data = t.result()

                # Before starting to yield data, take care of the
                # downloading queue to keep it busy.
                if nfiles_queued < ndatafiles:
                    # `nfiles_queued` is the index of the next file to download.
                    t = executor.submit(
                        self.load_data_file,
                        self._get_data_file(datafiles, nfiles_queued),
                        FileLoaderMode.ITER,
                    )
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
        After that, one or more workers independently call `concurrent_iter_files`,
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

    def concurrent_iter_files(self, task_id: str) -> Iterator[Sequence[T]]:
        """
        `task_id`: returned by `new_concurrent_file_iter`.
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
            yield self.load_data_file(file, FileLoaderMode.RAND)
            # Here, using `FileLoaderMode.ITER` is "eager" loading.
            # `RAND` is "lazy" loading, giving user more choices.

    def concurrent_file_iter_stat(self, task_id: str) -> dict:
        info = self._concurrent_file_iter_info_file(task_id).read_json()
        return {**info, "n_files": len(self._get_data_files()[0])}

    def concurrent_file_iter_done(self, task_id: str) -> bool:
        zz = self.concurrent_file_iter_stat(task_id)
        return zz["n_files_claimed"] >= zz["n_files"]

    def new_concurrent_iter(self) -> str:
        return self.new_concurrent_file_iter()

    def concurrent_iter(self, task_id: str) -> Iterator[T]:
        for f in self.concurrent_iter_files(task_id):
            yield from f

    def concurrent_iter_stat(self, task_id: str) -> dict:
        return self.concurrent_file_iter_stat()

    def concurrent_iter_done(self, task_id: str) -> bool:
        return self.concurrent_file_iter_done(task_id)

    def file_view(self, file: Union[Upath, int]) -> FileView:
        if isinstance(file, int):
            datafiles, _ = self._get_data_files()
            file = self._get_data_file(datafiles, file)
        return FileView(file, self.__class__.load_data_file)  # type: ignore

    def file_views(self) -> List[FileView]:
        # This is intended to facilitate parallel processing,
        # e.g. send views on diff files to diff processes.
        # `concurrent_iter_files` can achieve the same goal.
        datafiles, _ = self._get_data_files()
        return [
            self.file_view(self._get_data_file(datafiles, i))
            for i in range(len(datafiles))
        ]

    def view(self) -> ListView[T]:
        """
        By convention, "slicing" should return an object of the same class
        as the original object. This is not possible for the `Biglist` class,
        hence its `__getitem__` does not support slicing. Slicing is supported
        by this "view" method---the object returned by this method can be
        sliced, e.g.,

            biglist = Biglist(...)
            v = biglist.view()
            print(v[2:8])
            print(v[3::2])
        """
        # During the use of this view, the underlying Biglist should not change.
        # Multiple views may be used to view diff parts
        # of the Biglist; they open and read files independent of other views.
        return ListView(self)

    @property
    def num_datafiles(self) -> int:
        return len(self._get_data_files()[0])

    @property
    def datafiles(self) -> List[str]:
        raise NotImplementedError
