# from __future__ import annotations

# Will no longer be needed at Python 3.10.

import bisect
import logging
import queue
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from multiprocessing.util import Finalize
from pathlib import Path
from typing import (
    Iterator,
    Union,
    Sequence,
    List,
    Optional,
    Tuple,
    TypeVar,
)

from upathlib import LocalUpath, Upath  # type: ignore
from ._view import ListView, FileView


logger = logging.getLogger(__name__)



T = TypeVar("T")


class BiglistBase(Sequence[T]):
    '''
    This base class contains code concerning *read* only.
    '''
    @classmethod
    def load_data_file(cls, path: Upath) -> Sequence[T]:
        raise NotImplementedError

    @classmethod
    @contextmanager
    def lockfile(cls, file: Upath):
        # Although by default this uses `file.lock()`, it doesn't have to be.
        # All this method needs is to guarantee that the code block identified
        # by `file` (essentially the name) is NOT excecuted concurrently
        # by two "workers". It by no means has to be "locking that file".
        with file.lock():
            yield

    def __init__(
        self,
        path: Union[str, Path, Upath],
        thread_pool_executor: ThreadPoolExecutor = None,
        require_exists: bool = True,
    ):
        if isinstance(path, str):
            path = Path(path)
        if isinstance(path, Path):
            path = LocalUpath(str(path.absolute()))
        # Else it's something that already satisfies the
        # `Upath` protocol.
        self.path = path

        self._data_files: Optional[list] = None

        self._read_buffer: Optional[list] = None
        self._read_buffer_file: Optional[int] = None
        self._read_buffer_item_range: Optional[Tuple] = None
        # `self._read_buffer` contains the content of the file
        # indicated by `self._read_buffer_file`.

        self._append_buffer: List = []
        # This is for appending, butt is needed in some code for readinng.
        # For a read-only subclass, this stays empty and behaves correctly
        # where it is used.

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

    @property
    def _info_file(self) -> Upath:
        return self.path / "info.json"

    @property
    def _thread_pool(self):
        if self._thread_pool_ is None:
            executor = ThreadPoolExecutor(5)
            self._thread_pool_ = executor
            Finalize(self, executor.shutdown)

        return self._thread_pool_

    def __bool__(self) -> bool:
        return len(self) > 0

    def __getitem__(self, idx: int) -> T:
        """
        Element access by single index; negative index works as expected.

        This is not optimized for speed. For example, `self.get_data_files`
        involves an HTTP call. For better speed, use `__iter__`.

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

        if idx >= 0 and self._read_buffer_file is not None:
            n1, n2 = self._read_buffer_item_range  # type: ignore
            if n1 <= idx < n2:
                return self._read_buffer[idx - n1]  # type: ignore

        if idx < 0 and (-idx) <= len(self._append_buffer):
            return self._append_buffer[idx]

        # TODO: this is reliable only if there is no other "worker node"
        # changing the object by calling `append` or `extend`.
        datafiles = self.get_data_files()
        if self._data_files_cumlength_:
            length = self._data_files_cumlength_[-1]
        else:
            length = 0
        idx = range(length + len(self._append_buffer))[idx]

        if idx >= length:
            self._read_buffer_file = None
            return self._append_buffer[idx - length]  # type: ignore

        ifile0 = 0
        ifile1 = len(datafiles)
        if self._read_buffer_file is not None:
            n1, n2 = self._read_buffer_item_range  # type: ignore
            if idx < n1:
                ifile1 = (
                    self._read_buffer_file_idx_
                )  # pylint: disable=access-member-before-definition
            elif idx < n2:
                return self._read_buffer[idx - n1]  # type: ignore
            else:
                ifile0 = (
                    self._read_buffer_file_idx_ + 1
                )  # pylint: disable=access-member-before-definition

        # Now find the data file that contains the target item.

        ifile = bisect.bisect_right(
            self._data_files_cumlength_, idx, lo=ifile0, hi=ifile1
        )
        # `ifile`: index of data file that contains the target element.
        # `n`: total length before `ifile`.
        if ifile == 0:
            n = 0
        else:
            n = self._data_files_cumlength_[ifile - 1]
        self._read_buffer_item_range = (n, self._data_files_cumlength_[ifile])
        file = self.get_data_file(datafiles, ifile)
        if self._file_dumper is None:
            data = None
        else:
            data = self._file_dumper.get_file_data(file)
        if data is None:
            data = self.load_data_file(file)
        self._read_buffer_file = file
        self._read_buffer_file_idx_ = ifile
        self._read_buffer = data
        return data[idx - n]

    def __iter__(self) -> Iterator[T]:
        # Assuming the biglist will not change (not being appended to)
        # during iteration.
        self.flush()
        datafiles = self.get_data_files()
        ndatafiles = len(datafiles)

        if ndatafiles == 1:
            yield from self.load_data_file(self.get_data_file(datafiles, 0))
        elif ndatafiles > 1:
            max_workers = min(3, ndatafiles)
            tasks = queue.Queue(max_workers)
            executor = self._thread_pool

            for i in range(max_workers):
                t = executor.submit(
                    self.load_data_file, self.get_data_file(datafiles, i)
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
                        self.get_data_file(datafiles, nfiles_queued),
                    )
                    tasks.put(t)
                    nfiles_queued += 1

                yield from data

        # I don't think this is necessary.
        if self._append_buffer:
            yield from self._append_buffer

    def __len__(self) -> int:
        # This assumes the current object is the only one
        # that may be appending to the biglist.
        # In other words, if the current object is one of
        # of a number of workers that are concurrently using
        # the biglist, then all the other workers are reading only.
        self.get_data_files()
        if self._data_files_cumlength_:
            return self._data_files_cumlength_[-1] + len(self._append_buffer)
        return len(self._append_buffer)

    def file_view(self, file: Union[Upath, int]) -> FileView:
        if isinstance(file, int):
            datafiles = self.get_data_files()
            file = self.get_data_file(datafiles, file)
        return FileView(file, self.__class__.load_data_file)  # type: ignore

    def file_views(self) -> List[FileView]:
        # This is intended to facilitate parallel processing,
        # e.g. send views on diff files to diff `multiprocessing.Process`es.
        # However, `iter_files` may be a better way to do that.
        datafiles = self.get_data_files()
        return [
            self.file_view(self.get_data_file(datafiles, i))
            for i in range(len(datafiles))
        ]

    def get_data_files(self) -> list:
        raise NotImplementedError

    def get_data_file(self, datafiles, idx):
        # `datafiles` is the return of `get_datafiles`.
        # `idx` is the index of the file of interest in `datafiles`.
        raise NotImplementedError

    def view(self) -> ListView[T]:
        # During the use of this view, the underlying Biglist should not change.
        # Multiple views may be used to view diff parts
        # of the Biglist; they open and read files independent of
        # other views.
        self.flush()
        return ListView(self.__class__(self.path))

    def _concurrent_iter_info_file(self, task_id: str) -> Upath:
        """
        `task_id`: returned by `new_concurrent_iter`.
        """
        return self.path / ".concurrent_iter" / task_id / "info.json"

    def new_concurrent_iter(self) -> str:
        """
        One worker, such as a "coordinator", calls this method once.
        After that, one or more workers independently call `concurrent_iter`
        to iterate over the Biglist. `concurrent_iter` takes the task-ID returned by
        this method. The content of the Biglist is
        split between the workers because each data file will be obtained
        by exactly one worker.

        During this iteration, the Biglist object should stay unchanged---no
        calls to `append` and `extend`.
        """
        task_id = datetime.utcnow().isoformat()
        self._concurrent_iter_info_file(task_id).write_json(
            {"n_files_claimed": 0}, overwrite=True
        )
        return task_id

    def concurrent_iter(self, task_id: str) -> Iterator[T]:
        """
        `task_id`: returned by `new_concurrent_iter`.
        """
        datafiles = self.get_data_files()
        while True:
            ff = self._concurrent_iter_info_file(task_id)
            with self.lockfile(ff.with_suffix(".json.lock")):
                iter_info = ff.read_json()
                n_files_claimed = iter_info["n_files_claimed"]
                if n_files_claimed >= len(datafiles):
                    # No more date files to process.
                    break

                iter_info["n_files_claimed"] = n_files_claimed + 1
                ff.write_json(iter_info, overwrite=True)
                file = self.get_data_file(datafiles, n_files_claimed)

                fv = self.file_view(file)
                # This does not actually read the file.
                # TODO: make this read the file here?

            logger.debug('yielding data of file "%s"', file)
            yield from fv.data  # this is the data list contained in the data file

    def concurrent_iter_stat(self, task_id: str) -> dict:
        info = self._concurrent_iter_info_file(task_id).read_json()
        return {**info, "n_files": len(self.get_data_files())}

    def concurrent_iter_done(self, task_id: str) -> bool:
        zz = self.concurrent_iter_stat(task_id)
        return zz["n_files_claimed"] >= zz["n_files"]
