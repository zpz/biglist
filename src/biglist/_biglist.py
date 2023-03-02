from __future__ import annotations

import atexit
import concurrent.futures
import io
import itertools
import json
import logging
import multiprocessing
import os
import string
import threading
import warnings
import weakref
from collections.abc import Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from typing import (
    Any,
    Callable,
    Optional,
)
from uuid import uuid4

import pyarrow
from upathlib.serializer import (
    ByteSerializer,
    OrjsonSerializer,
    PickleSerializer,
    ZOrjsonSerializer,
    ZPickleSerializer,
    ZstdOrjsonSerializer,
    ZstdPickleSerializer,
    _loads,
)

from ._base import (
    BiglistBase,
    Element,
    FileReader,
    FileSeq,
    PathType,
    Upath,
)
from ._util import lock_to_use

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


class Biglist(BiglistBase[Element]):
    registered_storage_formats = {}

    DEFAULT_STORAGE_FORMAT = "pickle-zstd"

    @classmethod
    def register_storage_format(
        cls,
        name: str,
        serializer: type[ByteSerializer],
        overwrite: bool = False,
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
            ``storage_format='xyz'`` in calls to ``new``.
            When reading the object back from persistence.
            make sure this registry is also in place so that the correct
            deserializer can be found.

        serializer
            A subclass of `upathlib.serializer.ByteSerializer <https://github.com/zpz/upathlib/blob/main/src/upathlib/serializer.py>`_.

        overwrite
            Whether to overwrite an existent registrant by the same name.
        """
        good = string.ascii_letters + string.digits + "-_"
        assert all(n in good for n in name)
        if name.replace("_", "-") in cls.registered_storage_formats:
            if not overwrite:
                raise ValueError(f"serializer '{name}' is already registered")
        name = name.replace("_", "-")
        cls.registered_storage_formats[name] = serializer

    @classmethod
    def dump_data_file(cls, path: Upath, data: list[Element]) -> None:
        """
        This method persists a batch of data elements, always a list,
        to disk or cloud storage.

        It is recommended to persist objects of Python built-in types only,
        unless the :class:`Biglist` is being used on-the-fly temporarily.
        One useful pattern is to :meth:`append` output of ``custom_instance.to_dict()``,
        and use ``custom_class.from_dict(...)`` upon reading to transform
        the persisted dict back to the custom type.

        If a subclass wants to perform such ``to_dict``/``from_dict``
        transformations for the user, it can customize :meth:`dump_data_file`
        and :meth:`load_data_file`.

        See Also
        --------
        :meth:`load_data_file`
        """
        serializer = cls.registered_storage_formats[
            path.suffix.lstrip(".").replace("_", "-")
        ]
        path.write_bytes(serializer.serialize(data))

    @classmethod
    def load_data_file(cls, path: Upath) -> list[Element]:
        """Load the data file given by ``path``.

        This function is used as the argument ``loader`` to :meth:`BiglistFileReader.__init__`.
        The value it returns is contained in :class:`FileReader` for subsequent use.

        See Also
        --------
        dump_data_file
        """
        deserializer = cls.registered_storage_formats[
            path.suffix.lstrip(".").replace("_", "-")
        ]
        data = path.read_bytes()
        return deserializer.deserialize(data)

    @classmethod
    def new(
        cls,
        path: Optional[PathType] = None,
        *,
        batch_size: Optional[int] = None,
        storage_format: Optional[str] = None,
        init_info: dict = None,
        **kwargs,
    ) -> Biglist:
        """
        Parameters
        ----------
        path
            Passed on to :meth:`BiglistBase.new` of ``BiglistBase``.
        batch_size
            Max number of data elements in each persisted data file.

            There's no good default value for this parameter, although one is
            provided (currently the default is 10000),
            because the code of :meth:`new` doesn't know
            the typical size of the data elements. User is recommended to
            specify the value of this parameter.

            In choosing a value for ``batch_size``, the most important
            consideration is the size of each data file, which is determined
            by the typical size of the data elements as well as ``batch_size``,
            which is the upper bound of the the number
            of elements in each file.

            There are several considerations about the data file sizes:

            - It should not be so small that the file reading/writing is a large
              overhead relative to actual processing of the data.
              This is especially important when ``path`` is cloud storage.

            - It should not be so large that it is "unwieldy", e.g. approaching 1GB.

            - When :meth:`~_base.BiglistBase.__iter__`\\ating over a :class:`Biglist` object, there can be up to (by default) 4
              files-worth of data in memory at any time. See the method :meth:`iter_files`.

            - When :meth:`append`\\ing or :meth:`extend`\\ing to a :class:`Biglist` object at high speed,
              there can be up to (by default) 4 times ``batch_size`` data elements in memory at any time.
              See :meth:`_flush` and :class:`Dumper`.

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

        **kwargs
            additional arguments are passed on to :meth:`BiglistBase.new`.

        Returns
        -------
        Biglist
            A new :class:`Biglist` object.
        """
        if not batch_size:
            batch_size = 10000
        else:
            assert batch_size > 0

        if storage_format is None:
            storage_format = cls.DEFAULT_STORAGE_FORMAT
        if storage_format.replace("_", "-") not in cls.registered_storage_formats:
            raise ValueError(f"invalid value of `storage_format`: '{storage_format}'")

        init_info = {
            **(init_info or {}),
            "storage_format": storage_format.replace("_", "-"),
            "storage_version": 3,
            # `storage_version` is a flag for certain breaking changes in the implementation,
            # such that certain parts of the code (mainly concerning I/O) need to
            # branch into different treatments according to the version.
            # This has little relation to `storage_format`.
            # version 0 designator introduced on 2022/3/8
            # version 1 designator introduced on 2022/7/25
            # version 2 designator introduced in version 0.7.4.
            # version 3 designator introduced in version 0.7.7.
            "batch_size": batch_size,
            "data_files_info": [],
        }

        obj = super().new(path, init_info=init_info, **kwargs)  # type: ignore
        return obj

    def __init__(self, *args, **kwargs):
        """Please see doc of the base class."""
        super().__init__(*args, **kwargs)
        self.keep_files: bool = True
        """Indicates whether the persisted files should be kept or deleted when the object is garbage-collected."""

        self._append_buffer: list = []
        self._append_files_buffer: list = []
        self._file_dumper = None
        self._n_write_threads = 3

        _biglist_objs.add(self)
        self._flushed = True

        # For back compat.
        if self.info.get("storage_version", 0) < 3:
            # This is not called by ``new``, instead is opening an existing dataset.
            # Usually these legacy datasets are in a "read-only" mode, i.e., you should
            # not append more data to them. If you do, the back-compat code below
            # may not be totally reliable if the dataset is being used by multiple workers
            # concurrently.
            if "data_files_info" not in self.info:  # added in 0.7.4
                if self.storage_version == 0:
                    # This may not be totally reliable in every scenario.
                    # The older version had a parameter `lazy`, which is gone now.
                    # After some time we may stop supporting this storage version. (7/27/2022)
                    # However, as long as older datasets are in a "read-only" status,
                    # this is fine.
                    try:
                        data_info_file = self.path / "datafiles_info.json"
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
                    files1 = (v.split("_") + [v] for v in files0)
                    files2 = (
                        (float(v[0]), v[-1], int(v[2].partition(".")[0]))
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

                self.info["data_files_info"] = data_files_info
                with lock_to_use(self._info_file) as ff:
                    ff.write_json(self.info, overwrite=True)

            else:
                # Added in 0.7.5: check for a bug introduced in 0.7.4.
                # Convert full path to file name.
                # Version 0.7.4 was used very briefly, hence very few datasets
                # were created by that version.
                data_files_info = self.info["data_files_info"]
                if data_files_info:
                    new_info = None
                    if os.name == "nt" and "\\" in data_files_info[0][0]:
                        new_info = [
                            (f[(f.rfind("\\") + 1) :], *_) for f, *_ in data_files_info
                        ]
                    elif "/" in data_files_info[0][0]:
                        new_info = [
                            (f[(f.rfind("/") + 1) :], *_) for f, *_ in data_files_info
                        ]
                    if new_info:
                        self.info["data_files_info"] = new_info
                        with lock_to_use(self._info_file) as ff:
                            ff.write_json(self.info, overwrite=True)

    def __del__(self) -> None:
        if getattr(self, "keep_files", True) is False:
            self.destroy()
        else:
            if not self._flushed:
                warnings.warn(
                    f"did you forget to flush {self.__class__.__name__} at '{self.path}'?"
                )
            self.flush()

    @property
    def batch_size(self) -> int:
        """The max number of data items in one data file."""
        return self.info["batch_size"]

    @property
    def data_path(self) -> Upath:
        return self.path / "store"

    @property
    def storage_format(self) -> str:
        """The value of ``storage_format`` used in :meth:`new`, either user-specified or the default value."""
        return self.info["storage_format"].replace("_", "-")

    @property
    def storage_version(self) -> int:
        """The internal format used in persistence. This is a read-only attribute for information only."""
        return self.info.get("storage_version", 0)

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
        if not self._flushed:
            warnings.warn(
                f"did you forget to flush {self.__class__.__name__} at '{self.path}'?"
            )
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
        if not self._flushed:
            warnings.warn(
                f"did you forget to flush {self.__class__.__name__} at '{self.path}'?"
            )
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

        .. note:: Use the "spawn" method to start processes.
            In ``multiprocessing``, look for the method `get_context <https://docs.python.org/3/library/multiprocessing.html#multiprocessing.get_context>`_.
            In ``concurrent.futures.ProcessPoolExecutor``, look for the parameter `mp_context <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ProcessPoolExecutor>`_.
            Also check out `mpservice.util.MP_SPAWN_CTX <https://mpservice.readthedocs.io/en/latest/util.html#mpservice.util.MP_SPAWN_CTX>`_.
        """
        self._append_buffer.append(x)
        self._flushed = False  # This is about `flush`, not `_flush`.
        if len(self._append_buffer) >= self.batch_size:
            self._flush()

    def extend(self, x: Iterable[Element]) -> None:
        """This simply calls :meth:`append` repeatedly."""
        for v in x:
            self.append(v)

    def _flush(self, *, wait: bool = False) -> None:
        """
        Persist the content of the in-memory buffer to a file,
        reset the buffer, and update relevant book-keeping variables.

        This method is called any time the size of the in-memory buffer
        reaches ``self.batch_size``. This happens w/o the user's intervention.
        """
        if not self._append_buffer:
            if self._file_dumper is not None and wait:
                self._file_dumper.wait()
            return

        buffer = self._append_buffer
        buffer_len = len(buffer)
        self._append_buffer = []

        datafile_ext = self.storage_format.replace("-", "_")
        filename = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S.%f')}_{uuid4()}_{buffer_len}.{datafile_ext}"
        # File name pattern introduced on 7/25/2022.
        # this should almost guarantee the file name is unique, hence
        # we do not need to verify this file name does not exist in `data_files`.
        # Also include timestamp and item count in the file name, in case
        # later we decide to use these pieces of info.
        # Changes in 0.7.4: the time part changes from epoch to datetime, with guaranteed fixed length.

        data_file = self.data_path / filename
        if self._file_dumper is None:
            self._file_dumper = Dumper(self._get_thread_pool(), self._n_write_threads)
        if wait:
            self._file_dumper.wait()
            self.dump_data_file(data_file, buffer)
        else:
            self._file_dumper.dump_file(self.dump_data_file, data_file, buffer)
            # This call will return quickly if the dumper has queue
            # capacity for the file. The file meta data below
            # will be updated as if the saving has completed, although
            # it hasn't (it is only queued). This allows the waiting-to-be-saved
            # data to be accessed property.

            # TODO:
            # what if dump fails later? The 'n_data_files' file is updated
            # already assuming everything will be fine.

        self._append_files_buffer.append((filename, buffer_len))

    def flush(self) -> None:
        """
        :meth:`_flush` is called automatically whenever the "append buffer"
        is full, so to persist the data and empty the buffer.
        (The capacity of this buffer is equal to ``self.batch_size``.)
        However, if this buffer is only partially filled when the user is done
        adding elements to the biglist, the data in the buffer will not be persisted.
        This is the first reason that user should call ``flush`` when they are done
        adding data (via :meth:`append` or :meth:`extend`).

        Although :meth:`_flush` creates new data files, it does not update the "meta info file"
        (``info.json`` in the root of ``self.path``) to include the new data files;
        it only updates the in-memory ``self.info``. This is for efficiency reasons,
        because updating ``info.json`` involves locking.

        Updating ``info.json`` to include new data files (created due to :meth:`append` and :meth:`extend`)
        if performed by :meth:`flush`.
        This is the second reason that user should call :meth:`flush` at the end of their
        data writting session, regardless of whether all the new data have been persisted
        in data files. (They would be if their count is a multiple of ``self.batch_size``.)

        If there are multiple workers adding data to this biglist at the same time
        (from multiple processes or machines), data added by other workers will be
        invisible to this worker until :meth:`flush` or :meth:`reload` is called.

        Further, user should assume that data not yet persisted are not visible to
        data reading via :meth:`__getitem__` or :meth:`__iter__`, and not included in
        :meth:`__len__`. In common use cases, we do not start reading data until we're done
        adding data to the biglist (at least "for now").

        In summary, call :meth:`flush` when

        - You are done adding data (for this "session")
        - or you need to start reading data

        :meth:`flush` has overhead. You should call it only in the two situations above.
        **Do not** call it frequently "just to be safe".

        After a call to ``flush()``, there's no problem to add more elements again by
        :meth:`append` or :meth:`extend`. Data files created by ``flush()`` with less than
        :data:`batch_size` elements will stay as is among larger files.
        This is a legitimate case in parallel or distributed writing, or writing in
        multiple sessions.
        """
        if self._flushed:
            return

        self._flush(wait=True)

        # Other workers in other threads, processes, or machines may have appended data
        # to the list. This block merges the appends by the current worker with
        # appends by other workers. The last call to ``flush`` across all workers
        # will get the final meta info right.
        with lock_to_use(self._info_file) as ff:
            z0 = ff.read_json()["data_files_info"]
            z = sorted(set((*(tuple(v[:2]) for v in z0), *self._append_files_buffer)))
            # TODO: maybe a merge sort can be more efficient.
            cum = list(itertools.accumulate(v[1] for v in z))
            z = [(a, b, c) for (a, b), c in zip(z, cum)]
            self.info["data_files_info"] = z
            ff.write_json(self.info, overwrite=True)

        self._append_files_buffer.clear()
        self._flushed = True

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
        # with lock_to_use(self._info_file):
        # self.info = self._info_file.read_json()
        self.info = self._info_file.read_json()

    @property
    def files(self):
        # This method should be cheap to call.
        if not self._flushed:
            warnings.warn(
                f"did you forget to flush {self.__class__.__name__} at '{self.path}'?"
            )
        return BiglistFileSeq(
            self.path,
            [
                (str(self.data_path / row[0]), *row[1:])
                for row in self.info["data_files_info"]
            ],
            self.load_data_file,
        )

    def _multiplex_info_file(self, task_id: str) -> Upath:
        """
        `task_id`: returned by :meth:`new_multiplexer`.
        """
        return self.path / ".multiplexer" / task_id / "info.json"

    def new_multiplexer(self) -> str:
        """
        One worker, such as a "coordinator", calls this method once.
        After that, one or more workers independently call :meth:`multiplex_iter`
        to iterate over the :class:`Biglist`. :meth:`multiplex_iter` takes the task-ID returned by
        this method. The content of the :class:`Biglist` is
        split between the workers in that each data element will be obtained
        by exactly one worker.

        During this iteration, the :class:`Biglist` object should stay unchanged---no
        calls to :meth:`append` and :meth:`extend`.

        Difference between :meth:`~_base.BiglistBase.concurrent_iter_files` and :meth:`multiplex_iter`: the former
        distributes files to workers, whereas the latter distributes individual
        data elements to workers.

        The intended use case of multiplexer: each data element represents considerable amounts
        of work--it is a "hyper-parameter" or the like; :meth:`multiplex_iter` facilitates
        splitting the work represented by different values of this "hyper-parameter"
        between multiple workers.
        """
        assert not self._append_buffer
        if not self._flushed:
            warnings.warn(
                f"did you forget to flush {self.__class__.__name__} at '{self.path}'?"
            )
        task_id = datetime.utcnow().isoformat()
        self._multiplex_info_file(task_id).write_json(
            {
                "total": len(self),
                "next": 0,
                "time": datetime.utcnow().isoformat(),
            },
            overwrite=False,
        )
        return task_id

    def multiplex_iter(
        self, task_id: str, worker_id: Optional[str] = None
    ) -> Iterator[Element]:
        """
        Parameters
        ----------
        task_id
            The string returned by :meth:`new_multiplexer`.
        worker_id
            A string representing a particular worker. If missing,
            a default is constructed based on thread name and process name of the worker.
        """
        if not worker_id:
            worker_id = "{} {}".format(
                multiprocessing.current_process().name,
                threading.current_thread().name,
            )
        finfo = self._multiplex_info_file(task_id)
        while True:
            with lock_to_use(finfo) as ff:
                # In concurrent use cases, I've observed
                # `upathlib.LockAcquireError` raised here.
                # User may want to do retry here.
                ss = ff.read_json()
                # In concurrent use cases, I've observed
                # `FileNotFoundError` here. User may want
                # to do retry here.

                n = ss["next"]
                if n == ss["total"]:
                    return
                ff.write_json(
                    {
                        "next": n + 1,
                        "worker_id": worker_id,
                        "time": datetime.utcnow().isoformat(),
                        "total": ss["total"],
                    },
                    overwrite=True,
                )
            yield self[n]

    def multiplex_stat(self, task_id: str) -> dict:
        """
        Return status info of an ongoing "multiplex iter".
        """
        return self._multiplex_info_file(task_id).read_json()

    def multiplex_done(self, task_id: str) -> bool:
        """Return whether the "multiplex iter" identified by ``task_id`` is finished."""
        ss = self.multiplex_stat(task_id)
        return ss["next"] == ss["total"]


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
        self._sem: Optional[threading.Semaphore] = None  # type: ignore
        self._tasks: set[Future] = set()

    def _callback(self, t):
        self._sem.release()
        self._tasks.remove(t)
        if t.exception():
            raise t.exception()

    def dump_file(
        self, file_dumper: Callable[[Upath, list], None], data_file: Upath, data: list
    ):
        """
        Parameters
        ----------
        file_dumper
            This function takes a file path and the data as a list, and saves
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

        task = self._executor.submit(file_dumper, data_file, data)
        self._tasks.add(task)
        task.add_done_callback(self._callback)
        # If task is already finished when this callback is being added,
        # then it is called immediately.

    def wait(self):
        """
        Wait to finish all the submitted dumping tasks.
        """
        concurrent.futures.wait(self._tasks)


class BiglistFileReader(FileReader[Element]):
    def __init__(self, path: PathType, loader: Callable[[Upath], Any]):
        """
        Parameters
        ----------
        path
            Path of a data file.
        loader
            Usually this is :meth:`Biglist.load_data_file`.
            If you customize this, please see the doc of :meth:`FileReader.__init__`.
        """
        self._data: Optional[list] = None
        super().__init__(path, loader)

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


class JsonByteSerializer(ByteSerializer):
    @classmethod
    def serialize(cls, x, **kwargs):
        return json.dumps(x, **kwargs).encode()

    @classmethod
    def deserialize(cls, y, **kwargs):
        return _loads(json.loads, y.decode(), **kwargs)


class ParquetSerializer(ByteSerializer):
    @classmethod
    def serialize(cls, x: list[dict], schema=None, metadata=None, **kwargs):
        """
        `x` is a list of data items. Each item is a dict. In the output Parquet file,
        each item is a "row".

        The content of the item dict should follow a regular pattern.
        Not every structure is supported. The data `x` must be acceptable to
        ``pyarrow.Table.from_pylist``. If unsure, use a list with a couple data elements
        and experiment with ``pyarrow.Table.from_pylist`` directly, leaving out
        ``schema`` and ``metadata``.

        When using ``storage_format='parquet'`` for ``Biglist``, each data element is a dict
        with a consistent structure that is acceptable to ``pyarrow.Table.from_pylist``.
        When reading the Biglist, the original Python data elements are returned.
        In other words, the reading is *not* like that of ``ParquetBiglist``.
        You can always create a separate ParquetBiglist for the data files of the Biglist
        in order to use Parquet-style data reading. The data files are valid Parquet files.
        """
        table = pyarrow.Table.from_pylist(x, schema=schema, metadata=metadata)
        sink = io.BytesIO()
        writer = pyarrow.parquet.ParquetWriter(sink, table.schema, **kwargs)
        writer.write_table(table)
        writer.close()
        # return sink.getvalue()
        return sink.getbuffer()

    @classmethod
    def deserialize(cls, y: bytes, **kwargs):
        table = pyarrow.parquet.ParquetFile(io.BytesIO(y), **kwargs).read()
        return table.to_pylist()


Biglist.register_storage_format("json", JsonByteSerializer)
Biglist.register_storage_format("pickle", PickleSerializer)
Biglist.register_storage_format("pickle-z", ZPickleSerializer)
Biglist.register_storage_format("pickle-zstd", ZstdPickleSerializer)
Biglist.register_storage_format("orjson", OrjsonSerializer)
Biglist.register_storage_format("orjson-z", ZOrjsonSerializer)
Biglist.register_storage_format("orjson-zstd", ZstdOrjsonSerializer)
Biglist.register_storage_format("parquet", ParquetSerializer)
