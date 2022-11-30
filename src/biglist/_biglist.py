import atexit
import concurrent.futures
import itertools
import json
import logging
import multiprocessing
import string
import time
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor, Future
from datetime import datetime
from typing import (
    Iterable,
    Iterator,
    Dict,
    Type,
    Callable,
    Optional,
)
from uuid import uuid4

from upathlib.serializer import (
    ByteSerializer,
    _loads,
    ZJsonSerializer,
    ZstdJsonSerializer,
    PickleSerializer,
    ZPickleSerializer,
    ZstdPickleSerializer,
    OrjsonSerializer,
    ZOrjsonSerializer,
    ZstdOrjsonSerializer,
)
from ._base import BiglistBase, FileView, Upath, PathType, T


logger = logging.getLogger(__name__)


_biglist_objs = weakref.WeakSet()


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


class Dumper:
    """
    This class performs file-saving in a thread pool.

    Parameters
    ----------
    n_threads:
        Max number of threads to use. There are at most
        this many submitted and unfinished file-dumping tasks
        at any time.
    """

    def __init__(self, executor: ThreadPoolExecutor, n_threads: int):
        self._executor: executor = executor
        self._n_threads = n_threads
        self._sem: threading.Semaphore = None  # type: ignore
        self._task_file_data: Dict[Future, tuple] = {}

    def _callback(self, t):
        self._sem.release()
        del self._task_file_data[t]
        if t.exception():
            raise t.exception()

    def dump_file(
        self, file_dumper: Callable[[Upath, list], None], data_file: Upath, data: list
    ):
        """
        Parameters
        ----------
        file_dumper:
            This function takes a file path and the data as a list, and saves
            the data in the file named by the path.

        data_file, data:
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
        self._task_file_data[task] = (data_file.name, data)
        # It's useful to keep the data here, as it will be needed
        # by `get_file_data`.
        task.add_done_callback(self._callback)
        # If task is already finished when this callback is being added,
        # then it is called immediately.

    def get_file_data(self, data_file: Upath):
        """
        This is for such a special need:

            Suppose 2 files are in the dump queue, hence not saved on disk yet,
            however, they're already in the file-list of the ``Biglist``'s meta info.
            Now if we access one element by index, and the code determines based on
            meta info that the element is in one of the files in-queue here.
            Then we can't load the file from disk (as it is not persisted yet);
            we can only get that file's data from the dump-queue via calling
            this method.
        """
        file_name = data_file.name
        for name, data in self._task_file_data.values():
            # `_task_file_data` is not long, so this is OK.
            if name == file_name:
                return data
        return None

    def wait(self):
        """
        Wait to finish all the submitted dumping tasks.
        """
        concurrent.futures.wait(list(self._task_file_data.keys()))


class Biglist(BiglistBase[T]):
    registered_storage_formats = {}

    DEFAULT_STORAGE_FORMAT = "pickle-zstd"

    @classmethod
    def register_storage_format(
        cls,
        name: str,
        serializer: Type[ByteSerializer],
        overwrite: bool = False,
    ):
        good = string.ascii_letters + string.digits + "-_"
        assert all(n in good for n in name)
        if name.replace("_", "-") in cls.registered_storage_formats:
            if not overwrite:
                raise ValueError(f"serializer '{name}' is already registered")
        name = name.replace("_", "-")
        cls.registered_storage_formats[name] = serializer

    @classmethod
    def dump_data_file(cls, path: Upath, data: list):
        """
        This method persists a batch of data elements, always a list,
        to disk or cloud storage.

        If a subclass wants to perform a transformation to each
        element of the list, e.g. converting an object of a
        custom class to that of a Python built-in type, it can override
        this method to do the transformation prior to calling
        the ``super()`` version.

        It is recommended to persist objects of Python built-in types only,
        which is future-proof compared to custom classes.
        One useful pattern is to dump result of ``instance.to_dict()``,
        and use ``cls.from_dict(...)`` to transform persisted data
        to user's custom type upon loading. Such conversions can be
        achieved by customizing the methods ``dump_data_file`` and
        ``load_data_file``. It may work just as well to leave these
        conversions to application code.
        """
        serializer = cls.registered_storage_formats[
            path.suffix.lstrip(".").replace("_", "-")
        ]
        path.write_bytes(serializer.serialize(data))

    @classmethod
    def load_data_file(cls, path: Upath):
        """
        This method loads a data file.

        If a subclass wants to perform a transformation to each
        element of the list, e.g. converting an object of a
        Python built-in type to that of a custom class, it can override
        this method to do the transformation on the output of
        the ``super()`` version. However, it may work just fine to
        leave that transformation to the application code once it
        has retrieved a data element.
        """
        deserializer = cls.registered_storage_formats[
            path.suffix.lstrip(".").replace("_", "-")
        ]
        data = path.read_bytes()
        return deserializer.deserialize(data)

    @classmethod
    def new(
        cls,
        path: PathType = None,
        *,
        batch_size: int = None,
        keep_files: bool = None,
        storage_format: str = None,
        **kwargs,
    ):
        """
        A Biglist object construction is in either of the two modes
        below:

        a) create a new Biglist to store new data.

        b) create a Biglist object pointing to storage of
           existing data, which was created by a previous call to ``Biglist.new``.

        In case (a), one has called ``Biglist.new``. In case (b), one has called
        ``Biglist(..)`` (i.e. ``__init__``).

        Some settings are applicable only in mode (a), b/c in
        mode (b) they can't be changed and, if needed, should only
        use the value already set in mode (a).
        Such settings should happen in this classmethod ``new``
        and should not be parameters to the the method ``__init__``.
        Examples include ``storage_format`` and ``batch_size``.

        These settings typically should be taken care of in ``new`` after creating
        the object with ``__init__``.

        ``__init__`` should be defined in such a way that it works for
        both a barebone object that is created in this ``new``, as well as a
        fleshed out object that already has data.

        Some settings may be applicable to an existing ``Biglist`` object,
        i.e. they control ways to use the object, and are not an intrinsic
        property of the object. Hence they can be set to diff values while
        using an existing Biglist object. Such settings should be
        parameters to ``__init__`` and not to ``new``. If specified in a call
        to ``new``, these parameters will be passed on to ``__init__``.

        Parameters
        ----------
        path:
            A directory in which this ``Biglist`` will save data files
            as well as meta-info files. The directory must be non-existent.
            It is not necessary to pre-create the parent directory of this path.

            If not specified, ``cls.get_temp_path``
            will be called to determine a temporary path.

        batch_size:
            max number of data elements in each persisted data file.

            There's no good default value for this parameter, although one is
            provided, because the code doesn't know (at the beginning of ``new``)
            the typical size of the data elements. User is recommended to
            specify the value of this parameter.

            In determining the value for ``batch_size``, the most important
            consideration is the size of each data file, which is determined
            by the typical size of the data elements as well as the the number
            of elements in each file. ``batch_size`` is the upper bound for the latter.

            The file size impacts a few things.

            - It should not be so small that the file reading/writing is large
              relative overhead. This is especially important when ``path`` is cloud storage.

            - It should not be so large that it is "unwieldy", e.g. approaching 1GB.

            - When iterating over a ``Biglist`` object, there can be up to (by default) 4
              files-worth of data in memory at any time. See the method ``iter_files``.

            - When ``append``ing or ``extend``ing at high speed, there can be up to
              (by default) 4 times ``batch_size`` data elements in memory at any time.
              See ``_flush`` and ``Dumper``.

            Another consideration is access pattern of elements in the ``Biglist``. If
            there are many "jumping around" with random element access, large data files
            will lead to very wasteful file loading, because to read any element,
            its hosting file must be read into memory. (HOWEVER, if your use pattern is
            heavy on random access, you SHOULD NOT use ``Biglist``.)

            If the Biglist is consumed by end-to-end iteration, then ``batch_size`` is not
            expected to be a sensitive setting, as long as it is in a reasonable range.

            Rule of thumb: it is recommended to keep the persisted files between 32-128MB
            in size. (Note: no benchmark was performed to back this recommendation.)

        keep_files:
            if not specified, the default behavior this the following:

            If ``path`` is ``None``, then this is ``False``---the temporary directory
            will be deleted when this `Biglist` object goes away.

            If ``path`` is not ``None``, i.e. user has deliberately specified a location,
            then this is ``True``---files saved by this ``Biglist`` object will stay.

            User can pass in ``True`` or ``False`` to override the default behavior.

        storage_format:
            this should be a key in ``cls.registered_storage_formats``.
            If not specified, ``cls.DEFAULT_STORAGE_FORMAT`` is used.

        kwargs:
            additional arguments are passed on to ``__init__``.

        Returns
        -------
        Biglist:
            A new ``Biglist`` object.
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
        elif path.is_file():
            raise FileExistsError(path)

        obj = cls(path, require_exists=False, **kwargs)  # type: ignore

        obj.keep_files = keep_files

        if not batch_size:
            batch_size = 10000
        else:
            assert batch_size > 0
        obj.info["batch_size"] = batch_size

        if storage_format is None:
            storage_format = cls.DEFAULT_STORAGE_FORMAT
        if storage_format.replace("_", "-") not in cls.registered_storage_formats:
            raise ValueError(f"invalid value of `storage_format`: '{storage_format}'")
        obj.info["storage_format"] = storage_format.replace("_", "-")
        obj.info["storage_version"] = 1
        # `storage_version` is a flag for certain breaking changes in the implementation,
        # such that certain parts of the code (mainly concerning I/O) need to
        # branch into different treatments according to the version.
        # This has little relation to `storage_format`.
        # version 0 designator introduced on 2022/3/8
        # version 1 designator introduced on 2022/7/25
        obj._info_file.write_json(obj.info, overwrite=False)

        return obj

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._data_files: Optional[list] = None
        self._data_files_cumlength_ = []
        self.keep_files = True

        _biglist_objs.add(self)

    def __del__(self) -> None:
        if self.keep_files:
            self.flush()
        else:
            self.path.rmrf()

    @property
    def batch_size(self) -> int:
        return self.info["batch_size"]

    @property
    def _data_dir(self) -> Upath:
        return self.path / "store"

    @property
    def storage_format(self) -> str:
        return self.info["storage_format"].replace("_", "-")

    @property
    def storage_version(self) -> int:
        return self.info.get("storage_version", 0)

    def append(self, x: T) -> None:
        """
        Append a single element to the in-memory buffer.
        Once the buffer size reaches ``self.batch_size``, the buffer's content
        will be written to a file, and the buffer will re-start empty.

        In other words, whenever ``self._append_buffer`` is non-empty,
        its content is not written to disk yet.
        However, at any time, the content of this buffer is included in
        ``self.__len__`` and in element accesses, including iterations.
        """
        self._append_buffer.append(x)
        if len(self._append_buffer) >= self.batch_size:
            self._flush()

    def extend(self, x: Iterable[T]) -> None:
        for v in x:
            self.append(v)

    def _flush(self, *, wait: bool = False):
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

        datafile_ext = self.storage_format.replace("-", "_")
        filename = f"{time.time()}_{uuid4()}_{buffer_len}.{datafile_ext}"
        # File name pattern introduced on 7/25/2022.
        # this should almost guarantee the file name is unique, hence
        # we do not need to verify this file name does not exist in `data_files`.
        # Also include timestamp and item count in the file name, in case
        # later we decide to use these pieces of info.

        data_file = self._data_dir / filename
        if self._file_dumper is None:
            self._file_dumper = Dumper(self._thread_pool, self._n_write_threads)
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

        with self.lockfile(self.path / "_n_datafiles_.txt.lock"):
            try:
                n = (self.path / "_n_datafiles_.txt").read_text()
            except FileNotFoundError:
                n = 0
            else:
                n = int(n)
            (self.path / "_n_datafiles_.txt").write_text(str(n + 1), overwrite=True)

    def flush(self):
        """
        While ``_flush`` is called automatically whenever the "append buffer"
        is full, this method is not called automatically, because this method
        is expected to be called only when the user is done adding elements
        to the list, yet the code has no way to know the user is "done".
        When the user is done adding elements, the "append buffer" could be
        only partially filled, hence ``_flush`` is not called, and the content
        of the buffer is not persisted to disk.

        This is when the *user* should call this method.
        (If user does not call, it is auto called when this object is going
        out of scope, if ``self.keep_files`` is ``True``.)

        In summary, call this method once the user is done with adding elements
        to the list *in this session*, meaning in this run of the program.
        """
        self._flush(wait=True)

    def _get_data_files(self) -> list:
        if self.storage_version < 1:
            # This may not be totally reliable in every scenario.
            # The older version had a parameter `lazy`, which is gone now.
            # After some time we may stop supporting this storage version. (7/27/2022)
            # However, as long as older datasets are in a "read-only" status,
            # this is fine.
            if self._data_files is None:
                try:
                    data_info_file = self.path / "datafiles_info.json"
                    self._data_files = data_info_file.read_json()
                except FileNotFoundError:
                    self._data_files = []
        else:
            try:
                nfiles = (self.path / "_n_datafiles_.txt").read_text()
            except FileNotFoundError:
                nfiles = 0
            else:
                nfiles = int(nfiles)

            if self._data_files is None or len(self._data_files) != nfiles:
                files = []
                if nfiles > 0:
                    for _ in range(5):
                        # Starting with storage_version 1, data file name is
                        #   <timestamp>_<uuid>_<itemcount>.<ext>
                        # <timestamp> contains a '.', no '_';
                        # <uuid> contains '-', no '_';
                        # <itemcount> contains no '-' nor '_';
                        # <ext> may contain '_'.
                        files0 = (v.name for v in self._data_dir.iterdir())
                        files1 = (v.split("_") + [v] for v in files0)
                        files2 = (
                            (float(v[0]), v[-1], int(v[2].partition(".")[0]))
                            # timestamp, file name, item count
                            for v in files1
                        )
                        files = sorted(files2)  # sort by timestamp
                        if len(files) == nfiles:
                            break
                        time.sleep(0.2)
                        # A few files may be being dumped and not done yet.
                        # Wait for them.

                    if len(files) != nfiles:
                        raise RuntimeError(
                            f"{nfiles} data files are expected, yet only {len(files)} are found"
                        )

                if files:
                    self._data_files = [
                        (v[1], v[2]) for v in files
                    ]  # file name, item count
                else:
                    self._data_files = []

        if self._data_files:
            self._data_files_cumlength_ = list(
                itertools.accumulate(v[1] for v in self._data_files)
            )
        else:
            self._data_files_cumlength_ = []

        return self._data_files, self._data_files_cumlength_  # type: ignore

    @property
    def datafiles(self):
        df, _ = self._get_data_files()
        return [str(self._get_data_file(df, i)) for i in range(len(df))]

    @property
    def datafiles_info(self):
        files = self.datafiles
        counts = (v[1] for v in self._data_files)
        cumcounts = self._data_files_cumlength_
        return list(zip(files, counts, cumcounts))

    def _get_data_file(self, datafiles, idx) -> Upath:
        # `datafiles` is the return of `_get_datafiles`.
        return self._data_dir / datafiles[idx][0]

    def file_view(self, file):
        if isinstance(file, int):
            datafiles, _ = self._get_data_files()
            file = self._get_data_file(datafiles, file)
        return BiglistFileData(file, self.load_data_file)

    def iter_files(self):
        self.flush()
        return super().iter_files()

    def view(self):
        self.flush()
        return super().view()

    def _multiplex_info_file(self, task_id: str) -> Upath:
        """
        `task_id`: returned by `new_multiplexer`.
        """
        return self.path / ".multiplexer" / task_id / "info.json"

    def new_multiplexer(self) -> str:
        """
        One worker, such as a "coordinator", calls this method once.
        After that, one or more workers independently call ``multiplex_iter``
        to iterate over the Biglist. ``multiplex_iter`` takes the task-ID returned by
        this method. The content of the Biglist is
        split between the workers because each data item will be obtained
        by exactly one worker.

        During this iteration, the Biglist object should stay unchanged---no
        calls to ``append`` and ``extend``.

        Difference between ``concurrent_iter`` and ``multiplex_iter``: the former
        distributes files to workers, whereas the latter distributes individual
        data elements to workers.

        Use case of multiplexer: each data element represents considerable amounts
        of work--it is a "hyper-parameter" or the like; ``multiplex_iter`` facilitates
        splitting the work represented by different values of the "hyper-parameter"
        between multiple workers.
        """
        assert not self._append_buffer
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

    def multiplex_iter(self, task_id: str, worker_id: str = None) -> Iterator[T]:
        """
        Parameters
        ----------
        task_id:
            Returned by ``new_multiplexer``.
        """
        if not worker_id:
            worker_id = "{} {}".format(
                multiprocessing.current_process().name,
                threading.current_thread().name,
            )
        finfo = self._multiplex_info_file(task_id)
        flock = finfo.with_suffix(finfo.suffix + ".lock")
        while True:
            with self.lockfile(flock):
                # In concurrent use cases, I've observed
                # `upathlib.LockAcquireError` raised here.
                # User may want to do retry here.
                ss = finfo.read_json()
                # In concurrent use cases, I've observed
                # `FileNotFoundError` here. User may want
                # to do retry here.

                n = ss["next"]
                if n == ss["total"]:
                    return
                finfo.write_json(
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
        return self._multiplex_info_file(task_id).read_json()

    def multiplex_done(self, task_id: str) -> bool:
        ss = self.multiplex_stat(task_id)
        return ss["next"] == ss["total"]


class BiglistFileData(FileView):
    def __init__(self, path, loader):
        self._data: list = None
        super().__init__(path, loader)

    def load(self):
        if self._data is None:
            self._data = self.loader(self.path)

    def data(self):
        self.load()
        return self._data

    def __len__(self):
        return len(self.data())

    def __getitem__(self, idx: int):
        return self.data()[idx]

    def __iter__(self):
        return iter(self.data())


class JsonByteSerializer(ByteSerializer):
    @classmethod
    def serialize(cls, x, **kwargs):
        return json.dumps(x, **kwargs).encode()

    @classmethod
    def deserialize(cls, y, **kwargs):
        return _loads(json.loads, y.decode(), **kwargs)


Biglist.register_storage_format("json", JsonByteSerializer)
Biglist.register_storage_format("json-z", ZJsonSerializer)
Biglist.register_storage_format("json-zstd", ZstdJsonSerializer)
Biglist.register_storage_format("pickle", PickleSerializer)
Biglist.register_storage_format("pickle-z", ZPickleSerializer)
Biglist.register_storage_format("pickle-zstd", ZstdPickleSerializer)
Biglist.register_storage_format("orjson", OrjsonSerializer)
Biglist.register_storage_format("orjson-z", ZOrjsonSerializer)
Biglist.register_storage_format("orjson-zstd", ZstdOrjsonSerializer)
