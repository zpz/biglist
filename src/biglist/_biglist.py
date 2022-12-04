from __future__ import annotations

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
    List,
    Tuple,
    Type,
    Callable,
    Optional,
    Union,
    Any,
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
from ._base import BiglistBase, FileReader, Upath, PathType, T, ListView


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
    n_threads
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
            however, they're already in the file-list of the ``Biglist``s meta info.
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
            A subclass of ``ByteSerializer``.

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
    def dump_data_file(cls, path: Upath, data: list):
        """
        This method persists a batch of data elements, always a list,
        to disk or cloud storage.

        It is recommended to persist objects of Python built-in types only,
        unless the ``Biglist`` is being used on-the-fly temporarily.
        One useful pattern is to ``append`` output of ``custom_instance.to_dict()``,
        and use ``custom_class.from_dict(...)`` upon reading to transform
        the persisted ``dict`` back to the custom type.

        If a subclass wants to perform such ``to_dict``/``from_dict``
        transformations for the user, it can customize ``dump_data_file``
        and ``load_data_file``.

        See Also
        --------
        load_data_file
        """
        serializer = cls.registered_storage_formats[
            path.suffix.lstrip(".").replace("_", "-")
        ]
        path.write_bytes(serializer.serialize(data))

    @classmethod
    def load_data_file(cls, path: Upath) -> List[T]:
        """Load the data file given by ``path``.

        This function is used as the argument ``loader`` to ``BiglistFileReader.__init__``.

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
        path: PathType = None,
        *,
        batch_size: int = None,
        keep_files: bool = None,
        storage_format: str = None,
        **kwargs,
    ) -> Biglist:
        """
        Parameters
        ----------
        path
            A directory in which this ``Biglist`` will save data files
            as well as meta-info files. The directory must be non-existent.
            It is not necessary to pre-create the parent directory of this path.

            This path can be either on local disk or in a cloud storage.

            If not specified, ``cls.get_temp_path``
            will be called to determine a temporary path.

        batch_size
            Max number of data elements in each persisted data file.

            There's no good default value for this parameter, although one is
            provided (currently the default is 10000),
            because the code of ``new`` doesn't know
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

            - When ``__iter__``\\ating over a ``Biglist`` object, there can be up to (by default) 4
              files-worth of data in memory at any time. See the method ``iter_files``.

            - When ``append``\\ing or ``extend``\\ing to a ``Biglist`` object at high speed,
              there can be up to (by default) 4 times ``batch_size`` data elements in memory at any time.
              See ``_flush`` and ``Dumper``.

            Another consideration is access pattern of elements in the ``Biglist``. If
            there are many "jumping around" with random element access, large data files
            will lead to very wasteful file loading, because to read any element,
            its hosting file must be read into memory. (After all, if the application is
            heavy on random access, then ``Biglist`` is **not** the right tool.)

            The performance of iteration is not expected to be highly sensitive to the value
            of ``batch_size``, as long as it is in a reasonable range.

            A rule of thumb: it is recommended to keep the persisted files between 32-128MB
            in size. (Note: no benchmark was performed to back this recommendation.)

        keep_files
            If not specified, the default behavior is the following:

                If ``path`` is ``None``, then this is ``False``---the temporary directory
                will be deleted when this ``Biglist`` object goes away.

                If ``path`` is not ``None``, i.e. user has deliberately specified a location,
                then this is ``True``---files saved by this ``Biglist`` object will stay.

            User can pass in ``True`` or ``False`` explicitly to override the default behavior.

        storage_format
            this should be a key in ``cls.registered_storage_formats``.
            If not specified, ``cls.DEFAULT_STORAGE_FORMAT`` is used.

        **kwargs
            additional arguments are passed on to ``__init__``.

        Returns
        -------
        Biglist
            A new ``Biglist`` object.

        Notes
        -----
        A ``Biglist`` object construction is in either of the two modes
        below:

        a) create a new ``Biglist`` to store new data.

        b) create a ``Biglist`` object pointing to storage of
           existing data, which was created by a previous call to ``new``.

        In case (a), one has called ``Biglist.new``. In case (b), one has called
        ``Biglist(..)`` (i.e. ``__init__``).

        Some settings are applicable only in mode (a), b/c in
        mode (b) they can't be changed and, if needed, should only
        use the value already set in mode (a).
        Such settings can be parameters to ``new``
        but should not be parameters to ``__init__``.
        Examples include ``storage_format`` and ``batch_size``.
        These settings typically should be taken care of in ``new``,
        before and/or after the object has been created by a call to ``__init__``
        within ``new``.

        ``__init__`` should be defined in such a way that it works for
        both a barebone object that is created in this ``new``, as well as a
        fleshed out object that already has data in persistence.

        Some settings may be applicable to an existing ``Biglist`` object, e.g.,
        they control styles of display and not intrinsic attributes persisted along
        with the ``Biglist``.,
        Such settings should be parameters to ``__init__`` but not to ``new``.
        If provided in a call to ``new``, these parameters will be passed on to ``__init__``.

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
        """Please see doc of the base class."""
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
        """The value of ``storage_format`` used in ``new``, either user-specified or the default value."""
        return self.info["storage_format"].replace("_", "-")

    @property
    def storage_version(self) -> int:
        """The internal format used in persistence. This is a read-only attribute for information only."""
        return self.info.get("storage_version", 0)

    def append(self, x: T) -> None:
        """
        Append a single element to the ``Biglist``.

        In implementation, this appends to an in-memory buffer.
        Once the buffer size reaches ``self.batch_size``, the buffer's content
        will be persisted as a new data file, and the buffer will re-start empty.
        In other words, whenever the buffer is non-empty,
        its content is not yet persisted.
        However, at any time, the content of this buffer is included in
        ``self.__len__`` as well as in element accesses by ``__getitem__`` and ``__iter__``.
        """
        self._append_buffer.append(x)
        if len(self._append_buffer) >= self.batch_size:
            self._flush()

    def extend(self, x: Iterable[T]) -> None:
        """This simply calls ``append`` repeatedly."""
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

    def flush(self) -> None:
        """
        While ``_flush`` is called automatically whenever the "append buffer"
        is full, ``flush`` is not called automatically.
        When the user is done adding elements to the biglist, the "append buffer" could be
        partially filled, hence not yet persisted.
        This is when the *user* should call ``flush`` to force dumping the content of the buffer.
        (If user forgot to call ``flush`` and ``self.keep_files`` is ``True``,
        it is auto called when this object goes away. However, user should call ``flush`` for the explicity.)

        After a call to ``flush``, there's no problem to add more elements again by
        ``append`` or ``extend``. Data files created by ``flush`` with less than
        ``batch_size`` elements will stay as is among larger files.
        This is a legitimate case in parallel or distributed writing, or writing in
        multiple sessions.
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
    def datafiles(self) -> List[str]:
        df, _ = self._get_data_files()
        return [str(self._get_data_file(df, i)) for i in range(len(df))]

    @property
    def datafiles_info(self) -> List[Tuple[str, int, int]]:
        files = self.datafiles
        counts = (v[1] for v in self._data_files)
        cumcounts = self._data_files_cumlength_
        return list(zip(files, counts, cumcounts))

    def _get_data_file(self, datafiles, idx) -> Upath:
        # `datafiles` is the return of `_get_datafiles`.
        return self._data_dir / datafiles[idx][0]

    def file_reader(self, file: Union[Upath, int]) -> BiglistFileReader:
        if isinstance(file, int):
            datafiles, _ = self._get_data_files()
            file = self._get_data_file(datafiles, file)
        return BiglistFileReader(file, self.load_data_file)

    def iter_files(self) -> Iterator[BiglistFileReader]:
        self.flush()
        return super().iter_files()

    def view(self) -> ListView:
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
        to iterate over the ``Biglist``. ``multiplex_iter`` takes the task-ID returned by
        this method. The content of the ``Biglist`` is
        split between the workers in that each data element will be obtained
        by exactly one worker.

        During this iteration, the ``Biglist`` object should stay unchanged---no
        calls to ``append`` and ``extend``.

        Difference between ``concurrent_iter`` and ``multiplex_iter``: the former
        distributes files to workers, whereas the latter distributes individual
        data elements to workers.

        The intended use case of multiplexer: each data element represents considerable amounts
        of work--it is a "hyper-parameter" or the like; ``multiplex_iter`` facilitates
        splitting the work represented by different values of this "hyper-parameter"
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
        task_id
            The string returned by ``new_multiplexer``.
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
        """
        Return status info of an ongoing "multiplex iter".
        """
        return self._multiplex_info_file(task_id).read_json()

    def multiplex_done(self, task_id: str) -> bool:
        """Return whether the "multiplex iter" identified by ``task_id`` is finished."""
        ss = self.multiplex_stat(task_id)
        return ss["next"] == ss["total"]


class BiglistFileReader(FileReader):
    def __init__(self, path: PathType, loader: Callable[[Upath], Any]):
        """
        Parameters
        ----------
        path
            Path of a Parquet file.
        loader
            Usually this is ``Biglist.load_data_file``.
            If you customize this, please see the doc of ``FileReader.__init__``.
        """
        self._data: list = None
        super().__init__(path, loader)

    def load(self) -> None:
        if self._data is None:
            self._data = self.loader(self.path)

    def data(self) -> list:
        """Return the data loaded from the file."""
        self.load()
        return self._data

    def __len__(self) -> int:
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
