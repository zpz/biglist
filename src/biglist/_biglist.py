# from __future__ import annotations

# Will no longer be needed at Python 3.10.

import concurrent.futures
import itertools
import json
import logging
import multiprocessing
import os
import os.path
import string
import tempfile
import time
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, Future
from datetime import datetime
from pathlib import Path
from typing import (
    Iterable,
    Iterator,
    Union,
    List,
    Dict,
    Type,
    Callable,
    TypeVar,
)
from uuid import uuid4

from upathlib import LocalUpath, Upath  # type: ignore
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

from ._base import BiglistBase


logger = logging.getLogger(__name__)

T = TypeVar("T")


class Dumper:
    def __init__(self, executor: ThreadPoolExecutor):
        self._executor: executor = executor
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
        if self._sem is None:
            self._sem = threading.Semaphore(min(5, self._executor._max_workers))
        self._sem.acquire()  # Wait here if the executor is busy at capacity.
        task = self._executor.submit(file_dumper, data_file, data)
        self._task_file_data[task] = (data_file.name, data)
        # It's useful to keep the data here, as it will be needed
        # by `get_file_data`.
        task.add_done_callback(self._callback)
        # If task is already finished when this callback is being added,
        # then it is called immediately.

    def get_file_data(self, data_file: Upath):
        file_name = data_file.name
        for name, data in self._task_file_data.values():
            # `_task_file_data` is not long, so this is OK.
            if name == file_name:
                return data
        return None

    def wait(self):
        concurrent.futures.wait(list(self._task_file_data.keys()))

    def cancel(self):
        self._task_file_data = {}


class Biglist(BiglistBase[T]):
    """
    Data access is optimized for iteration, whereas random access
    (via index or slice) is less efficient, and assumed to be rare.
    """

    registered_storage_formats = {}

    DEFAULT_STORAGE_FORMAT = "pickle"

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
        serializer = cls.registered_storage_formats[
            path.suffix.lstrip(".").replace("_", "-")
        ]
        data = [cls.pre_serialize(v) for v in data]
        path.write_bytes(serializer.serialize(data))

    @classmethod
    def load_data_file(cls, path: Upath) -> List[T]:
        deserializer = cls.registered_storage_formats[
            path.suffix.lstrip(".").replace("_", "-")
        ]
        data = path.read_bytes()
        z = deserializer.deserialize(data)
        return [cls.post_deserialize(v) for v in z]

    @classmethod
    def pre_serialize(cls, x: T):
        """When the data element is an instance of a custom type,
        it is preferred to convert it to a native type, such as dict,
        before persisting it to files, especially for long-term storage.

        When using Biglist to store data of a custom class, it's recommended
        to create a subclass of Biglist for the particular class, and implement
        `pre_serialize` and `post_deserialize`. A good pattern is to define
        instance method `to_dict` and class method `from_dict` on the
        custom class, and call them in `pre_serialize` and `post_deserialize`.
        """
        return x

    @classmethod
    def post_deserialize(cls, x) -> T:
        """The reverse of `pre_serialize`."""
        return x

    @classmethod
    def get_temp_path(cls) -> Upath:
        """Subclass needs to customize this if it prefers to use
        a remote blobstore for temp Biglist.
        """
        path = LocalUpath(
            os.path.abspath(tempfile.gettempdir()), str(uuid.uuid4())
        )  # type: ignore
        return path  # type: ignore

    @classmethod
    def new(
        cls,
        path: Union[str, Path, Upath] = None,
        *,
        batch_size: int = None,
        keep_files: bool = None,
        storage_format: str = None,
        **kwargs,
    ):
        # A Biglist object construction is in either of the two modes
        # below:
        #    a) create a new Biglist to store new data.
        #    b) create a Biglist object pointing to storage of
        #       existing data, which was created by a previous call to Biglist.new.
        #
        # Some settings are applicable only in mode (a), b/c in
        # mode (b) they can't be changed and, if needed, should only
        # use the value already set in mode (a).
        # Such settings should happen in this classmethod `new`
        # and should not be parameters to the object initiator function `__init__`.
        #
        # These settings typically should be taken care of after creating
        # the object with `__init__`.
        #
        # `__init__` should be defined in such a way that it works for
        # both a barebone object that is created in this `new`, and a
        # fleshed out object that already has data.
        #
        # Some settings may be applicable to an existing Biglist object,
        # i.e. they control ways to use the object, and are not an intrinsic
        # property of the object. Hence they can be set to diff values while
        # using an existing Biglist object. Such settings should be
        # parameters to `__init__` and not to `new`. These parameters
        # may be used when calling `new`, in which case they will be passed
        # on to `__init__`.

        if not path:
            path = cls.get_temp_path()
            if keep_files is None:
                keep_files = False
        else:
            if isinstance(path, str):
                path = Path(path)
            if isinstance(path, Path):
                path = LocalUpath(str(path.absolute()))
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
            # There's no good default value for this,
            # because we don't know the typical size of
            # the data elements. User is recommended
            # to provide the argument `batch_size`.
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
        self.keep_files = True
        self._file_dumper = None

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

    def __del__(self) -> None:
        if self.keep_files:
            self.flush()
        else:
            self.destroy()

    def append(self, x: T) -> None:
        """
        Append a single element to the in-memory buffer.
        Once the buffer size reaches `self.batch_size`, the buffer's content
        will be written to a file, and the buffer will re-start empty.

        In other words, whenever `self._append_buffer` is non-empty,
        its content is not written to disk yet.
        However, at any time, the content of this buffer is included in
        `self.__len__` and in element accesses, including iterations.
        """
        self._append_buffer.append(x)
        if len(self._append_buffer) >= self.batch_size:
            self._flush()

    def destroy(self) -> None:
        """
        Clears all the files and releases all in-memory data held by this object,
        so that the object is as if upon `__init__` with an empty directory pointed to
        by `self.path`.

        After this method is called, this object is no longer usable.
        """
        if self._file_dumper is not None:
            self._file_dumper.cancel()
        self._read_buffer = None
        self._read_buffer_file = None
        self._read_buffer_item_range = None
        self._append_buffer = []
        self.path.rmrf()

    def extend(self, x: Iterable[T]) -> None:
        for v in x:
            self.append(v)

    def _flush(self, *, wait: bool = False):
        """
        Persist the content of the in-memory buffer to a file,
        reset the buffer, and update relevant book-keeping variables.

        This method is called any time the size of the in-memory buffer
        reaches `self.batch_size`. This happens w/o the user's intervention.
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
            self._file_dumper = Dumper(self._thread_pool)
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
        When the user is done adding elements to the list, the buffer size
        may not happen to be `self.batch_size`, hence this method is not called
        automatically,
        and the last chunk of elements are not persisted in files.
        This is when the *user* should call this method.
        (If user does not call, it is called when this object is going out of scope,
        as appropriate.)

        In summary, call this method once the user is done with adding elements
        to the list *in this session*, meaning in this run of the program.
        """
        self._flush(wait=True)

    def get_data_files(self) -> list:
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
                            for v in files1
                        )
                        files = sorted(files2)
                        if len(files) == nfiles:
                            break
                        time.sleep(0.2)

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

        return self._data_files  # type: ignore

    def get_data_file(self, datafiles, idx):
        # `datafiles` is the return of `get_datafiles`.
        return self._data_dir / datafiles[idx][0]

    def _multiplex_info_file(self, task_id: str) -> Upath:
        """
        `task_id`: returned by `new_multiplexer`.
        """
        return self.path / ".multiplexer" / task_id / "info.json"

    def new_multiplexer(self) -> str:
        """
        One worker, such as a "coordinator", calls this method once.
        After that, one or more workers independently call `multiplex_iter`
        to iterate over the Biglist. `multiplex_iter` takes the task-ID returned by
        this method. The content of the Biglist is
        split between the workers because each data item will be obtained
        by exactly one worker.

        During this iteration, the Biglist object should stay unchanged---no
        calls to `append` and `extend`.

        Difference between `concurrent_iter` and `multiplex_iter`: the former
        distributes files to workers, whereas the latter distributes individual
        data elements to workers.

        Use case of multiplexer: each data element represents considerable amounts
        of work--it is a "hyper-parameter" or the like; `multiplex_iter` facilitates
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
        `task_id`: returned by `new_multiplexer`.
        """
        if not worker_id:
            worker_id = "{} {}".format(
                multiprocessing.current_process().name,
                threading.current_thread().name,
            )
        while True:
            f = self._multiplex_info_file(task_id)
            with f.with_suffix(f.suffix + ".lock").lock():
                ss = self._multiplex_info_file(task_id).read_json()
                n = ss["next"]
                if n == ss["total"]:
                    return
                self._multiplex_info_file(task_id).write_json(
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
