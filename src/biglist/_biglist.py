from __future__ import annotations
# Will no longer be needed at Python 3.10.

import bisect
import concurrent.futures
import itertools
import json
import logging
import multiprocessing
import os
import os.path
import queue
import string
import tempfile
import threading
import uuid
from collections.abc import Sequence, Iterable
from concurrent.futures import ThreadPoolExecutor, Future
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator, Union, List, Dict, Optional, Tuple, Type, Callable
from uuid import uuid4

from upathlib import LocalUpath, Upath  # type: ignore
from upathlib.serializer import (
    ByteSerializer, _loads,
    ZJsonSerializer, ZstdJsonSerializer,
    PickleSerializer, ZPickleSerializer, ZstdPickleSerializer,
    OrjsonSerializer, ZOrjsonSerializer, ZstdOrjsonSerializer,
)


logger = logging.getLogger(__name__)


class Dumper:
    def __init__(self, max_workers: int = 8):
        assert 0 < max_workers < 20
        self._max_workers = max_workers
        self._executor: ThreadPoolExecutor = None  # type: ignore
        self._sem: threading.Semaphore = None  # type: ignore
        # Do not instantiate these now.
        # They would cause trouble when `Biglist`
        # file-views are sent to other processes.

        self._task_file_data: Dict[Future, tuple] = {}

    def __del__(self):
        self.cancel()

    def _callback(self, t):
        self._sem.release()
        del self._task_file_data[t]
        if t.exception():
            raise t.exception()

    def dump_file(self,
                  file_dumper: Callable[[Upath, list], None],
                  data_file: Upath,
                  data: list):
        if self._executor is None:
            self._executor = ThreadPoolExecutor(self._max_workers)
            self._sem = threading.Semaphore(self._max_workers)
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
        if self._executor is not None:
            self._executor.shutdown()
            self._executor = None
        self._task_file_data = {}


class Biglist(Sequence):
    '''
    Data access is optimized for iteration, whereas random access
    (via index or slice) is less efficient, and assumed to be rare.
    '''
    registered_storage_formats = {}

    DEFAULT_STORAGE_FORMAT = 'pickle'

    @classmethod
    def register_storage_format(cls,
                                name: str,
                                serializer: Type[ByteSerializer],
                                overwrite: bool = False,
                                ):
        good = string.ascii_letters + string.digits + '-_'
        assert all(n in good for n in name)
        if name.replace('_', '-') in cls.registered_storage_formats:
            if not overwrite:
                raise ValueError(f"serializer '{name}' is already registered")
        name = name.replace('_', '-')
        cls.registered_storage_formats[name] = serializer

    @classmethod
    def get_temp_path(cls) -> Upath:
        '''Subclass needs to customize this if it prefers to use
        a remote blobstore for temp Biglist.
        '''
        path = LocalUpath(os.path.abspath(tempfile.gettempdir()),
                          str(uuid.uuid4()))  # type: ignore
        return path  # type: ignore

    @classmethod
    def dump_data_file(cls, path: Upath, data: list):
        serializer = cls.registered_storage_formats[path.suffix.lstrip('.').replace('_', '-')]
        data = [cls.pre_serialize(v) for v in data]
        path.write_bytes(serializer.serialize(data))

    @classmethod
    def load_data_file(cls, path: Upath):
        deserializer = cls.registered_storage_formats[path.suffix.lstrip('.').replace('_', '-')]
        data = path.read_bytes()
        z = deserializer.deserialize(data)
        return [cls.post_deserialize(v) for v in z]

    @classmethod
    @contextmanager
    def _lockfile(cls, file: Upath):
        # Although by default this uses `file.lock()`, it doesn't have to be.
        # All this method needs is to guarantee that the code block identified
        # by `file` (essentially the name) is NOT excecuted concurrently
        # by two "workers". It by no means has to be "locking that file".
        with file.lock():
            yield

    @classmethod
    def pre_serialize(cls, x):
        '''When the data element is an instance of a custom type,
        it is preferred to convert it to a native type, such as dict,
        before persisting it to files, especially for long-term storage.

        When using Biglist to store data of a custom class, it's recommended
        to create a subclass of Biglist for the particular class, and implement
        `pre_serialize` and `post_deserialize`. A good pattern is to define
        instance method `to_dict` and class method `from_dict` on the
        custom class, and call them in `pre_serialize` and `post_deserialize`.
        '''
        return x

    @classmethod
    def post_deserialize(cls, x):
        '''The reverse of `pre_serialize`.'''
        return x

    @classmethod
    def new(cls,
            path: Union[str, Path, Upath] = None,
            *,
            batch_size: int = None,
            keep_files: bool = None,
            storage_format: str = None,
            **kwargs,
            ):
        # A Biglist object constrution is in either of the two modes
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
        # Some settings may be applicale to an existing Biglist object,
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

        obj = cls(path, **kwargs)  # type: ignore

        obj.keep_files = keep_files

        if not batch_size:
            batch_size = 10000
            # There's no good default value for this,
            # because we don't know the typical size of
            # the data elements. User is recommended
            # to provide the argument `batch_size`.
        else:
            assert batch_size > 0
        obj.info['batch_size'] = batch_size

        if storage_format is None:
            storage_format = cls.DEFAULT_STORAGE_FORMAT
        if storage_format.replace('_', '-') not in cls.registered_storage_formats:
            raise ValueError(
                f"invalid value of `storage_format`: '{storage_format}'")
        obj.info['storage_format'] = storage_format.replace('_', '-')
        obj.info['storage_version'] = 0
        # version 0 designator introduced on 2022/3/8
        obj._info_file.write_json(obj.info, overwrite=False)

        return obj

    def __init__(self, path: Union[str, Path, Upath]):
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

        self.keep_files = True
        self._file_dumper = Dumper()

        try:
            # Instantiate a Biglist object pointing to
            # existing data.
            self.info = self._info_file.read_json()
        except FileNotFoundError:
            self.info = {}

    @property
    def batch_size(self) -> int:
        return self.info['batch_size']

    @property
    def _data_dir(self) -> Upath:
        return self.path / 'store'

    @property
    def _data_info_file(self) -> Upath:
        return self.path / 'datafiles_info.json'

    @property
    def _info_file(self) -> Upath:
        return self.path / 'info.json'

    @property
    def storage_format(self) -> str:
        return self.info['storage_format'].replace('_', '-')

    @property
    def datafile_ext(self) -> str:
        return self.storage_format.replace('-', '_')

    @property
    def storage_version(self) -> int:
        return self.info.get('storage_version', 0)

    def __bool__(self) -> bool:
        return len(self) > 0

    def __del__(self) -> None:
        if self.keep_files:
            self.flush()
        else:
            self.destroy()

    def __getitem__(self, idx: int):  # type: ignore
        '''
        Element access by single index; negative index works as expected.

        This is called in these cases:

            - Access a single item of a Biglist object.
            - Access a single item of a Biglist.view()
            - Access a single item of a slice on a Biglist.view()
            - Iterate a slice on a Biglist.view()
        '''
        if not isinstance(idx, int):
            raise TypeError(
                f'{self.__class__.__name__} indices must be integers, not {type(idx).__name__}')

        if idx >= 0 and self._read_buffer_file is not None:
            n1, n2 = self._read_buffer_item_range  # type: ignore
            if n1 <= idx < n2:
                return self._read_buffer[idx - n1]  # type: ignore

        if idx < 0 and (-idx) <= len(self._append_buffer):
            return self._append_buffer[idx]

        datafiles = self.get_data_files(lazy=True)
        length = self._data_files_length_
        idx = range(length + len(self._append_buffer))[idx]

        if idx >= length:
            self._read_buffer_file = None
            return self._append_buffer[idx - length]  # type: ignore

        ifile0 = 0
        ifile1 = len(datafiles)
        if self._read_buffer_file is not None:
            n1, n2 = self._read_buffer_item_range  # type: ignore
            if idx < n1:
                ifile1 = self._read_buffer_file_idx_  # pylint: disable=access-member-before-definition
            elif idx < n2:
                return self._read_buffer[idx - n1]  # type: ignore
            else:
                ifile0 = self._read_buffer_file_idx_ + 1  # pylint: disable=access-member-before-definition

        # Now find the data file that contains the target item.

        ifile = bisect.bisect_right(self._data_files_cumlength_, idx, lo=ifile0, hi=ifile1)
        # `ifile`: index of data file that contains the target element.
        # `n`: total length before `ifile`.
        if ifile == 0:
            n = 0
        else:
            n = self._data_files_cumlength_[ifile - 1]
        self._read_buffer_item_range = (n, self._data_files_cumlength_[ifile])
        file = self._data_dir / datafiles[ifile][0]
        data = self._file_dumper.get_file_data(file)
        if data is None:
            data = self.load_data_file(file)
        self._read_buffer_file = file
        self._read_buffer_file_idx_ = ifile
        self._read_buffer = data
        return data[idx - n]

    def __iter__(self):
        # Assuming the biglist will not change (not being appended to)
        # during iteration.
        self.flush()
        datafiles = self.get_data_files()
        ndatafiles = len(datafiles)

        if ndatafiles == 1:
            yield from self.load_data_file(self._data_dir / datafiles[0][0])
        elif ndatafiles > 1:
            max_workers = min(3, ndatafiles)
            tasks = queue.Queue(max_workers)
            with ThreadPoolExecutor(max_workers) as executor:
                for i in range(max_workers):
                    t = executor.submit(
                        self.load_data_file,
                        self._data_dir / datafiles[i][0]
                    )
                    tasks.put(t)
                nfiles_queued = max_workers

                for _ in range(ndatafiles):
                    t = tasks.get()
                    data = t.result()

                    if nfiles_queued < ndatafiles:
                        t = executor.submit(
                            self.load_data_file,
                            self._data_dir / datafiles[nfiles_queued][0]
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
        # the biglist, then the biglist is not being changed.
        self.get_data_files(lazy=True)
        return self._data_files_length_ + len(self._append_buffer)

    def _append(self, x, concurrent: bool) -> None:
        '''
        Append a single element to the in-memory buffer.
        Once the buffer size reaches `self.batch_size`, the buffer's content
        will be written to a file, and the buffer will re-start empty.

        In other words, whenever `self._append_buffer` is non-empty,
        its content is not written to disk yet.
        However, at any time, the content of this buffer is included in
        `self.__len__` and in element accesses, including iterations.
        '''
        self._append_buffer.append(x)
        if len(self._append_buffer) >= self.batch_size:
            self._flush(concurrent=concurrent)

    def append(self, x) -> None:
        self._append(x, concurrent=False)

    def destroy(self) -> None:
        '''
        Clears all the files and releases all in-memory data held by this object,
        so that the object is as if upon `__init__` with an empty directory pointed to
        by `self.path`.

        After this method is called, this object is no longer usable.
        '''
        self._file_dumper.cancel()
        self._read_buffer = None
        self._read_buffer_file = None
        self._read_buffer_item_range = None
        self._append_buffer = []
        self.path.rmrf()

    def extend(self, x: Iterable) -> None:
        for v in x:
            self.append(v)

    def file_view(self, file: Union[Upath, int]) -> FileView:
        if isinstance(file, int):
            datafiles = self.get_data_files(lazy=True)
            file = self._data_dir / datafiles[file][0]
        return FileView(file, self.load_data_file)  # type: ignore

    def file_views(self) -> List[FileView]:
        # This is intended to facilitate parallel processing,
        # e.g. send views on diff files to diff `multiprocessing.Process`es.
        # However, `iter_files` may be a better way to do that.
        datafiles = self.get_data_files()
        return [
            self.file_view(self._data_dir / f)
            for f, _ in datafiles
        ]

    def _flush(self, *, wait: bool = False, concurrent: bool = True):
        '''
        Persist the content of the in-memory buffer to a file,
        reset the buffer, and update relevant book-keeping variables.

        This method is called any time the size of the in-memory buffer
        reaches `self.batch_size`. This happens w/o the user's intervention.
        '''
        if not self._append_buffer:
            return

        buffer = self._append_buffer
        buffer_len = len(buffer)
        self._append_buffer = []

        def _append_data_file():
            data_files = self.get_data_files(lazy=not concurrent)

            while True:
                data_file = f"{uuid4()}.{self.datafile_ext}"
                # TODO: this could be inefficient if there are many files.
                if not any((data_file == v[0] for v in data_files)):
                    break

            data_files.append((data_file, buffer_len))
            self._data_info_file.write_json(data_files, overwrite=True)
            self._data_files = data_files
            return data_file

        if concurrent:
            with self._lockfile(self._data_info_file.with_suffix('.json.lock')):
                filename = _append_data_file()
        else:
            filename = _append_data_file()

        self._data_files_length_ = self._data_files_length_ + buffer_len
        self._data_files_cumlength_.append(self._data_files_length_)

        data_file = self._data_dir / filename
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
            # what if dump fails later? the index file will be updated
            # already assuming everything will be fine.

    def flush(self):
        '''
        When the user is done adding elements to the list, the buffer size
        may not happen to be `self.batch_size`, hence this method is not called
        automatically,
        and the last chunk of elements are not persisted in files.
        This is when the *user* should call this method.
        (If user does not call, it is called when this object is going out of scope,
        as appropriate.)

        In summary, call this method once the user is done with adding elements
        to the list *in this session*, meaning in this run of the program.
        '''
        self._flush(wait=True)

    def get_data_files(self, lazy: bool = False) -> list:
        if lazy and self._data_files is not None:
            return self._data_files

        try:
            self._data_files = self._data_info_file.read_json()
            self._data_files_cumlength_ = list(itertools.accumulate(
                v[1] for v in self._data_files
            ))
            self._data_files_length_ = self._data_files_cumlength_[-1]
        except FileNotFoundError:
            self._data_files = []
            self._data_files_length_ = 0
            self._data_files_cumlength_ = []
        return self._data_files  # type: ignore

    def view(self) -> ListView:
        # During the use of this view, the underlying Biglist should not change.
        # Multiple views may be used to view diff parts
        # of the Biglist; they open and read files independent of
        # other views.
        self.flush()
        return ListView(self.__class__(self.path))

    def _concurrent_iter_info_file(self, task_id: str) -> Upath:
        return self.path / 'concurrent_iter' / task_id / 'iter_info.json'

    def concurrent_append(self, x) -> None:
        self._append(x, concurrent=True)

    def concurrent_extend(self, x: Iterable) -> None:
        for v in x:
            self.concurrent_append(v)

    def new_concurrent_iter(self) -> str:
        '''
        One worker, such as a "coordinator", calls this method once.
        After that, one or more workers independently call `concurrent_iter`
        to iterate over the Biglist, providing the iter-ID returned by
        this method. The content of the Biglist is
        split between the workers because each data file will be obtained
        by exactly one worker.
        '''
        task_id = datetime.utcnow().isoformat()
        self._concurrent_iter_info_file(task_id).write_json(
            {'n_files_claimed': 0}, overwrite=True)
        return task_id

    def concurrent_iter(self, task_id: str, *, reader_id: str = None) -> Iterator:
        if not reader_id and isinstance(self.path, LocalUpath):
            reader_id = multiprocessing.current_process().name
        datafiles = self.get_data_files()
        while True:
            ff = self._concurrent_iter_info_file(task_id)
            with self._lockfile(ff.with_suffix('.json.lock')):
                iter_info = ff.read_json()
                n_files_claimed = iter_info['n_files_claimed']
                if n_files_claimed >= len(datafiles):
                    # No more date files to process.
                    break

                iter_info['n_files_claimed'] = n_files_claimed + 1
                ff.write_json(iter_info, overwrite=True)
                file_name = datafiles[n_files_claimed][0]
                fv = self.file_view(self._data_dir / file_name)
                # This does not actually read the file.
                # TODO: make this read the file here?

            logger.debug('yielding data of file "%s"', file_name)
            yield from fv.data  # this is the data list contained in the data file

    def concurrent_iter_done(self, task_id: str, lazy: bool = True) -> Optional[bool]:
        try:
            iter_info = self._concurrent_iter_info_file(task_id).read_json()
        except FileNotFoundError:
            return None
        datafiles = self.get_data_files(lazy=lazy)
        # Usually, this method is called by a "controller" node to
        # check the status of iteration of the biglist by multiple worker nodes.
        # In the meantime, the biglist remain unchanged, hence
        # `get_data_files` can use the lazy mode.
        return iter_info['n_files_claimed'] >= len(datafiles)


class FileView(Sequence):
    def __init__(self, file: Upath, loader: Callable):
        # TODO: make `Upath` safe to pass across processes.
        self._file = file
        self._loader = loader
        self._data = None

    @property
    def data(self):
        if self._data is None:
            self._data = self._loader(self._file)
        return self._data

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx: Union[int, slice]):
        return self.data[idx]

    def __iter__(self):
        return iter(self.data)


class ListView(Sequence):
    def __init__(self, list_: Sequence, range_: range = None):
        '''
        This provides a "window" into the sequence `list_`,
        which is often a `Biglist` or another `ListView`.

        An object of `ListView` is created by `Biglist.view()` or
        by slicing a `ListView`.
        User should not attempt to create an object of this class directly.

        The main purpose of this class is to provide slicing over `Biglist`.

        During the use of this object, it is assumed that the underlying
        `list_` is not changing. Otherwise the results may be incorrect.
        '''
        self._list = list_
        self._range = range_

    def __len__(self) -> int:
        if self._range is None:
            return len(self._list)
        return len(self._range)

    def __bool__(self) -> bool:
        return len(self) > 0

    def __getitem__(self, idx: Union[int, slice]):
        '''
        Element access by a single index or by slice.
        Negative index and standard slice syntax both work as expected.

        Sliced access returns a new `ListView` object.
        '''
        if isinstance(idx, int):
            if self._range is None:
                return self._list[idx]
            return self._list[self._range[idx]]

        if isinstance(idx, slice):
            if self._range is None:
                range_ = range(len(self._list))[idx]
            else:
                range_ = self._range[idx]
            return self.__class__(self._list, range_)

        raise TypeError(
            f'{self.__class__.__name__} indices must be integers or slices, not {type(idx).__name__}')

    def __iter__(self):
        if self._range is None:
            yield from self._list
        else:
            for i in self._range:
                yield self._list[i]

    @property
    def raw(self):
        return self._list


class JsonByteSerializer(ByteSerializer):
    @classmethod
    def serialize(cls, x, **kwargs):
        return json.dumps(x, **kwargs).encode()

    @classmethod
    def deserialize(cls, y, **kwargs):
        return _loads(json.loads, y.decode(), **kwargs)


Biglist.register_storage_format('json', JsonByteSerializer)
Biglist.register_storage_format('json-z', ZJsonSerializer)
Biglist.register_storage_format('json-zstd', ZstdJsonSerializer)
Biglist.register_storage_format('pickle', PickleSerializer)
Biglist.register_storage_format('pickle-z', ZPickleSerializer)
Biglist.register_storage_format('pickle-zstd', ZstdPickleSerializer)
Biglist.register_storage_format('orjson', OrjsonSerializer)
Biglist.register_storage_format('orjson-z', ZOrjsonSerializer)
Biglist.register_storage_format('orjson-zstd', ZstdOrjsonSerializer)
