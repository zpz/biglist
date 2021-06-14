from __future__ import annotations
# Will no longer be needed at Python 3.10.

import gc
import logging
import multiprocessing
import os
import os.path
import queue
import string
import tempfile
import threading
from collections.abc import Sequence, Iterable
from concurrent.futures import ThreadPoolExecutor, Future
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator, Union, List, Dict, Optional, Tuple, Type, Callable
from uuid import uuid4

from upathlib import LocalUpath, Upath  # type: ignore
from ._serializer import (
    Serializer, PickleSerializer, CompressedPickleSerializer,
    JsonSerializer, OrjsonSerializer, CompressedOrjsonSerializer,
)


logger = logging.getLogger(__name__)


@contextmanager
def no_gc():
    isgc = gc.isenabled()
    if isgc:
        gc.disable()
    yield
    if isgc:
        gc.enable()


class Dumper:
    def __init__(self, max_workers: int = 3):
        assert 0 < max_workers < 10
        self._max_workers = max_workers
        self._executor: ThreadPoolExecutor = None  # type: ignore
        self._sem: threading.Semaphore = None  # type: ignore
        # Do not instantiate these now.
        # They would cause trouble when `Biglist`
        # file-views are sent to other processes.

        self._task_file_data: Dict[Future, Tuple] = {}

    def _callback(self, t):
        self._sem.release()
        del self._task_file_data[t]
        if t.exception():
            raise t.exception()

    def dump_file(self, file_dumper, data_file: Upath, data: List):
        if self._executor is None:
            self._executor = ThreadPoolExecutor(self._max_workers)
            self._sem = threading.Semaphore(self._max_workers)
        self._sem.acquire()
        task = self._executor.submit(file_dumper, data_file, data)
        self._task_file_data[task] = (data_file.name, data)
        task.add_done_callback(self._callback)
        # If task is already finished when this callback is being added,
        # then it is called immediately.

    def get_file_data(self, data_file: Upath):
        file_name = data_file.name
        for name, data in self._task_file_data.values():
            if name == file_name:
                return data
        return None

    def wait(self):
        for t in list(self._task_file_data.keys()):
            _ = t.result()
        assert not self._task_file_data

    def cancel(self):
        self.wait()


class FileIterStat:
    def __init__(self, data_info=None, iter_info=None):
        self._data_info = data_info
        self._iter_info = iter_info

    def __repr__(self) -> str:
        return "{}({}, {}, {})".format(
            self.__class__.__name__,
            self.n_files_total,
            self.n_files_claimed,
            self.n_files_finished
        )

    def __str__(self) -> str:
        return self.__repr__()

    @property
    def time_started(self) -> Optional[datetime]:
        if self._iter_info is None:
            return None
        return datetime.fromisoformat(
            self._iter_info[0]['time_started'])

    @property
    def time_finished(self) -> Optional[datetime]:
        if self._iter_info is None:
            return None
        if len(self._iter_info) < len(self._data_info):
            return None
        tt = None
        for z in self._iter_info:
            t = z.get('time_finished')
            if t is None:
                return None
            t = datetime.fromisoformat(t)
            if tt is None or t > tt:
                tt = t
        return tt

    @property
    def finished(self) -> bool:
        t = self.time_finished
        return t is not None

    @property
    def n_files_total(self) -> int:
        return len(self._data_info)

    @property
    def n_files_claimed(self) -> int:
        if self._iter_info is None:
            return 0
        return len(self._iter_info)

    @property
    def n_files_finished(self) -> int:
        if self._iter_info is None:
            return 0
        n = 0
        for z in self._iter_info:
            if 'time_finished' in z:
                n += 1
        return n

    @property
    def status(self) -> str:
        if self.started is None:
            return 'NOT STARTED'
        if self.finished:
            return 'FINISHED'
        return 'IN PROGRESS'


class Biglist(Sequence):
    '''
    Data access is optimized for iteration, whereas random access
    (via index or slice) is less efficient, and assumed to be rare.
    '''
    registered_storage_formats = {
        'pickle': PickleSerializer,
        'orjson': OrjsonSerializer,
    }

    DEFAULT_STORAGE_FORMAT = 'orjson'

    @classmethod
    def register_storage_format(cls,
                                name: str,
                                serializer: Type[Serializer],
                                overwrite: bool = False,
                                ):
        good = string.ascii_letters + string.digits + '-_'
        assert all(n in good for n in name)
        if name.replace('-', '_') in cls.registered_storage_formats:
            if not overwrite:
                raise ValueError(f"serializer '{name}' is already registered")
        name = name.replace('-', '_')
        cls.registered_storage_formats[name] = serializer

    @classmethod
    def get_temp_path(cls) -> Upath:
        '''Subclass needs to customize this if it prefers to use
        a remote blobstore for temp Biglist.
        '''
        path = tempfile.mkdtemp(dir=os.environ.get('TMPDIR', '/tmp'))
        # This directory is already created by now.
        path = LocalUpath(path)
        return path

    @classmethod
    def dump_data_file(cls, path: Upath, data: list):
        serializer = cls.registered_storage_formats[path.suffix.lstrip('.')]
        with no_gc():
            data = [cls.pre_serialize(v) for v in data]
            path.write_bytes(serializer.serialize(data))

    @classmethod
    def load_data_file(cls, path: Upath):
        deserializer = cls.registered_storage_formats[path.suffix.lstrip('.')]
        with no_gc():
            z = deserializer.deserialize(path.read_bytes())
            return [cls.post_deserialize(v) for v in z]

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
        # use the vlaue already set in mode (a).
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
                path = LocalUpath(path.absolute())
            if keep_files is None:
                keep_files = True
        if path.is_dir() and list(path.iterdir()):
            raise Exception(f'directory "{path}" already exists')

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
        else:
            if storage_format.replace('-', '_') not in cls.registered_storage_formats:
                raise ValueError(
                    f"invalid value of `storage_format`: '{storage_format}'")
        obj.info['storage_format'] = storage_format.replace('-', '_')
        obj._info_file.write_json(obj.info, overwrite=False)

        return obj

    def __init__(self,
                 path: Union[str, Path, Upath],
                 *,
                 multi_writers: bool = True):
        '''
        `multi_writers`: if `true`, this Biglist is possibly being written to
        (i.e. appended to) by other workers besides the current one.
        The default value, `True`, is a conservative setting---it does no harm
        other than slowing down random access. Setting it to `False` only when
        you are sure the current object is the only one that may be writing
        to the Biglist at this time.

        TODO: find a better name for `multi_writers`.
        '''
        if isinstance(path, str):
            path = Path(path)
        if isinstance(path, Path):
            path = LocalUpath(path.absolute())
        # Else it's something that already satisfies the
        # `Upath` protocol.
        self.path = path

        self._multi_writers = multi_writers
        self._data_files: Optional[list] = None

        self._read_buffer: Optional[list] = None
        self._read_buffer_file: Optional[int] = None
        self._read_buffer_item_range: Optional[Tuple] = None
        # `self._read_buffer` contains the content of the file
        # indicated by `self._read_buffer_file`.

        self._append_buffer: List = []

        self.keep_files = True
        self._file_dumper = Dumper()

        if self._info_file.is_file():
            # Instantiate a Biglist object pointing to
            # existing data.
            info = self._info_file.read_json()

            if 'file_lengths' in info:
                # Migrate to new storage plan.
                file_lengths = info.pop('file_lengths')
                suffix = self.storage_format
                files_info = [
                    (f'{k}.{suffix}', n) for
                    k, n in enumerate(file_lengths)
                ]
                self._data_info_file.write_json(
                    files_info,
                    overwrite=False,
                )
                self._info_file.write_json(info, overwrite=True)
        else:
            info = {}
        self.info = info

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
    def _fileiter_info_file(self) -> Upath:
        return self.path / 'fileiter_info.json'

    @property
    def _info_file(self) -> Upath:
        return self.path / 'info.json'

    @property
    def storage_format(self) -> str:
        return self.info['storage_format']

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

        datafiles = self.get_data_files()
        length = sum(l for _, l in datafiles)
        idx = range(length)[idx]

        if idx >= length:
            self._read_buffer_file = None
            return self._append_buffer[idx - length]  # type: ignore

        if self._read_buffer_file is not None:
            n1, n2 = self._read_buffer_item_range  # type: ignore
            if n1 <= idx < n2:
                return self._read_buffer[idx - n1]  # type: ignore

        n = 0
        for name, l in datafiles:
            if n <= idx < n + l:
                self._read_buffer_item_range = (n, n + l)
                file = self._data_dir / name
                data = self._file_dumper.get_file_data(file)
                if data is None:
                    data = self.load_data_file(file)
                self._read_buffer_file = file
                self._read_buffer = data
                return data[idx - n]
            n += l

        raise Exception('should never reach here!')

    def __iter__(self):
        self.flush()
        datafiles = self.get_data_files()
        ndatafiles = len(datafiles)

        if ndatafiles == 1:
            yield from self.load_data_file(self._data_dir / datafiles[0][0])
        elif ndatafiles > 1:
            max_workers = min(3, ndatafiles)
            tasks = queue.Queue(max_workers)
            executor = ThreadPoolExecutor(max_workers)
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

        if self._append_buffer:
            yield from self._append_buffer

    def __len__(self) -> int:
        z = self.get_data_files()
        return sum(k for _, k in z)

    def append(self, x) -> None:
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
            self._flush()

    def _append_data_files_info(self, filename: str, length: int):
        with self._data_info_file.lock():
            z = self.get_data_files()
            z.append((filename, length))
            self._data_info_file.write_json(z, overwrite=True)

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

    def file_iter_stat(self) -> FileIterStat:
        try:
            iter_info = self._fileiter_info_file.read_json()
        except FileNotFoundError:
            return FileIterStat()
        datafiles = self.get_data_files()
        return FileIterStat(datafiles, iter_info)

    def file_view(self, file: Union[Upath, int]) -> FileView:
        if isinstance(file, int):
            datafiles = self.get_data_files()
            file = self._data_dir / datafiles[file][0]
        return FileView(file, self.load_data_file)

    def file_views(self) -> List[FileView]:
        # This is intended to facilitate parallel processing,
        # e.g. send views on diff files to diff `multiprocessing.Process`es.
        # However, `iter_files` may be a better way to do that.
        datafiles = self.get_data_files()
        return [
            self.file_view(self._data_dir / f)
            for f, l in datafiles
        ]

    def _flush(self):
        '''
        Persist the content of the in-memory buffer to a file,
        reset the buffer, and update relevant book-keeping variables.

        This method is called any time the size of the in-memory buffer 
        reaches `self.batch_size`. This happens w/o the user's intervention.
        '''
        if not self._append_buffer:
            return

        buffer_len = len(self._append_buffer)

        while True:
            data_file = self._data_dir / \
                f'{uuid4()}.{self.storage_format}'
            if not data_file.exists():
                break

        self._file_dumper.dump_file(
            self.dump_data_file,
            data_file,
            self._append_buffer,
        )
        # This call will return quickly if the dumper has queue
        # capacity for the file. The file meta data below
        # will be updated as if the saving has completed, although
        # it hasn't (it is only queued). This allows the waiting-to-be-saved
        # data to be accessed property.

        self._append_buffer = []
        self._append_data_files_info(data_file.name, buffer_len)

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
        self._flush()
        self._file_dumper.wait()

    def get_data_files(self) -> list:
        if not self._multi_writers and self._data_files is not None:
            return self._data_files

        if self._data_info_file.exists():
            self._data_files = self._data_info_file.read_json()
        else:
            self._data_files = []
        return self._data_files  # type: ignore

    def iter_files(self, reader_id: str = None) -> Iterator[list]:
        if not reader_id and isinstance(self.path, LocalUpath):
            reader_id = multiprocessing.current_process().name
        datafiles = self.get_data_files()
        file_idx = None
        file_name = None
        while True:
            with self._fileiter_info_file.lock() as ff:
                iter_info = ff.read_json()
                if file_idx is not None:
                    z = iter_info[file_idx]
                    assert z['file_name'] == file_name
                    assert z['reader_id'] == reader_id
                    iter_info[file_idx]['time_finished'] = str(
                        datetime.utcnow())
                if len(iter_info) >= len(datafiles):
                    ff.write_json(iter_info, overwrite=True)
                    break
                file_idx = len(iter_info)
                file_name = datafiles[file_idx][0]
                iter_info.append({
                    'file_name': file_name,
                    'reader_id': reader_id,
                    'time_started': str(datetime.utcnow()),
                })
                ff.write_json(iter_info, overwrite=True)
            fv = self.file_view(self._data_dir / file_name)
            logger.info('yielding data of file "%s"', file_name)
            yield fv.data

    def reset_file_iter(self) -> None:
        '''
        One worker, such as a "coordinator", calls this method once.
        After that, one or more workers independently call `iter_files`
        to iterate over the Biglist. The content of the Biglist is
        split between the workers.
        '''
        self._fileiter_info_file.write_json([], overwrite=True)

    def view(self) -> ListView:
        # During the use of this view, the underlying Biglist should not change.
        # Multiple views may be used to view diff parts
        # of the Biglist; they open and read files independent of
        # other views.
        self.flush()
        return ListView(self.__class__(self.path))


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


Biglist.register_storage_format('json', JsonSerializer)
Biglist.register_storage_format('pickle-z', CompressedPickleSerializer)
Biglist.register_storage_format('orjson-z', CompressedOrjsonSerializer)
