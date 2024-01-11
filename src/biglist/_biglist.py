from __future__ import annotations

import atexit
import concurrent.futures
import copy
import functools
import io
import itertools
import json
import logging
import os
import string
import threading
import warnings
import weakref
from collections.abc import Iterable, Iterator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from typing import Any, Callable
from uuid import uuid4

import pyarrow
from typing_extensions import Self
from upathlib import Path, PathType, Upath, resolve_path, serializer

from ._base import BiglistBase
from ._parquet import ParquetFileReader, make_parquet_schema
from ._util import Element, FileReader, FileSeq, get_global_thread_pool, lock_to_use

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

    DEFAULT_STORAGE_FORMAT = 'pickle-zstd'

    @classmethod
    def register_storage_format(
        cls,
        name: str,
        serializer: type[serializer.ByteSerializer],
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
            ``storage_format='xyz'`` in calls to :meth:`new`.
            When reading the object back from persistence,
            make sure this registry is also in place so that the correct
            deserializer can be found.

        serializer
            A subclass of `upathlib.serializer.ByteSerializer <https://github.com/zpz/upathlib/blob/main/src/upathlib/serializer.py>`_.

            Although this class needs to provide the ``ByteSerializer`` API, it is possible to write data files in text mode.
            See :class:`JsonByteSerializer` for an example.
        """
        good = string.ascii_letters + string.digits + '-_'
        assert all(n in good for n in name)
        if name.replace('_', '-') in cls.registered_storage_formats:
            raise ValueError(f"serializer '{name}' is already registered")
        name = name.replace('_', '-')
        cls.registered_storage_formats[name] = serializer

    @classmethod
    def dump_data_file(cls, path: Upath, data: list[Element], **kwargs) -> None:
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

        Parameters
        ----------
        **kwargs
            Additional keyword arguments to the serializer.

        See Also
        --------
        :meth:`load_data_file`
        """
        serializer = cls.registered_storage_formats[
            path.suffix.lstrip('.').replace('_', '-')
        ]
        path.write_bytes(serializer.serialize(data, **kwargs))

    @classmethod
    def load_data_file(cls, path: Upath, **kwargs) -> list[Element]:
        """Load the data file given by ``path``.

        This function is used as the argument ``loader`` to :meth:`BiglistFileReader.__init__`.
        The value it returns is contained in :class:`FileReader` for subsequent use.

        Parameters
        ----------
        **kwargs
            Additional keyword arguments to the deserializer.

        See Also
        --------
        dump_data_file
        """
        deserializer = cls.registered_storage_formats[
            path.suffix.lstrip('.').replace('_', '-')
        ]
        data = path.read_bytes()
        return deserializer.deserialize(data, **kwargs)

    @classmethod
    def new(
        cls,
        path: PathType | None = None,
        *,
        batch_size: int | None = None,
        storage_format: str | None = None,
        serialize_kwargs: dict | None = None,
        deserialize_kwargs: dict | None = None,
        init_info: dict = None,
        **kwargs,
    ) -> Self:
        """
        Parameters
        ----------
        path
            Passed on to :meth:`BiglistBase.new`.
        batch_size
            Max number of data elements in each persisted data file.

            There's no good default value for this parameter, although one is
            provided (currently the default is 1000),
            because the code of :meth:`new` doesn't know
            the typical size of the data elements. User is recommended to
            specify the value of this parameter.

            In choosing a value for ``batch_size``, the most important
            consideration is the size of each data file, which is determined
            by the typical size of the data elements as well as ``batch_size``,
            which is the upper bound of the the number of elements in each file.

            There are several considerations about the data file sizes:

            - It should not be so small that the file reading/writing is a large
              overhead relative to actual processing of the data.
              This is especially important when ``path`` is cloud storage.

            - It should not be so large that it is "unwieldy", e.g. approaching 1GB.

            - When :meth:`__iter__`\\ating over a :class:`Biglist` object, there can be up to (by default) 4
              files-worth of data in memory at any time, where 4 is ``self._n_read_threads`` plus 1.

            - When :meth:`append`\\ing or :meth:`extend`\\ing to a :class:`Biglist` object at high speed,
              there can be up to (by default) 9 times ``batch_size`` data elements in memory at any time,
              where 9 is ``self._n_write_threads`` plus 1.
              See :meth:`_flush` and :class:`~biglist._biglist.Dumper`.

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
        serialize_kwargs
            Additional keyword arguments to the serialization function.
        deserialize_kwargs
            Additional keyword arguments to the deserialization function.

            ``serialize_kwargs`` and ``deserialize_kwargs`` are rarely needed.
            One use case is ``schema`` when storage format is "parquet".
            See :class:`~biglist._biglist.ParquetSerializer`.

            ``serialize_kwargs`` and ``deserialize_kwargs``, if not ``None``,
            will be saved in the "info.json" file, hence they must be JSON
            serializable, meaning they need to be the few simple native Python
            types that are supported by the standard ``json`` library.
            (However, the few formats "natively" supported by Biglist may get special treatment
            to relax this requirement.)
            If this is not possible, there are two solutions:

            1. Define a subclass of ``Biglist``. The subclass can customize the ``classmethod``\\s
               :meth:`dump_data_file` and :meth:`load_data_file` to handle extra serialization options
               internally.
            2. Define a custom serialization class and register it with :meth:`register_storage_format`.
        **kwargs
            additional arguments are passed on to :meth:`BiglistBase.new`.

        Returns
        -------
        Biglist
            A new :class:`Biglist` object.
        """
        if not batch_size:
            batch_size = 1000
            warnings.warn(
                'The default batch-size, 1000, may not be optimal for your use case; consider using the parameter ``batch_size``.'
            )
        else:
            assert batch_size > 0

        if storage_format is None:
            storage_format = cls.DEFAULT_STORAGE_FORMAT
        if storage_format.replace('_', '-') not in cls.registered_storage_formats:
            raise ValueError(f"invalid value of `storage_format`: '{storage_format}'")

        init_info = {
            **(init_info or {}),
            'storage_format': storage_format.replace('_', '-'),
            'storage_version': 3,
            # `storage_version` is a flag for certain breaking changes in the implementation,
            # such that certain parts of the code (mainly concerning I/O) need to
            # branch into different treatments according to the version.
            # This has little relation to `storage_format`.
            # version 0 designator introduced on 2022/3/8
            # version 1 designator introduced on 2022/7/25
            # version 2 designator introduced in version 0.7.4.
            # version 3 designator introduced in version 0.7.7.
            'batch_size': batch_size,
            'data_files_info': [],
        }
        if serialize_kwargs:
            init_info['serialize_kwargs'] = serialize_kwargs
        if deserialize_kwargs:
            init_info['deserialize_kwargs'] = deserialize_kwargs

        obj = super().new(path, init_info=init_info, **kwargs)  # type: ignore
        return obj

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        """
        Please see the base class for additional documentation.
        """
        super().__init__(*args, **kwargs)
        self.keep_files: bool = True
        """Indicates whether the persisted files should be kept or deleted when the object is garbage-collected."""

        self._append_buffer: list = []
        self._append_files_buffer: list = []
        self._file_dumper = None

        self._n_write_threads = 4
        """This value affects memory demand during quick "appending" (and flushing/dumping in the background).
        If the memory consumption of each batch is large, you could manually set this to a lower value, like::

            lst = Biglist(path)
            lst._n_write_threads = 4
        """

        self._serialize_kwargs = self.info.get('serialize_kwargs', {})
        self._deserialize_kwargs = self.info.get('deserialize_kwargs', {})
        if self.storage_format == 'parquet' and 'schema_spec' in self._serialize_kwargs:
            # Build the schema so that it does not need to be done each time the function
            # ``ParquetSerializer.serialize`` is called. Maybe this does not matter.
            assert 'schema' not in self._serialize_kwargs
            kk = copy.deepcopy(self._serialize_kwargs)
            kk['schema'] = make_parquet_schema(kk['schema_spec'])
            del kk['schema_spec']
            self._serialize_kwargs = kk

        _biglist_objs.add(self)

        # For back compat.
        if self.info.get('storage_version', 0) < 3:
            # This is not called by ``new``, instead is opening an existing dataset.
            # Usually these legacy datasets are in a "read-only" mode, i.e., you should
            # not append more data to them. If you do, the back-compat code below
            # may not be totally reliable if the dataset is being used by multiple workers
            # concurrently.
            if 'data_files_info' not in self.info:  # added in 0.7.4
                if self.storage_version == 0:
                    # This may not be totally reliable in every scenario.
                    # The older version had a parameter `lazy`, which is gone now.
                    # After some time we may stop supporting this storage version. (7/27/2022)
                    # However, as long as older datasets are in a "read-only" status,
                    # this is fine.
                    try:
                        data_info_file = self.path / 'datafiles_info.json'
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
                    files1 = (v.split('_') + [v] for v in files0)
                    files2 = (
                        (float(v[0]), v[-1], int(v[2].partition('.')[0]))
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

                self.info['data_files_info'] = data_files_info
                with lock_to_use(self._info_file) as ff:
                    ff.write_json(self.info, overwrite=True)

            else:
                # Added in 0.7.5: check for a bug introduced in 0.7.4.
                # Convert full path to file name.
                # Version 0.7.4 was used very briefly, hence very few datasets
                # were created by that version.
                data_files_info = self.info['data_files_info']
                if data_files_info:
                    new_info = None
                    if os.name == 'nt' and '\\' in data_files_info[0][0]:
                        new_info = [
                            (f[(f.rfind('\\') + 1) :], *_) for f, *_ in data_files_info
                        ]
                    elif '/' in data_files_info[0][0]:
                        new_info = [
                            (f[(f.rfind('/') + 1) :], *_) for f, *_ in data_files_info
                        ]
                    if new_info:
                        self.info['data_files_info'] = new_info
                        with lock_to_use(self._info_file) as ff:
                            ff.write_json(self.info, overwrite=True)

    def __del__(self) -> None:
        if getattr(self, 'keep_files', True) is False:
            self.destroy(concurrent=False)
        else:
            self._warn_flush()
            self.flush()

    @property
    def batch_size(self) -> int:
        """The max number of data items in one data file."""
        return self.info['batch_size']

    @property
    def data_path(self) -> Upath:
        return self.path / 'store'

    @property
    def storage_format(self) -> str:
        """The value of ``storage_format`` used in :meth:`new`, either user-specified or the default value."""
        return self.info['storage_format'].replace('_', '-')

    @property
    def storage_version(self) -> int:
        """The internal format used in persistence. This is a read-only attribute for information only."""
        return self.info.get('storage_version', 0)

    def _warn_flush(self):
        if self._append_buffer or self._append_files_buffer:
            warnings.warn(
                f"did you forget to flush {self.__class__.__name__} at '{self.path}'?"
            )

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
        self._warn_flush()
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
        self._warn_flush()
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
        """
        self._append_buffer.append(x)
        if len(self._append_buffer) >= self.batch_size:
            self._flush()

    def extend(self, x: Iterable[Element]) -> None:
        """This simply calls :meth:`append` repeatedly."""
        for v in x:
            self.append(v)

    def make_file_name(self, buffer_len: int, extra: str = '') -> str:
        """
        This method constructs the file name of a data file.
        If you need to customize this method for any reason, you should do it via ``extra``
        and keep the other patterns unchanged.
        The string ``extra`` will appear between other fixed patterns in the file name.

        One possible usecase is this: in distributed writing, you want files written by different workers
        to be distinguishable by the file names. Do something like this::

              def worker(datapath: str, worker_id: str, ...):
                  out = Biglist(datapath)
                  _make_file_name = out.make_file_name
                  out.make_file_name = lambda buffer_len: _make_file_name(buffer_len, worker_id)
                  ...
        """
        if extra:
            extra = extra.lstrip('_').rstrip('_') + '_'
        return f"{datetime.utcnow().strftime('%Y%m%d%H%M%S.%f')}_{extra}{str(uuid4()).replace('-', '')[:10]}_{buffer_len}"
        # File name pattern introduced on 7/25/2022.
        # This should guarantee the file name is unique, hence
        # we do not need to verify that this file name is not already used.
        # Also include timestamp and item count in the file name, in case
        # later we decide to use these pieces of info.
        # Changes in 0.7.4: the time part changes from epoch to datetime, with guaranteed fixed length.
        # Change in 0.8.4: the uuid part has dash removed and length reduced to 10; add ``extra``.

    def _flush(self) -> None:
        """
        Persist the content of the in-memory buffer to a file,
        reset the buffer, and update relevant book-keeping variables.

        This method is called any time the size of the in-memory buffer
        reaches ``self.batch_size``. This happens w/o the user's intervention.
        """
        if not self._append_buffer:
            # Called by `self.flush`.
            return

        buffer = self._append_buffer
        buffer_len = len(buffer)
        self._append_buffer = []

        datafile_ext = self.storage_format.replace('-', '_')
        filename = f'{self.make_file_name(buffer_len)}.{datafile_ext}'

        data_file = self.data_path / filename

        if self._file_dumper is None:
            self._file_dumper = Dumper(self._get_thread_pool(), self._n_write_threads)
        self._file_dumper.dump_file(
            self.dump_data_file, data_file, buffer, **self._serialize_kwargs
        )
        # This call will return quickly if the dumper has queue
        # capacity for the file. The file meta data below
        # will be updated as if the saving has completed, although
        # it hasn't (it is only queued). If dumping failed, the entry
        # will be deleted in `flush()`.

        self._append_files_buffer.append((filename, buffer_len))

    def flush(self, *, lock_timeout=300, raise_on_write_error: bool = True) -> None:
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
        is performed by :meth:`flush`.
        This is the second reason that user should call :meth:`flush` at the end of their
        data writting session, regardless of whether all the new data have been persisted
        in data files. (They would be if their count happens to be a multiple of ``self.batch_size``.)

        If there are multiple workers adding data to this biglist at the same time
        (from multiple processes or machines), data added by one worker will not be visible
        to another worker until the writing worker calls :meth:`flush` and the reading worker
        calls :meth:`reload`.

        Further, user should assume that data not yet persisted (i.e. still in "append buffer")
        are not visible to data reading via :meth:`__getitem__` or :meth:`__iter__` and not included in
        :meth:`__len__`, even to the same worker. In common use cases, we do not start reading data
        until we're done adding data to the biglist (at least "for now"), hence this is not
        a big issue.

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
        self._flush()
        if self._file_dumper is not None:
            errors = self._file_dumper.wait(raise_on_error=raise_on_write_error)
            if errors:
                for file, e in errors:
                    logger.error('failed to write file %s: %r', file, e)
                    fname = file.name
                    for i, (f, _) in enumerate(self._append_files_buffer):
                        if f == fname:
                            self._append_files_buffer.pop(i)
                            break
                    if file.exists():
                        try:
                            file.remove_file()
                        except Exception as e:
                            logger.error('failed to delete file %s: %r', file, e)

        # Other workers in other threads, processes, or machines may have appended data
        # to the list. This block merges the appends by the current worker with
        # appends by other workers. The last call to ``flush`` across all workers
        # will get the final meta info right.
        if self._append_files_buffer:
            with lock_to_use(self._info_file, timeout=lock_timeout) as ff:
                z0 = ff.read_json()['data_files_info']
                z = sorted(
                    set((*(tuple(v[:2]) for v in z0), *self._append_files_buffer))
                )
                # TODO: maybe a merge sort can be more efficient.
                cum = list(itertools.accumulate(v[1] for v in z))
                z = [(a, b, c) for (a, b), c in zip(z, cum)]
                self.info['data_files_info'] = z
                ff.write_json(self.info, overwrite=True)
            self._append_files_buffer.clear()

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
        self.info = self._info_file.read_json()

    @property
    def files(self):
        # This method should be cheap to call.
        # TODO: call `reload`?
        self._warn_flush()
        if self._deserialize_kwargs:
            fun = functools.partial(self.load_data_file, **self._deserialize_kwargs)
        else:
            fun = self.load_data_file
        return BiglistFileSeq(
            self.path,
            [
                (str(self.data_path / row[0]), *row[1:])
                for row in self.info['data_files_info']
            ],
            fun,
        )


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
        self._sem: threading.Semaphore | None = None  # type: ignore
        self._tasks: set[Future] = set()

    def _callback(self, t):
        self._sem.release()
        if not t.exception():
            self._tasks.remove(t)
        # If `t` raised exception, keep it in `self._tasks`
        # so that the exception can be re-raised in `self.wait`.

    def dump_file(
        self,
        file_dumper: Callable[[Upath, list], None],
        data_file: Upath,
        data: list,
        **kwargs,
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

        task = self._executor.submit(file_dumper, data_file, data, **kwargs)
        task._file = data_file
        self._tasks.add(task)
        task.add_done_callback(self._callback)
        # If task is already finished when this callback is being added,
        # then it is called immediately.

    def wait(self, *, raise_on_error: bool = True):
        """
        Wait to finish all the submitted dumping tasks.
        """
        if self._tasks:
            concurrent.futures.wait(self._tasks)
            if raise_on_error:
                for t in self._tasks:
                    _ = t.result()
            else:
                errors = []
                for t in self._tasks:
                    try:
                        _ = t.result()
                    except Exception as e:
                        errors.append((t._file, e))
                return errors


class BiglistFileReader(FileReader[Element]):
    def __init__(self, path: PathType, loader: Callable[[Upath], Any]):
        """
        Parameters
        ----------
        path
            Path of a data file.
        loader
            A function that will be used to load the data file.
            This must be pickle-able.
            Usually this is :meth:`Biglist.load_data_file`.
            If you customize this, please see the doc of :class:`~biglist.FileReader`.
        """
        super().__init__()
        self.path: Upath = resolve_path(path)
        self.loader = loader
        self._data: list | None = None

    def __getstate__(self):
        return self.path, self.loader

    def __setstate__(self, data):
        self.path, self.loader = data
        self._data = None

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


class JsonByteSerializer(serializer.ByteSerializer):
    @classmethod
    def serialize(cls, x, **kwargs):
        return json.dumps(x, **kwargs).encode()

    @classmethod
    def deserialize(cls, y, **kwargs):
        return serializer._loads(json.loads, y.decode(), **kwargs)


class ParquetSerializer(serializer.ByteSerializer):
    @classmethod
    def serialize(
        cls,
        x: list[dict],
        schema: pyarrow.Schema = None,
        schema_spec: Sequence = None,
        metadata=None,
        **kwargs,
    ):
        """
        `x` is a list of data items. Each item is a dict. In the output Parquet file,
        each item is a "row".

        The content of the item dict should follow a regular pattern.
        Not every structure is supported. The data `x` must be acceptable to
        `pyarrow.Table.from_pylist <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.from_pylist>`_. If unsure, use a list with a couple data elements
        and experiment with ``pyarrow.Table.from_pylist`` directly.

        When using ``storage_format='parquet'`` for :class:`Biglist`, each data element is a dict
        with a consistent structure that is acceptable to ``pyarrow.Table.from_pylist``.
        When reading the Biglist, the original Python data elements are returned.
        (A record read out may not be exactly equal to the original that was written, in that
        elements that were missing in a record when written may have been filled in with ``None``
        when read back out.)
        In other words, the reading is *not* like that of :class:`~biglist.ParquetBiglist`.
        You can always create a separate ParquetBiglist for the data files of the Biglist
        in order to use Parquet-style data reading. The data files are valid Parquet files.

        If neither ``schema`` nor ``schema_spec`` is specified, then the data schema is auto-inferred
        based on the first element of ``x``. If this does not work, you can specify either ``schema`` or ``schema_spec``.
        The advantage of ``schema_spec`` is that it is json-serializable Python types, hence can be passed into
        :meth:`Biglist.new() <biglist.Biglist.new>` via ``serialize_kwargs`` and saved in "info.json" of the biglist.

        If ``schema_spec`` is not flexible or powerful enough for your usecase, then you may have to use ``schema``.
        """
        if schema is not None:
            assert schema_spec is None
        elif schema_spec is not None:
            assert schema is None
            schema = make_parquet_schema(schema_spec)
        table = pyarrow.Table.from_pylist(x, schema=schema, metadata=metadata)
        sink = io.BytesIO()
        writer = pyarrow.parquet.ParquetWriter(sink, table.schema, **kwargs)
        writer.write_table(table)
        writer.close()
        sink.seek(0)
        # return sink.getvalue()  # bytes
        # return sink.getbuffer()  # memoryview
        return sink
        # this is a file-like object; `upathlib.LocalUpath.write_bytes` and `upathlib.GcsBlobUpath.write_bytes` accept this.
        # We do not returnn the bytes because `upathlib.GcsBlobUpath.write_bytes` can take file-like objects directly.

    @classmethod
    def deserialize(cls, y: bytes, **kwargs):
        try:
            memoryview(y)
        except TypeError:
            pass  # `y` is a file-like object
        else:
            # `y` is a bytes-like object
            y = io.BytesIO(y)
        table = pyarrow.parquet.ParquetFile(y, **kwargs).read()
        return table.to_pylist()


Biglist.register_storage_format('json', JsonByteSerializer)
Biglist.register_storage_format('pickle', serializer.PickleSerializer)
Biglist.register_storage_format('pickle-z', serializer.ZPickleSerializer)
Biglist.register_storage_format('pickle-zstd', serializer.ZstdPickleSerializer)
Biglist.register_storage_format('parquet', ParquetSerializer)


# Available if the package `lz4` is installed.
if hasattr(serializer, 'Lz4PickleSerializer'):
    Biglist.register_storage_format('pickle-lz4', serializer.Lz4PickleSerializer)


class ParquetBiglist(BiglistBase):
    """
    ``ParquetBiglist`` defines a kind of "external biglist", that is,
    it points to pre-existing Parquet files and provides facilities to read them.

    As long as you use a ParquetBiglist object to read, it is assumed that
    the dataset (all the data files) have not changed since the object was created
    by :meth:`new`.
    """

    @classmethod
    def new(
        cls,
        data_path: PathType | Sequence[PathType],
        path: PathType | None = None,
        *,
        suffix: str = '.parquet',
        **kwargs,
    ) -> ParquetBiglist:
        """
        This classmethod gathers info of the specified data files and
        saves the info to facilitate reading the data files.
        The data files remain "external" to the :class:`ParquetBiglist` object;
        the "data" persisted and managed by the ParquetBiglist object
        are the meta info about the Parquet data files.

        If the number of data files is small, it's feasible to create a temporary
        object of this class (by leaving ``path`` at the default value ``None``)
        "on-the-fly" for one-time use.

        Parameters
        ----------
        path
            Passed on to :meth:`BiglistBase.new` of :class:`BiglistBase`.
        data_path
            Parquet file(s) or folder(s) containing Parquet files.

            If this is a single path, then it's either a Parquet file or a directory.
            If this is a list, each element is either a Parquet file or a directory;
            there can be a mix of files and directories.
            Directories are traversed recursively for Parquet files.
            The paths can be local, or in the cloud, or a mix of both.

            Once the info of all Parquet files are gathered,
            their order is fixed as far as this :class:`ParquetBiglist` is concerned.
            The data sequence represented by this ParquetBiglist follows this
            order of the files. The order is determined as follows:

                The order of the entries in ``data_path`` is preserved; if any entry is a
                directory, the files therein (recursively) are sorted by the string
                value of each file's full path.

        suffix
            Only files with this suffix will be included.
            To include all files, use ``suffix='*'``.

        **kwargs
            additional arguments are passed on to :meth:`__init__`.
        """
        if isinstance(data_path, (str, Path, Upath)):
            #  TODO: in py 3.10, we will be able to do `isinstance(data_path, PathType)`
            data_path = [resolve_path(data_path)]
        else:
            data_path = [resolve_path(p) for p in data_path]

        def get_file_meta(p: Upath):
            ff = ParquetFileReader.load_file(p)
            meta = ff.metadata
            return {
                'path': str(p),  # str of full path
                'num_rows': meta.num_rows,
                # "row_groups_num_rows": [
                #     meta.row_group(k).num_rows for k in range(meta.num_row_groups)
                # ],
            }

        pool = get_global_thread_pool()
        tasks = []
        for p in data_path:
            if p.is_file():
                if suffix == '*' or p.name.endswith(suffix):
                    tasks.append(pool.submit(get_file_meta, p))
            else:
                tt = []
                for pp in p.riterdir():
                    if suffix == '*' or pp.name.endswith(suffix):
                        tt.append((str(pp), pool.submit(get_file_meta, pp)))
                tt.sort()
                for p, t in tt:
                    tasks.append(t)

        assert tasks
        datafiles = []
        for k, t in enumerate(tasks):
            datafiles.append(t.result())
            if (k + 1) % 1000 == 0:
                logger.info('processed %d files', k + 1)

        datafiles_cumlength = list(
            itertools.accumulate(v['num_rows'] for v in datafiles)
        )

        obj = super().new(path, **kwargs)  # type: ignore
        obj.info['datapath'] = [str(p) for p in data_path]

        # Removed in 0.7.4
        # obj.info["datafiles"] = datafiles
        # obj.info["datafiles_cumlength"] = datafiles_cumlength

        # Added in 0.7.4
        data_files_info = [
            (a['path'], a['num_rows'], b)
            for a, b in zip(datafiles, datafiles_cumlength)
        ]
        obj.info['data_files_info'] = data_files_info

        obj.info['storage_format'] = 'parquet'
        obj.info['storage_version'] = 1
        # `storage_version` is a flag for certain breaking changes in the implementation,
        # such that certain parts of the code (mainly concerning I/O) need to
        # branch into different treatments according to the version.
        # This has little relation to `storage_format`.
        # version 1 designator introduced in version 0.7.4.
        # prior to 0.7.4 it is absent, and considered 0.

        obj._info_file.write_json(obj.info, overwrite=True)

        return obj

    def __init__(self, *args, **kwargs):
        """Please see doc of the base class."""
        super().__init__(*args, **kwargs)
        self.keep_files: bool = True
        """Indicates whether the meta info persisted by this object should be kept or deleted when this object is garbage-collected.

        This does *not* affect the external Parquet data files.
        """

        # For back compat. Added in 0.7.4.
        if self.info and 'data_files_info' not in self.info:
            # This is not called by ``new``, instead is opening an existing dataset
            assert self.storage_version == 0
            data_files_info = [
                (a['path'], a['num_rows'], b)
                for a, b in zip(
                    self.info['datafiles'], self.info['datafiles_cumlength']
                )
            ]
            self.info['data_files_info'] = data_files_info
            with lock_to_use(self._info_file) as ff:
                ff.write_json(self.info, overwrite=True)

    def __repr__(self):
        return f"<{self.__class__.__name__} at '{self.path}' with {len(self)} records in {len(self.files)} data file(s) stored at {self.info['datapath']}>"

    @property
    def storage_version(self) -> int:
        return self.info.get('storage_version', 0)

    @property
    def files(self):
        # This method should be cheap to call.
        return ParquetFileSeq(
            self.path,
            self.info['data_files_info'],
        )


class ParquetFileSeq(FileSeq[ParquetFileReader]):
    def __init__(
        self,
        root_dir: Upath,
        data_files_info: list[tuple[str, int, int]],
    ):
        """
        Parameters
        ----------
        root_dir
            Root directory for storage of meta info.
        data_files_info
            A list of data files that constitute the file sequence.
            Each tuple in the list is comprised of a file path (relative to
            ``root_dir``), number of data items in the file, and cumulative
            number of data items in the files up to the one at hand.
            Therefore, the order of the files in the list is significant.
        """
        self._root_dir = root_dir
        self._data_files_info = data_files_info

    @property
    def path(self):
        return self._root_dir

    @property
    def data_files_info(self):
        return self._data_files_info

    def __getitem__(self, idx: int):
        return ParquetFileReader(
            self._data_files_info[idx][0],
        )
