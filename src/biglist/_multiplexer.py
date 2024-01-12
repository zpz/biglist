from __future__ import annotations

import logging
import multiprocessing
import threading
from collections.abc import Iterable, Iterator, Sized
from datetime import datetime

from upathlib import PathType, Upath, resolve_path

from ._biglist import Biglist
from ._util import Element, lock_to_use

logger = logging.getLogger(__name__)


class Multiplexer(Iterable[Element], Sized):
    """
    Multiplexer is used to distribute data elements to multiple "workers" so that
    each element is obtained by exactly one worker.

    Typically, the data element is small in size but requires significant time to process
    by the worker. The data elements are "hyper parameters".

    The usage consists of two main parts:

    1. In "coordinator" code, call :meth:`start` to start a new "session".
    Different sessions (at the same time or otherwise) are independent consumers of the data.

       Typically, this dataset, which is small and easy to create, is consumed only once.
       In this case, the coordinator code typically calls :meth:`new` to create a new Multiplexer,
       then calls :meth:`start` on it, and then manages to send the session ID to workers.

    2. In "worker" code, use the session ID that was returned by :meth:`start` to instantiate
    a Multiplexer and iterate over it. In so doing, multiple workers will obtain the data elements
    collectively, i.e., each element is obtained by exactly one worker.
    """

    @classmethod
    def new(
        cls,
        data: Iterable[Element],
        path: PathType | None,
        *,
        batch_size: int = 10_000,
        storage_format: str = 'pickle',
    ):
        """
        Parameters
        ----------
        data
            The data elements that need to be distributed. The elements should be pickle-able.
        path
            A non-existent directory where the data and any supporting info will be saved.

            If ``path`` is in the cloud, then the workers can be on multiple machines, and in multiple threads
            or processes on each machine.
            If ``path`` is on the local disk, then the workers are in threads or processes on the same machine.

            However, there are no strong reasons to use this facility on a local machine.

            Usually this class is used to distribute data to a cluster of machines, hence
            this path points to a location in a cloud storage that is supported by
            `upathlib <https://github.com/zpz/upathlib>`_.
        """
        path = resolve_path(path)
        bl = Biglist.new(
            path / 'data',
            batch_size=batch_size,
            storage_format=storage_format,
        )
        bl.extend(data)
        bl.flush()
        assert len(bl) > 0
        return cls(path)

    def __init__(
        self,
        path: PathType,
        task_id: str | None = None,
        worker_id: str | None = None,
        timeout: int | float = 300,
    ):
        """
        Create a Multiplexer object and use it to distribute the data elements that have been
        stored by :meth:`new`.

        Parameters
        ----------
        path
            The directory where data is stored. This is the ``path`` that was passed to :meth:`new`.
        task_id
            A string that was returned by :meth:`start` on another instance
            of this class that points to the same ``path``.
        worker_id
            A string representing the current worker (i.e. this instance).
            This is meaningful only if ``task_id`` is provided.
            If ``task_id`` is provided but ``worker_id`` is missing,
            a default is constructed based on thread name and process name.
        """
        if task_id is None:
            assert worker_id is None
        self.path = path
        self._task_id = task_id
        self._worker_id = worker_id
        self._data = None
        self._timeout = timeout

    @property
    def data(self) -> Biglist[Element]:
        """
        Return the data elements stored in this Multiplexer.
        """
        if self._data is None:
            self._data = Biglist(self.path / 'data')
        return self._data

    def __len__(self) -> int:
        """
        Return the number of data elements stored in this Multiplexer.
        """
        return len(self.data)

    def _mux_info_file(self, task_id: str) -> Upath:
        """
        `task_id`: returned by :meth:`start`.
        """
        return self.path / '.mux' / task_id / 'info.json'

    def start(self) -> str:
        """
        Let's say there is a "coordinator" and some "workers"; these are programs running in
        threads, processes, or distributed machines. The coordinator creates a new Multiplexer
        and calls this method to start a "session" to read (i.e. iterate over) the elements
        in this Multiplexer::

            mux = Multiplexer.new(range(1000))
            task_id = mux.start()
            mux_path = mux.path

        The ``task_id`` is then provided to the workers, which will create Multiplexer instances
        pointing to the same dataset and using the ``task_id`` to participate in the reading session::

            mux = Multiplexer(mux_path, task_id)
            for x in mux:
                ...

        The data that was provided to :meth:`new` is
        split between the workers so that each data element will be obtained
        by exactly one worker.

        In order to call this method, the object should have been initiated without
        ``task_id`` or ``worker_id``. Often, that is an object that has just been
        created by :meth:`new`.

        After this call, the coordinator also becomes a "member" in this reading session because
        it holds the session ID just like the other workers. However, the coordinator code
        does not have to *participate* in reading.

        The returned value identifies one particular reading session. All workers that use the same 
        session ID participate in the same reading session, i.e. the data elements will be split between them.
        There can be multiple, independent reading sessions going on at the same time.
        """
        assert not self._task_id
        assert not self._worker_id
        task_id = datetime.utcnow().isoformat()
        self._mux_info_file(task_id).write_json(
            {
                'total': len(self.data),
                'next': 0,
                'time': datetime.utcnow().isoformat(),
            },
            overwrite=False,
        )
        self._task_id = task_id
        # Usually the object that called ``start`` will not be used by a worker
        # to iterate over the data. The task-id is stored on the object
        # mainly to enable this "controller" to call ``stat`` later.
        return task_id

    def __iter__(self) -> Iterator[Element]:
        """
        Worker iterates over the data contained in the Multiplexer.

        In order to call this method, the instance must hold a reading session ID.
        This is the case either the instance has been created with ``task_id`` provided to :meth:`__init__`,
        or :meth:`start` has been called on the instance.
        """
        assert self._task_id
        if not self._worker_id:
            self._worker_id = '{} {}'.format(
                multiprocessing.current_process().name,
                threading.current_thread().name,
            )
        worker_id = self._worker_id
        timeout = self._timeout
        finfo = self._mux_info_file(self._task_id)
        while True:
            with lock_to_use(finfo, timeout=timeout) as ff:
                # In concurrent use cases, I've observed
                # `upathlib.LockAcquireError` raised here.
                # User may want to do retry here.
                ss = ff.read_json()
                # In concurrent use cases, I've observed
                # `FileNotFoundError` here. User may want
                # to do retry here.

                n = ss['next']
                if n == ss['total']:
                    return
                ff.write_json(
                    {
                        'next': n + 1,
                        'worker_id': worker_id,
                        'time': datetime.utcnow().isoformat(),
                        'total': ss['total'],
                    },
                    overwrite=True,
                )
            yield self.data[n]

    def stat(self) -> dict:
        """
        Return status info of an ongoing iteration.

        In order to call this method, the instance must hold a reading session ID.
        """
        assert self._task_id
        return self._mux_info_file(self._task_id).read_json()

    def done(self) -> bool:
        """
        Return whether the data iteration is finished.

        In order to call this method, the instance must hold a reading session ID.

        This is often called in the "coordinator" code on the object
        that has had its :meth:`start` called.
        """
        ss = self.stat()
        return ss['next'] == ss['total']

    def destroy(self) -> None:
        """
        Delete all the data stored by this Multiplexer, hence reclaiming the storage space.
        """
        self.data.destroy()
        self.path.rmrf()
