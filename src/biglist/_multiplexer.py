from __future__ import annotations

import logging
import multiprocessing
import threading
from collections.abc import Iterator, Iterable
from datetime import datetime

from upathlib import Upath, resolve_path, PathType

from ._util import lock_to_use, Element
from ._biglist import Biglist


logger = logging.getLogger(__name__)


class Multiplexer[Iterable[Element]]:
    """
    Multiplexer is used to distribute data elements to multiple "workers" so that
    each element is obtained by exactly one worker.

    Typically, the data element is small in size but requires significant time to process
    by the worker. The data elements are "hyper parameters".

    The usage consists of two main parts:

    1. In "controller" code, call :meth:`start` to start a new "session".
    Different sessions (at the same time or otherwise) are independent consumers of the data.

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
            path / "data",
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
            of this class with the same ``path`` parameter.
        worker_id
            A string representing a particular worker.
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
            self._data = Biglist(self.path / "data")
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
        return self.path / ".mux" / task_id / "info.json"

    def start(self) -> str:
        """
        One worker, such as a "coordinator", calls this method once.
        After that, one or more workers independently
        iterate over a :class:`Multiplexer` object with the task-ID returned by
        this method. The data that was provided to :meth:`new` is
        split between the workers in that each data element will be obtained
        by exactly one worker.

        In order to call this method, the object should have been initiated without
        ``task_id`` or ``worker_id``.

        The returned value is the argument ``task_id`` to be provided to :meth:`__init__`
        in worker code.
        """
        assert not self._task_id
        assert not self._worker_id
        task_id = datetime.utcnow().isoformat()
        self._mux_info_file(task_id).write_json(
            {
                "total": len(self.data),
                "next": 0,
                "time": datetime.utcnow().isoformat(),
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

        In order to call this method, ``task_id`` must have been provided
        to :meth:`__init__`.
        """
        assert self._task_id
        if not self._worker_id:
            self._worker_id = "{} {}".format(
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
            yield self.data[n]

    def stat(self) -> dict:
        """
        Return status info of an ongoing iteration.
        """
        assert self._task_id
        return self._mux_info_file(self._task_id).read_json()

    def done(self) -> bool:
        """
        Return whether the data iteration is finished.

        This is often called in the "controller" code on the object
        that has had its :meth:`start` called.
        """
        ss = self.stat()
        return ss["next"] == ss["total"]

    def destroy(self) -> None:
        """
        Delete all the data stored by this Multiplexer, hence reclaiming the storage space.
        """
        self.data.destroy()
        self.path.rmrf()
