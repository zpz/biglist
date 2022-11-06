import bisect
import collections.abc
import concurrent.futures
import itertools
import os
import random
from concurrent.futures import ThreadPoolExecutor
from typing import Union, Sequence

from pyarrow import parquet
from upathlib import Upath
from upathlib.util import PathType, resolve_path, is_path
from ._base import BiglistBase, FileLoaderMode


class ParquetBiglist(BiglistBase):
    """
    `ParquetBiglist` defines a kind of "external Biglist", that is,
    it points to pre-existing Parquet files (produced by other code)
    and provides facilities to read the data.

    Data files are read in arbitrary order determined by this class,
    that is, the data sequence produced will most likely differ from
    what is read out by other Parquet utilities. However, if you use
    a saved `ParquetBiglist` object again for reading, it will read
    in the same order.

    As long as you use a `ParquetBiglist` object to read, it is assumed
    the dataset has not changed since the creation of the object.
    """

    @classmethod
    def new(
        cls,
        data_path: Union[PathType, Sequence[PathType]],
        path: PathType = None,
        *,
        suffix: str = ".parquet",
        keep_files: bool = None,
        shuffle: bool = False,
        thread_pool_executor: ThreadPoolExecutor = None,
        **kwargs,
    ):
        """
        `data_path`: Parquet file(s) for folder(s) containing Parquet files;
            folders are traversed recursively. The data files can represent a mix
            of locations, including a mix of local and cloud locations, as long
            as they don't change. However, if any data file is on the local disk,
            you're tied to the particular machine for the use of the `ParquetBiglist`
            object.

        This classmethod gathers info of the data files and saves it to facilitate
        reading the data.

        If the number of data files is small, it's entirely feasible to create a temporary
        object of this class (by leaving `path` at the default `None`) "on-the-fly"
        for one-time use.
        """
        if is_path(data_path):
            data_path = [resolve_path(data_path)]
        else:
            data_path = [resolve_path(p) for p in data_path]

        if not path:
            path = cls.get_temp_path()
            if keep_files is None:
                keep_files = False
        else:
            path = resolve_path(path)
            if keep_files is None:
                keep_files = True
        if path.is_dir():
            raise Exception(f'directory "{path}" already exists')
        elif path.is_file():
            raise FileExistsError(path)

        datafiles = []

        def get_file_meta(f): 
            meta = parquet.read_metadata(str(f))
            datafiles.append(
                {
                    "path": str(f),  # because Upath object does not support JSON
                    "num_rows": meta.num_rows,
                    "row_groups_num_rows": [
                        meta.row_group(k).num_rows for k in range(meta.num_row_groups)
                    ],
                }
            )

        if thread_pool_executor is not None:
            pool = thread_pool_executor
        else:
            pool = ThreadPoolExecutor(min(32, (os.cpu_count() or 1) + 4))
        tasks = []
        for p in data_path:
            if p.is_file():
                if suffix == "*" or p.name.endswith(suffix):
                    pool.submit(get_file_meta, p)
            else:
                for q in p.riterdir():
                    if suffix == "*" or q.name.endswith(suffix):
                        pool.submit(get_file_meta, q)
        assert tasks
        if thread_pool_executor is None:
            pool.shutdown()
        else:
            concurrent.futures.wait(tasks)

        if shuffle:
            random.shuffle(datafiles)

        datafiles_cumlength = list(
            itertools.accumulate(v["num_rows"] for v in datafiles)
        )

        obj = cls(path, require_exists=False, thread_pool_executor=thread_pool_executor, **kwargs)  # type: ignore
        obj.keep_files = keep_files
        obj.info["datafiles"] = datafiles
        obj.info["datafiles_cumlength"] = datafiles_cumlength
        obj._info_file.write_json(obj.info)

        return obj

    def __del__(self) -> None:
        if self.keep_files:
            self.flush()
        else:
            self.destroy()

    @classmethod
    def load_data_file(cls, path: Upath, mode: int):
        return ParquetFileData(path, mode)

    def get_data_files(self):
        return self.info["datafiles"], self.info["datafiles_cumlength"]

    def get_data_file(self, datafiles, idx):
        return resolve_path(datafiles[idx]["path"])

    def iter_batches(self, batch_size=10000):
        # Yield native Apache Arrow objects for experiments.
        datafiles, _ = self.get_data_files()
        for ifile in range(len(datafiles)):
            filedata = self.load_data_file(
                self.get_data_file(datafiles, ifile), FileLoaderMode.RAND
            )
            yield from filedata.file.iter_batches(batch_size)


class ParquetFileData(collections.abc.Sequence):
    # Represents data of a single Parquet file,
    # with facilities to make it conform to our required APIs.
    #
    # If you want to use the  `arrow.parquet` methods directly,
    # use it via `self.file`, or `self.table` if not None.

    # TODO: allow selecting columns.

    def __init__(self, path: Upath, mode: int):
        self.path = path
        self.file = parquet.ParquetFile(str(path))
        self.num_columns = self.file.metadata.num_columns
        self.num_rows = self.file.metadata.num_rows
        self.num_row_groups = self.file.metadata.num_row_groups
        self._row_groups_num_rows = None
        self._row_groups_num_rows_cumsum = None
        self._row_groups = [None] * self.num_row_groups
        self._data = None
        if mode == FileLoaderMode.ITER:
            _ = self.data
        self._batch_size = 10000

    @property
    def data(self):
        if self._data is None:
            self._data = self.file.read()
        return self._data

    def __len__(self):
        return self.num_rows

    def __getitem__(self, idx: int):
        if idx < 0:
            idx = self.num_rows + idx
        if idx < 0 or idx >= self.num_rows:
            raise IndexError(idx)

        if self._data is not None:
            if self._data.num_columns == 1:
                return self._data.column(0).take([idx])[0]
            else:
                return self._data.take([idx]).to_pylist()[0]  # dict

        if self._row_groups_num_rows is None:
            meta = self.file.metadata
            self._row_groups_num_rows = [
                meta.row_group(i).num_rows for i in range(meta.num_row_groups)
            ]
            self._row_groups_num_rows_cumsum = list(
                itertools.accumulate(self._row_groups_num_rows)
            )

        igrp = bisect.bisect_right(self._row_groups_num_rows_cumsum, idx)
        if self._row_groups[igrp] is None:
            self._row_groups[igrp] = self.file.read_row_group(igrp)
        if igrp == 0:
            idx_in_row_group = idx
        else:
            idx_in_row_group = idx - self._row_groups_num_rows_cumsum[igrp - 1]
        row_group = self._row_groups[igrp]
        if self.num_columns == 1:
            return row_group.column(0).take([idx_in_row_group])[0]
        else:
            return row_group.take([idx_in_row_group]).to_pylist()[0]  # dict

    def __iter__(self):
        if self._data is None:
            for batch in self.file.iter_batches(self._batch_size):
                if batch.num_columns == 1:
                    yield from batch.column(0)
                else:
                    yield from batch.to_pylist()
        else:
            if self._data.num_columns == 1:
                yield from self._data.column(0)
            else:
                yield from self._data.to_pylist()
