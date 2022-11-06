import bisect
import collections.abc
import itertools
import random
from typing import Union, Sequence

from pyarrow import parquet
from upathlib import Upath
from upathlib.util import PathType, resolve_path, is_path
from ._base import BiglistBase, FileLoaderMode


class BigParquetList(BiglistBase):
    @classmethod
    def new(
        cls,
        data_path: Union[PathType, Sequence[PathType]],
        path: PathType = None,
        *,
        suffix: str = ".parquet",
        keep_files: bool = None,
        shuffle: bool = False,
        **kwargs,
    ):
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

        # TODO: threading
        files = []
        for p in data_path:
            if p.is_file():
                if suffix == "*" or p.name.endswith(suffix):
                    files.append(p)
            else:
                for pp in p.riterdir():
                    if suffix == "*" or pp.name.endswith(suffix):
                        files.append(pp)
        assert files
        datafiles = []
        for f in files:
            meta = parquet.read_metadata(str(f))
            datafiles.append(
                {
                    "path": f,
                    "num_rows": meta.num_rows,
                    "row_groups_num_rows": [
                        meta.row_group(k).num_rows for k in range(meta.num_row_groups)
                    ],
                }
            )

        if shuffle:
            random.shuffle(datafiles)

        datafiles_cumlength = list(
            itertools.accumulate(v["num_rows"] for v in datafiles)
        )

        obj = cls(path, require_exists=False, **kwargs)  # type: ignore
        obj.info["datafiles"] = datafiles
        obj.info["datafiles_cumlength"] = datafiles_cumlength
        if keep_files:
            for v in obj.info["datafiles"]:
                v["path"] = str(v["path"])  # because Upath object does not support JSON
            obj._info_file.write_json(obj.info)

        return obj

    @classmethod
    def load_data_file(cls, path: Upath, mode: int):
        return ParquetData(path, mode)

    def get_data_files(self):
        return self.info["datafiles"], self.info["datafiles_cumlength"]

    def get_data_file(self, datafiles, idx):
        return resolve_path(datafiles[idx]["path"])


class ParquetData(collections.abc.Sequence):
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
        if mode == FileLoaderMode.ITER:
            self.table = self.file.read()
        else:
            self.table = None
        self._batch_size = 10000

    def __len__(self):
        return self.num_rows

    def __getitem__(self, idx: Union[int, slice]):
        if isinstance(idx, slice):
            raise NotImplementedError

        if idx < 0:
            idx = self.num_rows + idx
        if idx >= self.num_rows:
            raise IndexError(idx)

        if self.table is not None:
            if self._table.num_columns == 1:
                return self.table.column(0).take([idx])[0]
            else:
                return self.table.take([idx]).to_pylist()[0]  # dict

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
        if self.table is None:
            for batch in self.file.iter_batches(self._batch_size):
                if batch.num_columns == 1:
                    yield from batch.column(0)
                else:
                    yield from batch.to_pylist()
        else:
            if self.table.num_columns == 1:
                yield from self.table.column(0)
            else:
                yield from self.table.to_pylist()
