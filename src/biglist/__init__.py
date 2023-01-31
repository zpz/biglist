"""
The package `biglist <https://github.com/zpz/biglist>`_ provides persisted, out-of-memory Python data structures
that implement the |Sequence|_ and |Iterable|_ interfaces with the capabilities of
concurrent and distributed reading and writing.
The main use case is processing large amounts of data that can not fit in memory.

Currently, two kinds of "biglists" are provided, namely :class:`Biglist` and :class:`ParquetBiglist`.

:class:`Biglist` manages writing and reading.
Here, "writing" refers to adding data to this facility and be managed by it.
The class manages its data files in addition to meta info.
Writing is append-only; updating existing data is not supported.
Appending can be conducted by a number of distributed workers.

:class:`ParquetBiglist` defines a kind of "external biglist". When given the paths to a set of
pre-existing data files in the Apache Parquet format,
this class provides a rich set of facilities for reading the data.

:class:`Biglist` and :class:`ParquetBiglist` share the same core API for *reading*.
Although random element access is supported, it is not optimized
and is not the target usage pattern. The intended way of data consumption is by iteration.
Iteration can be done by distributed workers *collectively*.

Persistence can be on local disk or in cloud storage.
Thanks to the package `upathlib <https://github.com/zpz/upathlib>`_, the user API as well as
the implementation is agnostic to the location of storage.

Additional utilities provide mechanisms for "slicing and dicing" a biglist,
as well as "chaining up" a series of biglists. These utilities work not only for biglist,
but also for any |Sequence|_.
"""
from __future__ import annotations

from ._base import BiglistBase, FileReader, FileSeq
from ._biglist import Biglist, BiglistFileReader, BiglistFileSeq
from ._util import Chain, Seq, Slicer

__version__ = "0.7.4b4"

# Back compat; will be removed >=0.7.6
FileView = FileReader
BiglistFileView = BiglistFileReader
ListView = Slicer
ChainedList = Chain


try:
    from ._parquet import (
        ParquetBatchData,
        ParquetBiglist,
        ParquetFileReader,
        ParquetFileSeq,
        read_parquet_file,
        write_parquet_file,
    )

    # Back compat; will be removed after >=0.7.6
    ParquetFileData = ParquetFileReader

except ImportError:
    pass
