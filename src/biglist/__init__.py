"""
The package ``biglist`` provides persisted, out-of-memory Python data structures
that implement the ``Sequence`` interface, most importantly iteration, as well as random element access.
The main use case is processing large amounts of data that can not fit in memory.
Persistence can be on local disk or in cloud storage. Interfacing with the storage
uses the package ``upathlib``.

Currently, two classes are provided, namely ``Biglist`` and ``ParquetBiglist``.

``Biglist`` manages writing (i.e. adding data to this facility and be managed by it) and reading.
The class manages its data files, in addition to meta info files.
Writing, or "mutation", is append-only; updating existing data elements is not supported.
Appending can be conducted by a number of distributed workers.

``ParquetBiglist`` defines a kind of "external" ``Biglist``. A set of data files in the Apache Parquet
format have been prepared by other code; this class performs reading only,
after gathering and persisting some meta info about the data files.

``Biglist`` and ``ParquetBiglist`` share the same core API for reading.
Although random element access is supported, it is not optimized
and is not the target usage pattern. The recommended way of data consumption is by iteration.
Distributed reading, i.e. iterating over the entire dataset by distributed workers collectively,
is supported.

The utility class ``ListView`` takes any ``Sequence`` object and implements accessing
its elements by single index, slice, and list of indices (like what ``numpy`` supports).
Since ``Biglist`` and ``ParquetBiglist`` are both ``Sequence``'s, they also support
these access mechanisms via ``ListView``.
"""
# flake8: noqa
__version__ = "0.7.1b1"

from ._base import FileView, ListView, ChainedList
from ._biglist import Biglist, BiglistFileData

try:
    from ._parquet import (
        ParquetBiglist,
        ParquetFileData,
        ParquetBatchData,
        read_parquet_file,
        write_parquet_file,
    )
except ImportError:
    pass
