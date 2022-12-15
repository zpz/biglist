.. biglist documentation master file, created by
   sphinx-quickstart on Fri Nov 25 22:11:50 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. :tocdepth: 3

.. |Sequence| replace:: :class:`Sequence`
.. _Sequence: https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence
.. |Iterable| replace:: :class:`Iterable`
.. _Iterable: https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable
.. _pyarrow: https://arrow.apache.org/docs/python/index.html
.. _pyarrow.Array: https://arrow.apache.org/docs/python/generated/pyarrow.Array.html#pyarrow.Array
.. _pyarrow.ChunkedArray: https://arrow.apache.org/docs/python/generated/pyarrow.ChunkedArray.html#pyarrow.ChunkedArray
.. _pyarrow.parquet.ParquetFile: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html
.. _pyarrow.Table: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html
.. _pyarrow.RecordBatch: https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html#pyarrow.RecordBatch
.. _pyarrow.Scalar: https://arrow.apache.org/docs/python/generated/pyarrow.Scalar.html#pyarrow.Scalar
.. _pyarrow.lib.StringScalar: https://arrow.apache.org/docs/python/generated/pyarrow.StringScalar.html
.. _pyarrow.fs.GcsFileSystem: https://arrow.apache.org/docs/python/generated/pyarrow.fs.GcsFileSystem.html
.. _upathlib.Upath: https://github.com/zpz/upathlib/blob/main/src/upathlib/_upath.py

===========
``biglist``
===========

.. automodule:: biglist


Installation
============

To install ``biglist`` and use local disk for persistence, simply do 

::

   pip install biglist

``biglist`` has a couple optional components:

   ``gcs``
      for using Google Cloud Storage for persistence
   ``parquet``
      for reading data files saved in the Apache Parquet format

So, as appropriate for your need, you may do 

::

   pip install "biglist[gcs]"

or

::

   pip install "biglist[parquet]"

or even

::

   pip install "biglist[gcs,parquet]"


Creating a Biglist
==================

Create a new :class:`Biglist` object via the classmethod :meth:`Biglist.new`::

   >>> from biglist import Biglist
   >>> mylist = Biglist.new(batch_size=100)

then add data to it, for example,

::

   >>> for x in range(10_023):
   ...     mylist.append(x)

This saves a new data file for every 100 elements
accumulated. In the end, there are 23 elements in a memory buffer that are
not yet persisted to disk. The code has no way to know whether we will append
more elements, hence it does not save this partial batch.
Suppose we're done with adding data, we call :meth:`Biglist.flush` to persist
the content of the buffer to disk::

   >>> mylist.flush()

If, after a while, we decide to append more data to ``mylist``, we just call :meth:`Biglist.append` again.
We can continue to add more data as long as the disk has space.
New data files will be saved. The smaller file containing 23 elements will stay there
among larger files with no problem.

Now let's take a look::

   >>> mylist
   <Biglist at '/tmp/19f88a17-3e78-430f-aad0-a35d39485f80' with 10023 elements in 101 data file(s)>
   >>> len(mylist)
   10023
   >>> mylist.path
   LocalUpath('/tmp/19f88a17-3e78-430f-aad0-a35d39485f80')
   >>> mylist.num_datafiles
   101

The data have been saved in the directory ``/tmp/19f88a17-3e78-430f-aad0-a35d39485f80``,
which is a temporary one because we did not tell :meth:`Biglist.new` where to save data.
When the object ``mylist`` dies, this directory will be deleted automatically.
This has its uses, but often we want to save the data for future use. In that case, just pass 
a non-existent directory to :meth:`Biglist.new`, e.g.

   >>> yourlist = Biglist.new('/project/data/store-a', batch_size=10_000)

Later, initiate a :class:`Biglist` object for reading the existing dataset::

   >>> yourlist = Biglist('/project/data/store-a')

If we want to persist the data in Google Cloud Storage, we would specify a path in the
``'gs://bucket-name/path/to/data'`` format.


Reading a Biglist
=================

Random element access
---------------------

:class:`Biglist` implements the |Sequence|_ interface, hence we can access any element like we do a list::

   >>> mylist[18]
   18
   >>> mylist[-3]
   10020

It does not support slicing directly.
However, the method :meth:`Biglist.view` returns an object that supports element access by a single index, by a slice, or by a list of indices::

   >>> v = mylist.view()
   >>> len(v)
   10023
   >>> v
   <ListView into 10023/10023 of <Biglist at '/tmp/dc260854-8041-40e8-801c-34084451d7a3' with 10023 elements in 101 data file(s)>>
   >>> v[83]
   83
   >>> v[100:104]
   <ListView into 4/10023 of <Biglist at '/tmp/dc260854-8041-40e8-801c-34084451d7a3' with 10023 elements in 101 data file(s)>>
   >>>

Note that slicing the view does not return a list of values.
Instead, it returns another :class:`ListView` object, which, naturally, can be used the same way,
including slicing further.

A :class:`ListView` object is a |Sequence|_, hence we can gather all of its elements in a list::

   >>> list(v[100:104])
   [100, 101, 102, 103]

:class:`ListView` provides a convenience method :meth:`ListView.collect` to do the same::

   >>> v[100:104].collect()
   [100, 101, 102, 103]


A few more examples::

   >>> v[-8:].collect()
   [10015, 10016, 10017, 10018, 10019, 10020, 10021, 10022]
   >>> v[-8::2].collect()
   [10015, 10017, 10019, 10021]
   >>> v[[1, 83, 250, -2]]
   <ListView into 4/10023 of <Biglist at '/tmp/dc260854-8041-40e8-801c-34084451d7a3' with 10023 elements in 101 data file(s)>>
   >>> v[[1, 83, 250, -2]].collect()
   [1, 83, 250, 10021]
   >>> v[[1, 83, 250, -2]][-3:].collect()
   [83, 250, 10021]


Iteration
---------

Despite the myriad ways of random access,
don't be carried away by the ease and flexibilities.
Random element access for :class:`Biglist` is **inefficient**.
The reason is that it needs to load a data file that contains the element of interest.
If the biglist has many data files and we are "jumping around" randomly,
it is wasting a lot of time loading entire files just to access select data elements in them.
(However, *consecutive* random accesses to elements residing in the same file will not load the file
repeatedly.)

The preferred way to consume the data of a :class:`Biglist` is to iterate over it. For example,

::

   >>> for i, x in enumerate(mylist):
   ...     print(x)
   ...     if i > 4:
   ...         break
   0
   1
   2
   3
   4
   5

Under the hood, this iterates the data files, and then simply iterates the elements in each file.
In other words,

::

   >>> for x in mylist: 
   ...     print(x)    

is implemented by

::

   >>> for batch in mylist.iter_files():
   ...     for x in batch:
   ...         print(x)

There are cases where we want to use :meth:`Biglist.iter_files` directly and handle the yielded :class:`FileReader` objects.


Reading from a Biglist in multiple processes
--------------------------------------------

To *collectively* consume a :class:`Biglist` object from multiple processes,
we can distribute :class:`FileReader`\s to the processes. The method :meth:`~biglist._base.BiglistBase.file_readers` returns
a list of FileReader objects::

   >>> file_readers = mylist.file_readers()
   >>> len(file_readers)
   101
   >>> file_readers[0]
   BiglistFileReader('/tmp/cfb39dc0-94bb-4557-a056-c7cea20ea653/store/1669667946.647939_46eb97f6-bdf3-45d2-809c-b90c613d69c7_100.pickle_zstd', <bound method Biglist.load_data_file of <class 'biglist._biglist.Biglist'>>)

(:class:`BiglistFileReader` inherits from :class:`FileReader`.)
A :class:`FileReader` object is light-weight. Upon initiation, it has not loaded the file yet---it merely records the file path along with the function that will be used to load the file.
In addition, FileReader objects are friendly to pickling, hence lend themselves to multiprocessing code.
Let's design a small experiment to consume this dataset in multiple processes::

   >>> def worker(file_reader):
   ...     total = 0
   ...     for x in file_reader:
   ...         total += x
   ...     return total
   >>> from concurrent.futures import ProcessPoolExecutor
   >>> total = 0
   >>> with ProcessPoolExecutor(5) as pool:
   ...     tasks = [pool.submit(worker, fr) for fr in file_readers]
   ...     for t in tasks:
   ...         total += t.result()
   >>> total
   50225253

What is the expected result?

::

   >>> sum(mylist)
   50225253

Sure enough, this verifies that the entire biglist is consumed by the processes *collectively*.

If the file loading is the bottleneck of the task, we can use threads in place of processes.


Reading from a Biglist in multiple machines
-------------------------------------------

A similar need is to collectively read the data from multiple machines in a distributed settting.
For this purpose, the data must be stored in the cloud.
(Currently `upathlib <https://github.com/zpz/upathlib>`_ works with Google Cloud Storage, hence ``biglist`` works with GCS as well.)
Because the ``biglist`` API is agnostic to the location of storage, we'll use the locally-stored
dataset and multiprocessing to emulate the behavior of distributed reading.

First, start a new "concurrent file iter" task::

   >>> task_id = mylist.new_concurrent_file_iter()
   >>> task_id
   '2022-11-28T21:59:49.709094'

This creates a directory in the data folder to save whatever "meta" or "control" info the algorithm needs.
The returned ``task_id`` identifies this particular task.
There could be multiple such tasks going on at the same time independently, provided the dataset stays unchanged.
Each task will have its own task ID.

Next, create a worker function to run in another process::

   >>> def worker(path, task_id):
   ...     data = Biglist(path)
   ...     total = 0
   ...     for batch in data.concurrent_iter_files(task_id):
   ...         total += sum(batch)
   ...     return total

Each ``batch`` above is a :class:`FileReader` object, which implements the |Sequence|_ and |Iterable|_ APIs.

Back in the main process::

   >>> mylist.path
   LocalUpath('/tmp/19f88a17-3e78-430f-aad0-a35d39485f80')
   >>>
   >>> total = 0
   >>> with ProcessPoolExecutor(5) as pool:
   ...     tasks = [pool.submit(worker, mylist.path, task_id) for _ in range(5)]
   ...     for t in tasks:
   ...         total += t.result()
   >>> total
   50225253

The mechanisms of :meth:`~biglist._base.BiglistBase.file_readers` and :meth:`~biglist._base.BiglistBase.concurrent_iter_files` are different.
While the former deals with "file-wise" data batches explicitly listed upfront,
the latter makes a request to a "coordinator" for the next file to consume, one file at a time.
The coordinator acts as a server to handle such requests from any worker.

:meth:`~biglist._base.BiglistBase.concurrent_iter_files` can be used on a local machine just fine,
but it has some overhead incurred by "file locks" (the coordinator locks a book-keeping file when
serving requests from workers).
On the other hand, :meth:`~biglist._base.BiglistBase.file_readers` can't be used in a distributed setup.


Using Biglist as a "multiplexer"
--------------------------------

The above distributes *files* to workers.
This is efficient for processing large amounts of data.

There are cases where we want to distribute individual *elements* to workers.
Suppose we perform some brute-force search on a cluster of machines;
there are 1000 grids, and the algorithm takes on one grid at a time.
Now, the grid is a "hyper-parameter" or "control parameter" that takes 1000 possible values.
We want to distribute these 1000 values to the workers.
How can we do that?

Simple. Just put the 1000 values in a :class:`Biglist` with ``batch_size=1`` and use
:meth:`~biglist._base.BiglistBase.concurrent_iter_files`. Problem solved.

Except it's not as efficient as it can be.
For one thing, creating the 1000 (tiny) files in the cloud takes a while.
:class:`Biglist` provides "multiplexer" methods to do this job more efficiently.
First, it uses a "regular" ``batch_size``, say 1000, so that it may need to create only one or a few
data files.
Second, compared to :meth:`~biglist._base.BiglistBase.concurrent_iter_files`,
each call to :meth:`~Biglist.multiplex_iter` makes one fewer trip to the cloud.

Let's show it using local data and multiprocessing.
(For real work, we would use cloud storage and a cluster of machines.)
First, create a :class:`Biglist` to hold the values to be distributed::

   >>> hyper = Biglist.new(batch_size=1000)
   >>> hyper.extend(range(20))
   >>> hyper.flush()
   >>>
   >>> hyper.num_datafiles
   1
   >>> len(hyper)
   20

Next, design an interesting worker function::

   >>> import multiprocessing, random, time
   >>>
   >>> def worker(path, task_id):
   ...     hyper = Biglist(path)
   ...     for x in hyper.multiplex_iter(task_id):
   ...         time.sleep(random.uniform(0.1, 0.2))  # doing a lot of things
   ...         print(x, 'done in', multiprocessing.current_process().name)

Back in the main process,

::

   >>> task_id = hyper.new_multiplexer()
   >>> tasks = [multiprocessing.Process(target=worker, args=(hyper.path, task_id)) for _ in range(5)]
   >>> for t in tasks:
   ...     t.start()
   >>> 2 done in Process-13
   0 done in Process-11
   1 done in Process-12
   4 done in Process-15
   3 done in Process-14
   6 done in Process-11
   7 done in Process-12
   8 done in Process-15
   5 done in Process-13
   9 done in Process-14
   12 done in Process-15
   13 done in Process-13
   11 done in Process-12
   10 done in Process-11
   14 done in Process-14
   15 done in Process-15
   18 done in Process-11
   16 done in Process-13
   17 done in Process-12
   19 done in Process-14
   >>>
   >>> for t in tasks:
   ...     t.join()


Writing to a Biglist in multiple workers
========================================

The flip side of distributed reading is distributed writing, and that is covered as well.
If we have a biglist on the local disk, we can append to it from multiple processes or threads.
If we have a biglist in the cloud, we can append to it from multiple machines.
Let's use multiprocessing to demo the idea.

First, we create a new :class:`Biglist` at a storage location of our choosing::

   >>> from upathlib import LocalUpath
   >>> path = LocalUpath('/tmp/a/b/c/d')
   >>> path.rmrf()
   0
   >>> yourlist = Biglist.new(path, batch_size=6)

Now that this has a presence on the disk, ``Biglist(path)`` will not complain the dataset does not exist.

::

   >>> yourlist.info
   {'batch_size': 6, 'storage_format': 'pickle-zstd', 'storage_version': 1}
   >>> yourlist.path
   LocalUpath('/tmp/a/b/c/d')
   >>> len(yourlist)
   0

Then we can tell workers, "here is the location, add data to it." Let's design a simple worker::

   >>> def worker(path, idx):
   ...     yourlist = Biglist(path)
   ...     for i in range(idx):
   ...         yourlist.append(100 * idx + i)
   ...     yourlist.flush()

From the main process, let's instruct the workers to write data to the same :class:`Biglist`::

   >>> with ProcessPoolExecutor(10) as pool:
   ...     tasks = [pool.submit(worker, path, idx) for idx in range(10)]
   ...     for t in tasks:
   ...         _ = t.result()

Let's see what we've got::

   >>> len(yourlist)
   45
   >>> yourlist.num_datafiles
   12
   >>> list(yourlist)
   [100, 200, 201, 300, 301, 302, 400, 401, 402, 403, 500, 501, 502, 503, 504, 600, 601, 602, 603, 604, 605, 800, 801, 802, 803, 804, 805, 700, 701, 702, 703, 704, 705, 900, 901, 902, 903, 904, 905, 806, 807, 906, 907, 908, 706]

Does this look right? It took me a moment to realize that ``idx = 0`` did not append anything.
So, the data elements are in 100, 200, ..., 900 ranges; that looks right.
But the order of things is confusing.

Well, in a distributed setting, there's no guarantee of order. It's not a problem that numbers in the 700 range come *after* those in the 800 range.
Then we notice that 800, 801, 802, 803, 804, 805 are not followed by 806, 807---some 700 and 900 numbers cut in line!
That's because of the ``batch_size=6``: the 800 numbers are split in two files, and there is no guaranteed order among files
written by different workers.

We can get more insights if we dive to the file level::

   >>> for f in yourlist.datafiles:
   ...     print(f)
   /tmp/a/b/c/d/store/1669677904.1907494_92e0347b-0cb8-4a28-b4c4-4355bb6e5832_1.pickle_zstd
   /tmp/a/b/c/d/store/1669677904.1935146_d4a22b4a-e693-4cc5-a139-ed930df04579_2.pickle_zstd
   /tmp/a/b/c/d/store/1669677904.1959705_466bca01-78a0-4025-a4f4-5c98f7d11be0_3.pickle_zstd
   /tmp/a/b/c/d/store/1669677904.1993327_eaa0e122-b646-4fe1-ba1c-0cbb0ea777e7_4.pickle_zstd
   /tmp/a/b/c/d/store/1669677904.201901_eb18dd7f-406d-4a46-a9a2-03d441b0a263_5.pickle_zstd
   /tmp/a/b/c/d/store/1669677904.2046561_2b450fa4-3c61-4d11-99d6-e4b0af4af3b6_6.pickle_zstd
   /tmp/a/b/c/d/store/1669677904.2135856_0b312288-8906-4d73-8a27-c536ab65505e_6.pickle_zstd
   /tmp/a/b/c/d/store/1669677904.2151945_79150077-e8b4-44db-9368-7ca71a219c10_6.pickle_zstd
   /tmp/a/b/c/d/store/1669677904.217562_baaa6787-9bd4-4b0b-ab2e-f866e1bc4399_6.pickle_zstd
   /tmp/a/b/c/d/store/1669677904.2293315_9203aca8-d9ff-44d4-94c1-96730e82ba22_2.pickle_zstd
   /tmp/a/b/c/d/store/1669677904.2323425_b6abc131-91ce-4be8-8e4f-70a108231992_3.pickle_zstd
   /tmp/a/b/c/d/store/1669677904.2333698_b6bd2183-cdba-45b8-8c42-dccb4c6eedd7_1.pickle_zstd

The file names do not appear to be totally random. They follow some pattern that facilitates ordering, and they have encoded some useful info.
In fact, the part before the first underscore is the epoch timestamp of file creation.
When we iter the :class:`Biglist` object, files are read in the order of their paths, hence in the order of creation time.
The number in the file name before the suffix is the number of elements in the file.

We can get similar info in a more readable format::

   >>> for f in yourlist.datafiles_info:
   ...     print(f)
   ('/tmp/a/b/c/d/store/1669677904.1907494_92e0347b-0cb8-4a28-b4c4-4355bb6e5832_1.pickle_zstd', 1, 1)
   ('/tmp/a/b/c/d/store/1669677904.1935146_d4a22b4a-e693-4cc5-a139-ed930df04579_2.pickle_zstd', 2, 3)
   ('/tmp/a/b/c/d/store/1669677904.1959705_466bca01-78a0-4025-a4f4-5c98f7d11be0_3.pickle_zstd', 3, 6)
   ('/tmp/a/b/c/d/store/1669677904.1993327_eaa0e122-b646-4fe1-ba1c-0cbb0ea777e7_4.pickle_zstd', 4, 10)
   ('/tmp/a/b/c/d/store/1669677904.201901_eb18dd7f-406d-4a46-a9a2-03d441b0a263_5.pickle_zstd', 5, 15)
   ('/tmp/a/b/c/d/store/1669677904.2046561_2b450fa4-3c61-4d11-99d6-e4b0af4af3b6_6.pickle_zstd', 6, 21)
   ('/tmp/a/b/c/d/store/1669677904.2135856_0b312288-8906-4d73-8a27-c536ab65505e_6.pickle_zstd', 6, 27)
   ('/tmp/a/b/c/d/store/1669677904.2151945_79150077-e8b4-44db-9368-7ca71a219c10_6.pickle_zstd', 6, 33)
   ('/tmp/a/b/c/d/store/1669677904.217562_baaa6787-9bd4-4b0b-ab2e-f866e1bc4399_6.pickle_zstd', 6, 39)
   ('/tmp/a/b/c/d/store/1669677904.2293315_9203aca8-d9ff-44d4-94c1-96730e82ba22_2.pickle_zstd', 2, 41)
   ('/tmp/a/b/c/d/store/1669677904.2323425_b6abc131-91ce-4be8-8e4f-70a108231992_3.pickle_zstd', 3, 44)
   ('/tmp/a/b/c/d/store/1669677904.2333698_b6bd2183-cdba-45b8-8c42-dccb4c6eedd7_1.pickle_zstd', 1, 45)

The values for each entry are file path, number of elements in the file, and accumulative number of elements.
The accumulative count is obviously the basis for random access---:class:`Biglist` uses this to
figure out which file contains the element at a specified index.



Creating a ParquetBiglist
=========================


Apache Parquet is a popular file format in the "big data" domain.
Many tools save large amounts of data in this format, often in a large number of files,
sometimes in nested directories.

:class:`ParquetBiglist` takes such data files as pre-existing, read-only, external data,
and provides an API to read the data in various ways.
This is analogous to, for example, the "external table" concept in BigQuery.

Let's create a couple small Parquet files to demonstrate this API.

::

   >>> from upathlib import LocalUpath
   >>> import random
   >>> from biglist import write_parquet_file
   >>>
   >>> path = LocalUpath('/tmp/a/b/c/e')
   >>> path.rmrf()
   0
   >>
   >>> year = list(range(1970, 2021))
   >>> make = ['honda'] * len(year)
   >>> sales = [random.randint(100, 300) for _ in range(len(year))]
   >>> write_parquet_file(path / 'honda.parquet', [make, year, sales], names=['make', 'year', 'sales'], row_group_size=10)
   >>>
   >>> year = list(range(1960, 2021))
   >>> make = ['ford'] * len(year)
   >>> sales = [random.randint(50, 500) for _ in range(len(year))]
   >>> write_parquet_file(path / 'ford.parquet', [make, year, sales], names=['make', 'year', 'sales'], row_group_size=10)

Now we want to treat the contents of ``honda.parquet`` and ``ford.parquet`` combined as one dataset, and
use ``biglist`` tools to read it.

::

   >>> from biglist import ParquetBiglist
   >>> car_data = ParquetBiglist.new(path)
   >>> car_data
   <ParquetBiglist at '/tmp/edd9cefb-179b-46d2-8946-7dc8ae1bdc50' with 112 records in 2 data file(s) stored at ['/tmp/a/b/c/e']>
   >>> car_data.path
   LocalUpath('/tmp/edd9cefb-179b-46d2-8946-7dc8ae1bdc50')
   >>> len(car_data)
   112
   >>> car_data.num_datafiles
   2
   >>> car_data.datafiles
   ['/tmp/a/b/c/e/ford.parquet', '/tmp/a/b/c/e/honda.parquet']

what :meth:`ParquetBiglist.new` does is to read the meta data of each file in the directory, recursively,
and save relevant info to facilitate its reading later.
The location given by ``car_data.path`` is the directory where :class:`ParquetBiglist` saves its meta info,
and not where the actual data are.
As is the case with :class:`Biglist`, this directory is a temporary one, which will be deleted once the object
``car_data`` goes away. If we wanted to keep the directory for future use, we should have specified a location
when calling :meth:`ParquetBiglist.new`.


Reading a ParquetBiglist
========================

The fundamental reading API is the same between :class:`Biglist` and :class:`ParquetBiglist`:
random access, slicing/dicing via :meth:`~biglist._base.BiglistBase.view`, iteration, concurrent same-machine reading
via :meth:`~biglist._base.BiglistBase.file_readers`, distributed reading via :meth:`~_base.BiglistBase.concurrent_iter_files`---these are all used the same way.

However, the structures of the data files are very different between :class:`Biglist` and :class:`ParquetBiglist`.
For Biglist, each data file contains a straight Python list, elements of which being whatever have been
passed into :meth:`Biglist.append`.
For ParquetBiglist, each data file is in a sophisticated columnar format, which is publicly documented.
A variety of ways are provided to get data out of the Parquet format;
some favor convenience, some others favor efficiency. Let's see some examples.

A row perspective
-----------------

::

   >>> for i, x in enumerate(car_data):
   ...     print(x)
   ...     if i > 5:
   ...         break
   {'make': 'ford', 'year': 1960, 'sales': 78}
   {'make': 'ford', 'year': 1961, 'sales': 50}
   {'make': 'ford', 'year': 1962, 'sales': 311}
   {'make': 'ford', 'year': 1963, 'sales': 249}
   {'make': 'ford', 'year': 1964, 'sales': 249}
   {'make': 'ford', 'year': 1965, 'sales': 167}
   {'make': 'ford', 'year': 1966, 'sales': 170}

This is the most basic iteration, :class:`Biglist`-style, one row (or "record") at a time.
When there are multiple columns, each row is presented as a dict with column names as keys.

Remember that ``car_data.__iter__`` is just further iteration on top of ``car_data.iter_files``,
which yields ParquetFileReader objects.
:class:`ParquetFileReader`, a subclass of :class:`FileReader`, is the fundamental unit for Parquet data reading.

::

   >>> f0 = car_data.file_reader(0)
   >>> f0
   ParquetFileReader('/tmp/a/b/c/e/ford.parquet', <bound method ParquetBiglist.load_data_file of <class 'biglist._parquet.ParquetBiglist'>>)
   >>> f0.path
   LocalUpath('/tmp/a/b/c/e/ford.parquet')

First of all, a :class:`FileReader` object is a |Sequence|_, providing row-based view into the data::

   >>> len(f0)
   61
   >>> f0[2]
   {'make': 'ford', 'year': 1962, 'sales': 311}
   >>> f0[-10]
   {'make': 'ford', 'year': 2011, 'sales': 116}
   >>> f0.view()[-3:].collect()
   [{'make': 'ford', 'year': 2018, 'sales': 248}, {'make': 'ford', 'year': 2019, 'sales': 354}, {'make': 'ford', 'year': 2020, 'sales': 216}]
   >>> for i, x in enumerate(f0):
   ...     print(x)
   ...     if i > 5:
   ...         break
   {'make': 'ford', 'year': 1960, 'sales': 78}
   {'make': 'ford', 'year': 1961, 'sales': 50}
   {'make': 'ford', 'year': 1962, 'sales': 311}
   {'make': 'ford', 'year': 1963, 'sales': 249}
   {'make': 'ford', 'year': 1964, 'sales': 249}
   {'make': 'ford', 'year': 1965, 'sales': 167}
   {'make': 'ford', 'year': 1966, 'sales': 170}

:class:`ParquetFileReader` uses `pyarrow`_ to read the Parquet files.
The values above are nice and simple Python types, but they are not the original
pyarrow types;
they have undergone a conversion. This conversion can be toggled by the property
:data:`ParquetFileReader.scalar_as_py`::

   >>> f0[8]
   {'make': 'ford', 'year': 1968, 'sales': 381}
   >>> f0.scalar_as_py = False
   >>> f0[8]
   {'make': <pyarrow.StringScalar: 'ford'>, 'year': <pyarrow.Int64Scalar: 1968>, 'sales': <pyarrow.Int64Scalar: 381>}
   >>> f0.scalar_as_py = True

A Parquet file consists of one or more "row groups". Each row-group is a batch of rows stored column-wise.
We can get info about the row-groups, or even retrieve a row-group as the unit of processing::

   >>> f0.num_row_groups
   7
   >>> f0.metadata
   <pyarrow._parquet.FileMetaData object at 0x7faa8c0bb2c0>
   created_by: parquet-cpp-arrow version 10.0.1
   num_columns: 3
   num_rows: 61
   num_row_groups: 7
   format_version: 2.6
   serialized_size: 2375
   >>> f0.metadata.row_group(1)
   <pyarrow._parquet.RowGroupMetaData object at 0x7faa7fae5400>
   num_columns: 3
   num_rows: 10
   total_byte_size: 408
   >>> f0.metadata.row_group(0)
   <pyarrow._parquet.RowGroupMetaData object at 0x7faa6f1ec1d0>
   num_columns: 3
   num_rows: 10
   total_byte_size: 400
   >>> rg = f0.row_group(0)
   >>> rg
   <ParquetBatchData with 10 rows, 3 columns>

(We have specified ``row_group_size=10`` in the call to :func:`write_parquet_file` for demonstration.
In practice, a row-group tends to be much larger.)

A :class:`ParquetBatchData` object is again a |Sequence|_.
All our row access tools are available::

   >>> rg.num_rows
   10
   >>> len(rg)
   10
   >>> rg.num_columns
   3
   >>> rg[3]
   {'make': 'ford', 'year': 1963, 'sales': 249}
   >>> rg[-2]
   {'make': 'ford', 'year': 1968, 'sales': 381}
   >>> rg.view()[4:7].collect()
   [{'make': 'ford', 'year': 1964, 'sales': 249}, {'make': 'ford', 'year': 1965, 'sales': 167}, {'make': 'ford', 'year': 1966, 'sales': 170}]
   >>> rg.scalar_as_py = False
   >>> rg[3]
   {'make': <pyarrow.StringScalar: 'ford'>, 'year': <pyarrow.Int64Scalar: 1963>, 'sales': <pyarrow.Int64Scalar: 249>}
   >>> rg.scalar_as_py = True

When we request a specific row, :class:`ParquetFileReader` will load the row-group that contains the row of interest.
It doe not load the entire data in the file.
However, we can get greedy and ask for the whole data in one go::

   >>> f0
   ParquetFileReader('/tmp/a/b/c/e/ford.parquet', <bound method ParquetBiglist.load_data_file of <class 'biglist._parquet.ParquetBiglist'>>)
   >>> f0.data()
   <ParquetBatchData with 61 rows, 3 columns>

This, again, is a :class:`ParquetBatchData` object. All the familiar row access tools are at our disposal.

Finally, if the file is large, we may choose to iterate over it by batches instead of by rows::

   >>> for batch in f0.iter_batches(batch_size=10):
   ...     print(batch)
   <ParquetBatchData with 10 rows, 3 columns>
   <ParquetBatchData with 10 rows, 3 columns>
   <ParquetBatchData with 10 rows, 3 columns>
   <ParquetBatchData with 10 rows, 3 columns>
   <ParquetBatchData with 10 rows, 3 columns>
   <ParquetBatchData with 10 rows, 3 columns>
   <ParquetBatchData with 1 rows, 3 columns>

The batches are again :class:`ParquetBatchData` objects.
At the core of a ParquetBatchData is
a `pyarrow.Table`_
or `pyarrow.RecordBatch`_.
ParquetBatchData is friendly to `pickle <https://docs.python.org/3/library/pickle.html>`_ and,
I suppose, pickling `pyarrow`_ objects are very efficient.
So, the batches could be useful in `multiprocessing <https://docs.python.org/3/library/multiprocessing.html>`_ code.

A column perspective
--------------------

Parquet is a *columnar* format.
If we only need a subset of the columns, we should say so, so that the un-needed columns will
not be loaded from disk (or cloud, as it may be).

Both :class:`ParquetFileReader` and :class:`ParquetBatchData` provide the method ``columns`` to return a new object
with only the selected columns.
For ParquetFileReader, if data have not been loaded, reading of the new object will only load the selected columns.
For ParquetBatchData, its data is already in memory, hence column selection leads to a data subset.

   >>> f0.column_names
   ['make', 'year', 'sales']
   >>> cols = f0.columns(['year', 'sales'])
   >>> cols
   ParquetFileReader('/tmp/a/b/c/e/ford.parquet', <bound method ParquetBiglist.load_data_file of <class 'biglist._parquet.ParquetBiglist'>>)
   >>> cols.num_columns
   2
   >>> cols.column_names
   ['year', 'sales']

:meth:`ParquetFileReader.columns` returns another :class:`ParquetFileReader`, whereas
:meth:`ParquetBatchData.columns` returns another :class:`ParquetBatchData`::

   >>> rg
   <ParquetBatchData with 10 rows, 3 columns>
   >>> rg.column_names
   ['make', 'year', 'sales']
   >>> rgcols = rg.columns(['make', 'year'])
   >>> rgcols.column_names
   ['make', 'year']
   >>> len(rgcols)
   10
   >>> rgcols[5]
   {'make': 'ford', 'year': 1965}

It's an interesting case when there's only one column::

   >>> f0
   ParquetFileReader('/tmp/a/b/c/e/ford.parquet', <bound method ParquetBiglist.load_data_file of <class 'biglist._parquet.ParquetBiglist'>>)
   >>> sales = f0.columns(['sales'])
   >>> sales
   ParquetFileReader('/tmp/a/b/c/e/ford.parquet', <bound method ParquetBiglist.load_data_file of <class 'biglist._parquet.ParquetBiglist'>>)
   >>> sales.column_names
   ['sales']
   >>> len(sales)
   61
   >>> sales[3]
   249
   >>> list(sales)
   [78, 50, 311, 249, 249, 167, 170, 410, 381, 456, 106, 140, 104, 87, 127, 385, 443, 381, 294, 403, 74, 495, 97, 341, 276, 364, 421, 93, 378, 256, 352, 464, 413, 192, 436, 500, 103, 489, 197, 386, 454, 409, 450, 325, 484, 361, 201, 88, 446, 433, 475, 116, 388, 427, 275, 216, 332, 168, 248, 354, 216]
   >>> sales.view()[:8].collect()
   [78, 50, 311, 249, 249, 167, 170, 410]

Notice the type of the values (rows) returned from the element access methods: it's *not* ``dict``.
Because there's only one column whose name is known, there is no need to carry that info with every row.
Also note that the values have been converted to Python builtin types.
The original `pyarrow`_ values will not look as nice::
   
   >>> sales.scalar_as_py = False
   >>> sales.view()[:8].collect()
   [<pyarrow.Int64Scalar: 78>, <pyarrow.Int64Scalar: 50>, <pyarrow.Int64Scalar: 311>, <pyarrow.Int64Scalar: 249>, <pyarrow.Int64Scalar: 249>, <pyarrow.Int64Scalar: 167>, <pyarrow.Int64Scalar: 170>, <pyarrow.Int64Scalar: 410>]
   >>> sales.scalar_as_py = True

Both :class:`ParquetFileReader` and :class:`ParquetBatchData` have another method called ``column``, which retrieves a single column
and returns a
`pyarrow.Array`_ or
`pyarrow.ChunkedArray`_. For example,

::

   >>> sales2 = f0.column('sales')
   >>> sales2
   <pyarrow.lib.ChunkedArray object at 0x7faa6e354270>
   [
   [
      78,
      50,
      311,
      249,
      249,
      ...
      332,
      168,
      248,
      354,
      216
   ]
   ]

:meth:`ParquetFileReader.column` returns a 
`pyarrow.ChunkedArray`_, whereas
:meth:`ParquetBatchData.column` returns either a 
pyarrow.ChunkedArray or a 
`pyarrow.Array`_.


Performance considerations
--------------------------

While some ``biglist`` facilities shown here provide convenience and API elegance,
it may be a safe bet to use `pyarrow`_ facilities directly if ultimate performance is a requirement.

We have seen :data:`ParquetFileReader.scalar_as_py`
(and :data:`ParquetBatchData.scalar_as_py`); it's worthwhile to experiment whether that conversion impacts performance in a particular context.

There are several ways to get to a `pyarrow`_ object quickly and proceed with it.
A newly initiated :class:`ParquetFileReader` has not loaded any data yet.
Its :data:`ParquetFileReader.file` property initiates a 
`pyarrow.parquet.ParquetFile`_ object (reading meta data during initiation)
and returns it. We may take it and go all the way down the `pyarrow`_ path::

   >>> f1 = car_data.file_reader(1)
   >>> f1._data is None
   True
   >>> f1._file is None
   True
   >>> file = f1.file
   >>> file
   <pyarrow.parquet.core.ParquetFile object at 0x7f04e24e53a0>
   >>> f1._file
   <pyarrow.parquet.core.ParquetFile object at 0x7f04e24e53a0>

We have seen that :meth:`ParquetFileReader.row_group` and :meth:`ParquetFileReader.iter_batches` both
return :class:`ParquetBatchData` objects. In contrast to :class:`ParquetFileReader`, which is "lazy" in terms of data loading,
a ParquetBatchData already has its data in memory. ParquetFileReader has another method,
namely :meth:`ParquetFileReader.data`, that
eagerly loads the entire data of the file and wraps it in a ParquetBatchData object::

   >>> data = f1.data()
   >>> data
   <ParquetBatchData with 51 rows, 3 columns>

The `pyarrow`_ data wrapped in :class:`ParquetBatchData` can be acquired easily::

   >>> padata = data.data()
   >>> padata
   pyarrow.Table
   make: string
   year: int64
   sales: int64
   ----
   make: [["honda","honda","honda","honda","honda",...,"honda","honda","honda","honda","honda"]]
   year: [[1970,1971,1972,1973,1974,...,2016,2017,2018,2019,2020]]
   sales: [[125,257,186,243,206,...,136,247,187,128,209]]

Finally, we have seen that :meth:`ParquetFileReader.column` and :meth:`ParquetBatchData.column`---the single-column selectors---return
a `pyarrow`_ object. It is either a 
`pyarrow.Array`_ or a 
`pyarrow.ChunkedArray`_.



Reading a standalone Parquet file
=================================

The function :func:`read_parquet_file` is provided to read a single Parquet file independent of
:class:`ParquetBiglist`. It returns a :class:`ParquetFileReader`. All the facilities of this class,
as demonstrated above, are ready for use::

   >>> car_data.datafiles
   ['/tmp/a/b/c/e/ford.parquet', '/tmp/a/b/c/e/honda.parquet']
   >>>
   >>> from biglist import read_parquet_file
   >>> ff = read_parquet_file(car_data.datafiles[1])
   >>> ff
   ParquetFileReader('/tmp/a/b/c/e/honda.parquet', <bound method ParquetBiglist.load_data_file of <class 'biglist._parquet.ParquetBiglist'>>)
   >>> len(ff)
   51
   >>> ff.column_names
   ['make', 'year', 'sales']
   >>> ff[3]
   {'make': 'honda', 'year': 1973, 'sales': 243}
   >>> ff.columns(['year', 'sales']).view()[10:16].collect()
   [{'year': 1980, 'sales': 136}, {'year': 1981, 'sales': 292}, {'year': 1982, 'sales': 200}, {'year': 1983, 'sales': 199}, {'year': 1984, 'sales': 214}, {'year': 1985, 'sales': 125}]
   >>> ff.num_row_groups
   6
   >>> ff.row_group(3).column('sales')
   <pyarrow.lib.ChunkedArray object at 0x7faa6c166270>
   [
   [
      189,
      168,
      147,
      277,
      292,
      235,
      203,
      200,
      137,
      150
   ]
   ]


Other utilities
===============

:class:`ChainedList` takes a series of |Sequence|_\s and returns a combined Sequence without data copy.
For example,

::

   >>> from biglist import ChainedList
   >>> numbers = list(range(10))
   >>> car_data
   <ParquetBiglist at '/tmp/edd9cefb-179b-46d2-8946-7dc8ae1bdc50' with 112 records in 2 data file(s) stored at ['/tmp/a/b/c/e']>
   >>> combined = ChainedList(numbers, car_data)
   >>> combined[3]
   3
   >>> combined[9]
   9
   >>> combined[10]
   {'make': 'ford', 'year': 1960, 'sales': 78}
   >>>
   >>> car_data[0]
   {'make': 'ford', 'year': 1960, 'sales': 78}

:class:`ListView` takes any |Sequence|_ and provides :meth:`~ListView.__getitem__` that accepts
a single index, or a slice, or a list of indices. A single-index access will return
the requested element; the other two scenarios return a new ListView via a zero-copy operation.
To get all the elements out of a ListView, either iterate it or call its method :meth:`~ListView.collect`.

:class:`~_base.BiglistBase` (including :class:`Biglist` and :class:`ParquetBiglist`),
:class:`FileReader` (including :class:`BiglistFileReader` and :class:`ParquetFileReader`),
:class:`ParquetBatchData`, and :class:`ChainedList` all have a method ``view``, which returns
a :class:`ListView` to give them slicing capabilities. All these ``view`` methods are implemented
by the one-liner

::

   def view(self):
      return ListView(self)

because, after all, this ``self`` is a |Sequence|_.

We should emphasize that :class:`ChainedList` and :class:`ListView` work with any |Sequence|_,
hence they are useful independent of the other ``biglist`` classes.


API reference
=============


.. autodata:: biglist._base.Element


.. autodata:: biglist._base.SequenceType


.. autoclass:: biglist.ListView


.. autoclass:: biglist.ChainedList


.. autoclass:: biglist.FileReader


.. autoclass:: biglist._base.BiglistBase


.. autoclass:: biglist.Biglist


.. autoclass:: biglist.BiglistFileReader


.. autoclass:: biglist.ParquetBiglist


.. autoclass:: biglist.ParquetFileReader


.. autoclass:: biglist.ParquetBatchData


.. autofunction:: biglist.read_parquet_file


.. autofunction:: biglist.write_parquet_file


Indices and tables
==================

* :ref:`genindex`
* :ref:`search`
