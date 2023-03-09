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

.. testsetup:: *

   from biglist import *
   from upathlib import LocalUpath

.. testcleanup::

   LocalUpath('/tmp/a/b/c').rmrf()


=======
biglist
=======

(Generated on |today| for biglist version |version|.)


.. automodule:: biglist


Installation
============

To install ``biglist`` and use local disk for persistence, simply do 

::

   pip install biglist

If you need to use Google Cloud Storage for persistence, do

::

   pip install "biglist[gcs]"


Creating a Biglist
==================

Create a new :class:`Biglist` object via the classmethod :meth:`~Biglist.new`:

>>> from biglist import Biglist
>>> mylist = Biglist.new(batch_size=100)

then add data to it, for example,

>>> for x in range(10_023):
...     mylist.append(x)

This saves a new data file for every 100 elements
accumulated. In the end, there are 23 elements in a memory buffer that are
not yet persisted to disk. The code has no way to know whether we will append
more elements soon, hence it does not save this *partial* batch.
Suppose we're done with adding data, we call :meth:`~Biglist.flush` to persist
the content of the buffer to disk:

>>> mylist.flush()

If, after a while, we decide to append more data to ``mylist``, we just call :meth:`~Biglist.append` again.
We can continue to add more data as long as the disk has space.
New data files will be saved. The smaller file containing 23 elements will stay there
among larger files with no problem.

Now let's take a look at the biglist object:

>>> mylist  # doctest: +SKIP
<Biglist at '/tmp/19f88a17-3e78-430f-aad0-a35d39485f80' with 10023 elements in 101 data file(s)>
>>> len(mylist)
10023
>>> mylist.path  # doctest: +SKIP
LocalUpath('/tmp/19f88a17-3e78-430f-aad0-a35d39485f80')
>>> mylist.num_data_files
101

The data have been saved in the directory ``/tmp/19f88a17-3e78-430f-aad0-a35d39485f80``,
which is a temporary one because we did not tell :meth:`~Biglist.new` where to save data.
When the object ``mylist`` gets garbage collected, this directory will be deleted automatically.
This has its uses, but often we want to save the data for future use. In that case, just pass 
a currently non-existent directory to :meth:`~Biglist.new`, for example,

>>> yourlist = Biglist.new('/tmp/project/data/store-a', batch_size=10_000)

Later, initiate a :class:`Biglist` object for reading the existing dataset:

>>> yourlist = Biglist('/tmp/project/data/store-a')

.. doctest::
   :hide:

   >>> yourlist.destroy()

If we want to persist the data in Google Cloud Storage, we would specify a path in the
``'gs://bucket-name/path/to/data'`` format.


The Seq protocol and FileReader class
=====================================

Before going further with Biglist, let's digress a bit and introduce a few helper facilities.

:class:`BiglistBase`
(and its subclasses :class:`Biglist` and :class:`ParquetBiglist`)
could have implemented the |Sequence|_ interface in the standard library.
However, that interface contains a few methods that are potentially hugely inefficient for Biglist,
and hence are not supposed to be used on a Biglist.
These methods include ``__contains__``, ``count``, and ``index``.
These methods require iterating over the entire dataset for purposes about one particular data item.
For Biglist, this would require loading and unloading each of a possibly large number of data files.
Biglist does not want to give user the illusion that they can use these methods at will and lightly.

For this reason, the protocol :class:`Seq` is defined, which has three methods:
``__len__``, ``__iter__``, and ``__getitem__``.
Therefore, classes that implement this protocol are
`Sized <https://docs.python.org/3/library/collections.abc.html#collections.abc.Sized>`_,
`Iterable <https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable>`_,
and support random element access by index. Most classes in the ``biglist`` package
implement this protocol rather than the standard |Sequence|_.

Because a biglist manages any number of data files, a basic operation concerns reading one data file.
Each subclass of :class:`BiglistBase` implements its file-reading class as a subclass of
:class:`FileReader`. FileReader implements the :class:`Seq` protocol, hence
the data items in one data file can be used like a list.
Importantly, a FileReader instance does not load the data file upon initialization.
At that moment, the instance can be *pickled*. This lends itself to uses in multiprocessing.
This point of the design will be showcased later.

The centerpiece of a biglist is a sequence of data files in persistence, or correspondingly,
a sequence of FileReader's in memory. The property :meth:`BiglistBase.files` of BiglistBase
returns a :class:`FileSeq` to manage the FileReader objects of the biglist.
Besides implementing the :class:`Seq` protocol, FileSeq provides ways to use the FileReader's
by distributed workers.

Finally, :class:`BiglistBase`
implements the :class:`Seq` protocol for its data items across the data files.

To sum up,
a BiglistBase is a Seq of data items across data files;
BiglistBase.files is a FileSeq, which in turn is a Seq of FileReaders;
a FileReader is a Seq of data items in one data file.



Reading a Biglist
=================

Random element access
---------------------

We can access any element of a :class:`Biglist` like we do a list:

>>> mylist[18]
18
>>> mylist[-3]
10020

Biglist does not support slicing directly.
However, the class :class:`Slicer` wraps a :class:`Seq` and enables element access by a single index, by a slice, or by a list of indices:

>>> from biglist import Slicer
>>> v = Slicer(mylist)
>>> len(v)
10023
>>> v  # doctest: +SKIP
<Slicer into 10023/10023 of <Biglist at '/tmp/dc260854-8041-40e8-801c-34084451d7a3' with 10023 elements in 101 data file(s)>>
>>> v[83]
83
>>> v[100:104]  # doctest: +SKIP
<Slicer into 4/10023 of <Biglist at '/tmp/dc260854-8041-40e8-801c-34084451d7a3' with 10023 elements in 101 data file(s)>>
>>>

Note that slicing the slicer does not return a list of values.
Instead, it returns another :class:`Slicer` object, which, naturally, can be used the same way,
including slicing further.

A :class:`Slicer` object is a |Iterable|_ (in fact, it is a :class:`Seq`),
hence we can gather all of its elements in a list:

>>> list(v[100:104])
[100, 101, 102, 103]

:class:`Slicer` provides a convenience method :meth:`~Slicer.collect` to do the same:

>>> v[100:104].collect()
[100, 101, 102, 103]


A few more examples:

>>> v[-8:].collect()
[10015, 10016, 10017, 10018, 10019, 10020, 10021, 10022]
>>> v[-8::2].collect()
[10015, 10017, 10019, 10021]
>>> v[[1, 83, 250, -2]]  # doctest: +SKIP
<Slicer into 4/10023 of <Biglist at '/tmp/dc260854-8041-40e8-801c-34084451d7a3' with 10023 elements in 101 data file(s)>>
>>> v[[1, 83, 250, -2]].collect()
[1, 83, 250, 10021]
>>> v[[1, 83, 250, -2]][-3:].collect()
[83, 250, 10021]


Iteration
---------

Don't be carried away by the many easy and flexible ways of random access.
Random element access for :class:`Biglist` is **inefficient**.
The reason is that it needs to load a data file that contains the element of interest.
If the biglist has many data files and we are "jumping around" randomly,
it is wasting a lot of time loading entire files just to access a few data elements in them.
(However, *consecutive* random accesses to elements residing in the same file will not load the file
repeatedly.)

The preferred way to consume the data of a :class:`Biglist` is to iterate over it. For example,

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

Conceptually, this loads each data file in turn and yields the elements in each file.
The implementation "pre-loads" a few files in background threads to speed up the iteration.


Reading from a Biglist in multiple processes
--------------------------------------------

To *collectively* consume a :class:`Biglist` object from multiple processes,
we can distribute :class:`FileReader`\s to the processes.
The FileReader's of ``mylist`` is accessed via its property ``files``, which returns a :class:`FileSeq`:

>>> files = mylist.files
>>> files  # doctest: +SKIP
<BiglistFileSeq at '/tmp/dc260854-8041-40e8-801c-34084451d7a3' with 10023 elements in 101 data file(s)>
>>> len(files)
101
>>> files.num_data_files
101
>>> files.num_data_items
10023
>>> files[0]  # doctest: +SKIP
<BiglistFileReader for '/tmp/cfb39dc0-94bb-4557-a056-c7cea20ea653/store/1669667946.647939_46eb97f6-bdf3-45d2-809c-b90c613d69c7_100.pickle_zstd'>

A :class:`FileReader` object is light-weight. Upon initialization, it has not loaded the file yet---it merely records the file path along with the function that will be used to load the file.
In addition, FileReader objects are friendly to pickling, hence lend themselves to multiprocessing code.
Let's design a small experiment to consume this dataset in multiple processes:

>>> def worker(file_reader):
...     total = 0
...     for x in file_reader:
...         total += x
...     return total
>>> from concurrent.futures import ProcessPoolExecutor
>>> total = 0
>>> with ProcessPoolExecutor(5) as pool:  # doctest: +SKIP
...     tasks = [pool.submit(worker, fr) for fr in mylist.files]  # doctest: +SKIP
...     for t in tasks:  # doctest: +SKIP
...         total += t.result()  # doctest: +SKIP
>>> total  # doctest: +SKIP
50225253

.. using the ProcessPoolExecutor in a context manager causes it to hang;
.. see https://stackoverflow.com/questions/48218897/python-doctest-hangs-using-processpoolexecutor

What is the expected result?

>>> sum(mylist)
50225253

Sure enough, this verifies that the entire biglist is consumed by the processes *collectively*.

If the file loading is the bottleneck of the task, we can use threads in place of processes.

Similarly, it is possible to read ``mylist`` from multiple machines if ``mylist`` is stored
in the cloud. Since a FileReader object is pickle-able, it works just fine if we pickle it
and send it to another machine, provided the file path that is contained in the FileReader object
is in the cloud, hence accessible from the other machine.
We need a mechanism to distribute these FileReader objects to machines.
For that, read on.

Using Biglist to implement a "multiplexer"
------------------------------------------

The above distributes *files* to workers.
This is efficient for processing large amounts of data.

There are cases where we want to distribute individual *elements* to workers.
Suppose we perform some brute-force search on a cluster of machines;
there are 1000 grids, and the algorithm takes on one grid at a time.
Now, the grid is a "hyper-parameter" or "control parameter" that takes 1000 possible values.
We want to distribute these 1000 values to the workers.
How can we do that?

Use :class:`Multiplexer` provided by `biglist`.

Multiplexer is implemented using a Biglist.
Let's show its usage using local data and multiprocessing.
(For real work, we would use cloud storage and a cluster of machines.)
First, create a :class:`Multiplexer` to hold the values to be distributed:

>>> from upathlib import LocalUpath
>>> p = LocalUpath('/tmp/test/mux')
>>> p.rmrf()
0
>>> hyper = Multiplexer.new(range(20), p)
>>> len(hyper)
20

Next, design an interesting worker function:

>>> import multiprocessing, random, time
>>>
>>> def worker(path, task_id):
...     for x in Multiplexer(path, task_id):
...         time.sleep(random.uniform(0.1, 0.2))  # doing a lot of things
...         print(x, 'done in', multiprocessing.current_process().name)

Back in the main process,

>>> task_id = hyper.start()
>>> tasks = [multiprocessing.Process(target=worker, args=(hyper.path, task_id)) for _ in range(5)]
>>> for t in tasks:
...     t.start()
>>>
2 done in Process-13
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
>>> hyper.done()
True
>>> hyper.destroy()
>>>


Writing to a Biglist in multiple workers
========================================

The flip side of distributed reading is distributed writing.
If we have a biglist on the local disk, we can append to it from multiple processes or threads.
If we have a biglist in the cloud, we can append to it from multiple machines.
Let's use multiprocessing to demo the idea.

First, we create a new :class:`Biglist` at a storage location of our choosing:

>>> from upathlib import LocalUpath
>>> path = LocalUpath('/tmp/a/b/c/d')
>>> path.rmrf()
0
>>> yourlist = Biglist.new(path, batch_size=6)

In each worker process, we will open this biglist by ``Biglist(path)`` and append data to it.
Now that this has a presence on the disk, ``Biglist(path)`` will not complain the dataset does not exist.

>>> yourlist.info
{'storage_format': 'pickle-zstd', 'storage_version': 3, 'batch_size': 6, 'data_files_info': []}
>>> yourlist.path
LocalUpath('/tmp/a/b/c/d')
>>> len(yourlist)
0

Then we can tell workers, "here is the location, add data to it." Let's design a simple worker:

>>> def worker(path, idx):
...     yourlist = Biglist(path)
...     for i in range(idx):
...         yourlist.append(100 * idx + i)
...     yourlist.flush()

From the main process, let's instruct the workers to write data to the same :class:`Biglist`:

>>> import multiprocessing
>>> with ProcessPoolExecutor(10, mp_context=multiprocessing.get_context('spawn')) as pool:  # doctest: +SKIP
...     tasks = [pool.submit(worker, path, idx) for idx in range(10)]  # doctest: +SKIP
...     for t in tasks:  # doctest: +SKIP
...         _ = t.result()  # doctest: +SKIP

Let's see what we've got:


>>> yourlist.reload()  # doctest: +SKIP
>>> len(yourlist)  # doctest: +SKIP
45
>>> yourlist.num_data_files  # doctest: +SKIP
12
>>> list(yourlist)  # doctest: +SKIP
[400, 401, 402, 403, 500, 501, 502, 503, 504, 600, 601, 602, 603, 604, 605, 700, 701, 702, 703, 704, 705, 706, 900, 901, 902, 903, 904, 905, 906, 907, 908, 100, 800, 801, 802, 803, 804, 805, 806, 807, 200, 201, 300, 301, 302]
>>>

Does this look right? It took me a moment to realize that ``idx = 0`` did not append anything.
So, the data elements are in the 100, 200, ..., 900 ranges; that looks right.
But the order of things is confusing.

Well, in a distributed setting, there's no guarantee of order.
It's not a problem that numbers in the 800 range come *after* those in the 900 range.

We can get more insights if we dive to the file level:

>>> for f in yourlist.files:  # doctest: +SKIP
...     print(f)  # doctest: +SKIP
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073410.971439_f84d0cf3-e2c4-40a7-acf2-a09296ff73bc_1.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073410.973651_63e4ca6d-4e44-49e1-a035-6d60a88f7789_.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073410.975576_f59ab2f0-be9c-477d-a95b-70d3dfc00d94_6.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073410.982828_3219d2d1-50e2-4b41-b595-2c6df4e63d3c_6.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073410.983024_674e57de-66ed-4e3b-bb73-1db36c13fd6f_1.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073410.985425_78eec966-8139-4401-955a-7b81fb8b47b9_6.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073410.985555_752b4975-fbf3-4172-9063-711722a83abc_3.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073411.012161_3a7620f5-b040-4cec-9018-e8bd537ea98d_1.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073411.034502_4a340751-fa1c-412e-8f49-13f2ae83fc3a_6.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073411.035010_32c58dbe-e3a2-4ba1-9ffe-32c127df11a6_2.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073411.067370_20a0e926-7a5d-46a1-805d-86d16c346852_2.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073411.119890_89ae31bc-7c48-488d-8dd1-e22212773d79_3.pickle_zstd'>

The file names do not appear to be totally random. They follow some pattern that facilitates ordering, and they have encoded some useful info.
In fact, the part before the first underscore is the date and time of file creation, with a resolution to microseconds.
This is followed by a `uuid.uuid4 <https://docs.python.org/3/library/uuid.html#uuid.uuid4>`_ random string.
When we iterate the :class:`Biglist` object, files are read in the order of their paths, hence in the order of creation time.
The number in the file name before the suffix is the number of elements in the file.

We can get similar info in a more readable format:

>>> for v in yourlist.files.data_files_info:  # doctest: +SKIP
...     print(v)  # doctest: +SKIP
['/tmp/a/b/c/d/store/20230129073410.971439_f84d0cf3-e2c4-40a7-acf2-a09296ff73bc_4.pickle_zstd', 4, 4]
['/tmp/a/b/c/d/store/20230129073410.973651_63e4ca6d-4e44-49e1-a035-6d60a88f7789_5.pickle_zstd', 5, 9]
['/tmp/a/b/c/d/store/20230129073410.975576_f59ab2f0-be9c-477d-a95b-70d3dfc00d94_6.pickle_zstd', 6, 15]
['/tmp/a/b/c/d/store/20230129073410.982828_3219d2d1-50e2-4b41-b595-2c6df4e63d3c_6.pickle_zstd', 6, 21]
['/tmp/a/b/c/d/store/20230129073410.983024_674e57de-66ed-4e3b-bb73-1db36c13fd6f_1.pickle_zstd', 1, 22]
['/tmp/a/b/c/d/store/20230129073410.985425_78eec966-8139-4401-955a-7b81fb8b47b9_6.pickle_zstd', 6, 28]
['/tmp/a/b/c/d/store/20230129073410.985555_752b4975-fbf3-4172-9063-711722a83abc_3.pickle_zstd', 3, 31]
['/tmp/a/b/c/d/store/20230129073411.012161_3a7620f5-b040-4cec-9018-e8bd537ea98d_1.pickle_zstd', 1, 32]
['/tmp/a/b/c/d/store/20230129073411.034502_4a340751-fa1c-412e-8f49-13f2ae83fc3a_6.pickle_zstd', 6, 38]
['/tmp/a/b/c/d/store/20230129073411.035010_32c58dbe-e3a2-4ba1-9ffe-32c127df11a6_2.pickle_zstd', 2, 40]
['/tmp/a/b/c/d/store/20230129073411.067370_20a0e926-7a5d-46a1-805d-86d16c346852_2.pickle_zstd', 2, 42]
['/tmp/a/b/c/d/store/20230129073411.119890_89ae31bc-7c48-488d-8dd1-e22212773d79_3.pickle_zstd', 3, 45]

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

>>> from upathlib import LocalUpath
>>> import random
>>> from biglist import write_parquet_file_from_arrays
>>>
>>> path = LocalUpath('/tmp/a/b/c/e')
>>> path.rmrf()
0
>>> year = list(range(1970, 2021))
>>> make = ['honda'] * len(year)
>>> sales = list(range(123, 123 + len(make)))
>>> write_parquet_file_from_arrays([make, year, sales], path / 'honda.parquet', names=['make', 'year', 'sales'], row_group_size=10)
>>>
>>> year = list(range(1960, 2021))
>>> make = ['ford'] * len(year)
>>> sales = list(range(234, 234 + len(make)))
>>> write_parquet_file_from_arrays([make, year, sales], path / 'ford.parquet', names=['make', 'year', 'sales'], row_group_size=10)

Now we want to treat the contents of ``honda.parquet`` and ``ford.parquet`` combined as one dataset, and
use ``biglist`` tools to read it.

>>> from biglist import ParquetBiglist
>>> car_data = ParquetBiglist.new(path)
>>> car_data  # doctest: +SKIP
<ParquetBiglist at '/tmp/edd9cefb-179b-46d2-8946-7dc8ae1bdc50' with 112 records in 2 data file(s) stored at ['/tmp/a/b/c/e']>
>>> car_data.path  # doctest: +SKIP
LocalUpath('/tmp/edd9cefb-179b-46d2-8946-7dc8ae1bdc50')
>>> len(car_data)
112
>>> car_data.num_data_files
2
>>> list(car_data.files)
[<ParquetFileReader for '/tmp/a/b/c/e/ford.parquet'>, <ParquetFileReader for '/tmp/a/b/c/e/honda.parquet'>]

what :meth:`ParquetBiglist.new` does is to read the meta data of each file in the directory, recursively,
and save relevant info to facilitate its reading later.
The location given by ``car_data.path`` is the directory where :class:`ParquetBiglist` saves its meta info,
and not where the actual data are.
As is the case with :class:`Biglist`, this directory is a temporary one, which will be deleted once the object
``car_data`` goes away. If we wanted to keep the directory for future use, we should have specified a location
when calling :meth:`~ParquetBiglist.new`.


Reading a ParquetBiglist
========================

The fundamental reading API is the same between :class:`Biglist` and :class:`ParquetBiglist`:
random access, slicing/dicing using :class:`Slicer`, iteration,
distributed reading via :meth:`FileSeq.concurrent_iter`---these are all used the same way.

However, the structures of the data files are very different between :class:`Biglist` and :class:`ParquetBiglist`.
For Biglist, each data file contains a straight Python list, elements of which being whatever have been
passed into :meth:`Biglist.append`.
For ParquetBiglist, each data file is in a sophisticated columnar format, which is publicly documented.
A variety of ways are provided to get data out of the Parquet format;
some favor convenience, some others favor efficiency. Let's see some examples.

A row perspective
-----------------

>>> for i, x in enumerate(car_data):
...     print(x)
...     if i > 5:
...         break
{'make': 'ford', 'year': 1960, 'sales': 234}
{'make': 'ford', 'year': 1961, 'sales': 235}
{'make': 'ford', 'year': 1962, 'sales': 236}
{'make': 'ford', 'year': 1963, 'sales': 237}
{'make': 'ford', 'year': 1964, 'sales': 238}
{'make': 'ford', 'year': 1965, 'sales': 239}
{'make': 'ford', 'year': 1966, 'sales': 240}

This is the most basic iteration, :class:`Biglist`-style, one row (or "record") at a time.
When there are multiple columns, each row is presented as a dict with column names as keys.

Reading a Parquet data file is performed by :class:`ParquetFileReader`.

>>> f0 = car_data.files[0]
>>> f0
<ParquetFileReader for '/tmp/a/b/c/e/ford.parquet'>
>>> f0.path
LocalUpath('/tmp/a/b/c/e/ford.parquet')

First of all, a :class:`FileReader` object is a :class:`Seq`, providing row-based view into the data:

>>> len(f0)
61
>>> f0[2]
{'make': 'ford', 'year': 1962, 'sales': 236}
>>> f0[-10]
{'make': 'ford', 'year': 2011, 'sales': 285}
>>> Slicer(f0)[-3:].collect()
[{'make': 'ford', 'year': 2018, 'sales': 292}, {'make': 'ford', 'year': 2019, 'sales': 293}, {'make': 'ford', 'year': 2020, 'sales': 294}]
>>> for i, x in enumerate(f0):
...     print(x)
...     if i > 5:
...         break
{'make': 'ford', 'year': 1960, 'sales': 234}
{'make': 'ford', 'year': 1961, 'sales': 235}
{'make': 'ford', 'year': 1962, 'sales': 236}
{'make': 'ford', 'year': 1963, 'sales': 237}
{'make': 'ford', 'year': 1964, 'sales': 238}
{'make': 'ford', 'year': 1965, 'sales': 239}
{'make': 'ford', 'year': 1966, 'sales': 240}

:class:`ParquetFileReader` uses `pyarrow`_ to read the Parquet files.
The values above are nice and simple Python types, but they are not the original
pyarrow types;
they have undergone a conversion. This conversion can be toggled by the property
:data:`ParquetFileReader.scalar_as_py`:

>>> f0[8]
{'make': 'ford', 'year': 1968, 'sales': 242}
>>> f0.scalar_as_py = False
>>> f0[8]
{'make': <pyarrow.StringScalar: 'ford'>, 'year': <pyarrow.Int64Scalar: 1968>, 'sales': <pyarrow.Int64Scalar: 242>}
>>> f0.scalar_as_py = True

A Parquet file consists of one or more "row groups". Each row-group is a batch of rows stored column-wise.
We can get info about the row-groups, or even retrieve a row-group as the unit of processing:

>>> f0.num_row_groups
7
>>> f0.metadata  # doctest: +ELLIPSIS
<pyarrow._parquet.FileMetaData object at 0x7...>
  created_by: parquet-cpp-arrow version 11.0.0
  num_columns: 3
  num_rows: 61
  num_row_groups: 7
  format_version: 2.6
  serialized_size: 2375
>>> f0.metadata.row_group(1)  # doctest: +ELLIPSIS
<pyarrow._parquet.RowGroupMetaData object at 0x7...>
  num_columns: 3
  num_rows: 10
  total_byte_size: 408
>>> f0.metadata.row_group(0)  # doctest: +ELLIPSIS
<pyarrow._parquet.RowGroupMetaData object at 0x7...>
  num_columns: 3
  num_rows: 10
  total_byte_size: 408
>>> rg = f0.row_group(0)
>>> rg
<ParquetBatchData with 10 rows, 3 columns>

(We have specified ``row_group_size=10`` in the call to :func:`write_parquet_file_from_arrays` for demonstration.
In practice, a row-group tends to be much larger.)

A :class:`ParquetBatchData` object is again a :class:`Seq`.
All of our row access tools are available:

>>> rg.num_rows
10
>>> len(rg)
10
>>> rg.num_columns
3
>>> rg[3]
{'make': 'ford', 'year': 1963, 'sales': 237}
>>> rg[-2]
{'make': 'ford', 'year': 1968, 'sales': 242}
>>> Slicer(rg)[4:7].collect()
[{'make': 'ford', 'year': 1964, 'sales': 238}, {'make': 'ford', 'year': 1965, 'sales': 239}, {'make': 'ford', 'year': 1966, 'sales': 240}]
>>> rg.scalar_as_py = False
>>> rg[3]
{'make': <pyarrow.StringScalar: 'ford'>, 'year': <pyarrow.Int64Scalar: 1963>, 'sales': <pyarrow.Int64Scalar: 237>}
>>> rg.scalar_as_py = True

When we request a specific row, :class:`ParquetFileReader` will load the row-group that contains the row of interest.
It doe not load the entire data in the file.
However, we can get greedy and ask for the whole data in one go:

>>> f0
<ParquetFileReader for '/tmp/a/b/c/e/ford.parquet'>
>>> f0.data()
<ParquetBatchData with 61 rows, 3 columns>

This, again, is a :class:`ParquetBatchData` object. All the familiar row access tools are at our disposal.

Finally, if the file is large, we may choose to iterate over it by batches instead of by rows:

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
<ParquetFileReader for '/tmp/a/b/c/e/ford.parquet'>
>>> cols.num_columns
2
>>> cols.column_names
['year', 'sales']

:meth:`ParquetFileReader.columns` returns another :class:`ParquetFileReader`, whereas
:meth:`ParquetBatchData.columns` returns another :class:`ParquetBatchData`:

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

It's an interesting case when there's only one column:

>>> f0
<ParquetFileReader for '/tmp/a/b/c/e/ford.parquet'>
>>> sales = f0.columns(['sales'])
>>> sales
<ParquetFileReader for '/tmp/a/b/c/e/ford.parquet'>
>>> sales.column_names
['sales']
>>> len(sales)
61
>>> sales[3]
237
>>> list(sales)  # doctest: +ELLIPSIS
[234, 235, 236, 237, 238, 239, ..., 291, 292, 293, 294]
>>> Slicer(sales)[:8].collect()
[234, 235, 236, 237, 238, 239, 240, 241]

Notice the type of the values (rows) returned from the element access methods: it's *not* ``dict``.
Because there's only one column whose name is known, there is no need to carry that info with every row.
Also note that the values have been converted to Python builtin types.
The original `pyarrow`_ values will not look as nice:
   
>>> sales.scalar_as_py = False
>>> Slicer(sales)[:8].collect()
[<pyarrow.Int64Scalar: 234>, <pyarrow.Int64Scalar: 235>, <pyarrow.Int64Scalar: 236>, <pyarrow.Int64Scalar: 237>, <pyarrow.Int64Scalar: 238>, <pyarrow.Int64Scalar: 239>, <pyarrow.Int64Scalar: 240>, <pyarrow.Int64Scalar: 241>]
>>> sales.scalar_as_py = True

Both :class:`ParquetFileReader` and :class:`ParquetBatchData` have another method called ``column``, which retrieves a single column
and returns a
`pyarrow.Array`_ or
`pyarrow.ChunkedArray`_. For example,

>>> sales2 = f0.column('sales')
>>> sales2  # doctest: +ELLIPSIS
<pyarrow.lib.ChunkedArray object at 0x...>
[
  [
    234,
    235,
    236,
    237,
    238,
    ...
    290,
    291,
    292,
    293,
    294
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
Its property :data:`~ParquetFileReader.file` initiates a 
`pyarrow.parquet.ParquetFile`_ object (reading meta data during initiation)
and returns it. We may take it and go all the way down the `pyarrow`_ path:

>>> f1 = car_data.files[1]
>>> f1._data is None
True
>>> file = f1.file
>>> file  # doctest: +ELLIPSIS
<pyarrow.parquet.core.ParquetFile object at 0x7...>
>>> f1._file
<pyarrow.parquet.core.ParquetFile object at 0x7...>

We have seen that :meth:`ParquetFileReader.row_group` and :meth:`ParquetFileReader.iter_batches` both
return :class:`ParquetBatchData` objects. In contrast to :class:`ParquetFileReader`, which is "lazy" in terms of data loading,
a ParquetBatchData already has its data in memory. ParquetFileReader has another method,
namely :meth:`~ParquetFileReader.data`, that
eagerly loads the entire data of the file and wraps it in a ParquetBatchData object:

>>> data = f1.data()
>>> data
<ParquetBatchData with 51 rows, 3 columns>

The `pyarrow`_ data wrapped in :class:`ParquetBatchData` can be acquired easily:

>>> padata = data.data()
>>> padata
pyarrow.Table
make: string
year: int64
sales: int64
----
make: [["honda","honda","honda","honda","honda",...,"honda","honda","honda","honda","honda"]]
year: [[1970,1971,1972,1973,1974,...,2016,2017,2018,2019,2020]]
sales: [[123,124,125,126,127,...,169,170,171,172,173]]

Finally, we have seen that :meth:`ParquetFileReader.column` and :meth:`ParquetBatchData.column`---the single-column selectors---return
a `pyarrow`_ object. It is either a 
`pyarrow.Array`_ or a 
`pyarrow.ChunkedArray`_.



Other utilities
===============

Chain
-----

:class:`Chain` takes a series of :class:`Seq`\s and returns a combined Seq without data copy.
For example,

>>> from biglist import Chain
>>> numbers = list(range(10))
>>> car_data  # doctest: +SKIP
<ParquetBiglist at '/tmp/edd9cefb-179b-46d2-8946-7dc8ae1bdc50' with 112 records in 2 data file(s) stored at ['/tmp/a/b/c/e']>
>>> combined = Chain(numbers, car_data)
>>> combined[3]
3
>>> combined[9]
9
>>> combined[10]
{'make': 'ford', 'year': 1960, 'sales': 234}
>>>
>>> car_data[0]
{'make': 'ford', 'year': 1960, 'sales': 234}


Slicer
------

:class:`Slicer` takes any :class:`Seq`` and provides :meth:`~Slicer.__getitem__` that accepts
a single index, or a slice, or a list of indices. A single-index access will return
the requested element; the other two scenarios return a new Slicer via a zero-copy operation.
To get all the elements out of a Slicer, either iterate it or call its method :meth:`~Slicer.collect`.

We should emphasize that :class:`Chain` and :class:`Slicer` work with any :class:`Seq`,
hence they are useful independent of the other ``biglist`` classes.


Reading Parquet files
---------------------

The function :func:`read_parquet_file` is provided to read a single Parquet file independent of
:class:`ParquetBiglist`. It returns a :class:`ParquetFileReader`. All the facilities of this class,
as demonstrated above, are ready for use:

>>> [v.path for v in car_data.files]
[LocalUpath('/tmp/a/b/c/e/ford.parquet'), LocalUpath('/tmp/a/b/c/e/honda.parquet')]
>>>
>>> from biglist import read_parquet_file
>>> ff = read_parquet_file(car_data.files[1].path)
>>> ff
<ParquetFileReader for '/tmp/a/b/c/e/honda.parquet'>
>>> len(ff)
51
>>> ff.column_names
['make', 'year', 'sales']
>>> ff[3]
{'make': 'honda', 'year': 1973, 'sales': 126}
>>> Slicer(ff.columns(['year', 'sales']))[10:16].collect()
[{'year': 1980, 'sales': 133}, {'year': 1981, 'sales': 134}, {'year': 1982, 'sales': 135}, {'year': 1983, 'sales': 136}, {'year': 1984, 'sales': 137}, {'year': 1985, 'sales': 138}]
>>> ff.num_row_groups
6
>>> ff.row_group(3).column('sales')  # doctest: +ELLIPSIS
<pyarrow.lib.ChunkedArray object at 0x7...>
[
  [
    153,
    154,
    155,
    156,
    157,
    158,
    159,
    160,
    161,
    162
  ]
]

Writing Parquet files
---------------------

The function :func:`write_parquet_file_from_arrays` is provided to write data columns to a single Parquet file.

>>> from uuid import uuid4
>>> from biglist import write_parquet_file_from_arrays, read_parquet_file
>>> import random
>>> from upathlib import LocalUpath
>>> N = 10000
>>> path = LocalUpath('/tmp/a/b/c/d')
>>> path.rmrf()
1
>>> write_parquet_file_from_arrays([[random.randint(0, 10000) for _ in range(N)], [str(uuid4()) for _ in range(N)]], path / 'data.parquet', names=['key', 'value'])
>>> f = read_parquet_file(path / 'data.parquet')
>>> f
<ParquetFileReader for '/tmp/a/b/c/d/data.parquet'>
>>> len(f)
10000
>>> f.metadata   # doctest: +ELLIPSIS
<pyarrow._parquet.FileMetaData object at 0x7...>
  created_by: parquet-cpp-arrow version 11.0.0
  num_columns: 2
  num_rows: 10000
  num_row_groups: 1
  format_version: 2.6
  serialized_size: 609
>>> f.metadata.schema  # doctest: +ELLIPSIS
<pyarrow._parquet.ParquetSchema object at 0x7...>
required group field_id=-1 schema {
  optional int64 field_id=-1 key;
  optional binary field_id=-1 value (String);
}
<BLANKLINE>
>>>

Similarly, :func:`write_parquet_file_from_pylist` writes data rows to a Parquet file::

>>> from biglist import write_parquet_file_from_pylist
>>> data = [{'name': str(uuid4()), 'age': random.randint(1, 100), 'income': {'employer': str(uuid4()), 'amount': random.randint(10000, 100000)}} for _ in range(100... 0)]
>>> f = LocalUpath('/tmp/test/data.parquet')
>>> f.rmrf()
0
>>> write_parquet_file_from_pylist(data, f)
>>> ff = read_parquet_file(f)
>>> ff[0]  # doctest: +SKIP
{'name': '066ced72-fd33-492a-9180-39eeca541b1a', 'age': 75, 'income': {'amount': 17840, 'employer': 'bfc176a0-5257-4913-bd1e-3c4d51885e0c'}}
>>> ff[11]  # doctest: +SKIP
{'name': 'a239af28-41ff-4215-b560-9c45db15478e', 'age': 12, 'income': {'amount': 17488, 'employer': 'e97f70c9-1659-4fa6-9123-eb39779d00d6'}}
>>>


API reference
=============


.. autodata:: biglist._util.Element


.. autoclass:: biglist.Seq
   :exclude-members: __init__


.. autodata:: biglist._util.SeqType


.. autoclass:: biglist.FileReader


.. autodata:: biglist._base.FileReaderType


.. autoclass:: biglist.FileSeq


.. autoclass:: biglist.Slicer


.. autoclass:: biglist.Chain


.. autoclass:: BiglistBase


.. autoclass:: biglist.Biglist


.. autoclass:: biglist.BiglistFileReader


.. autoclass:: biglist.BiglistFileSeq


.. autoclass:: biglist.Multiplexer


.. autoclass:: biglist.ParquetBiglist


.. autoclass:: biglist.ParquetFileReader


.. autoclass:: biglist.ParquetFileSeq


.. autoclass:: biglist.ParquetBatchData


.. autofunction:: biglist.read_parquet_file

.. autofunction:: biglist.write_parquet_file_from_arrays

.. autofunction:: biglist.write_parquet_file_from_pylist

.. autoclass:: biglist._biglist.ParquetSerializer

.. autofunction:: biglist._util.make_parquet_schema

.. autofunction:: biglist._util.make_parquet_field

.. autofunction:: biglist._util.make_parquet_type


Indices and tables
==================

* :ref:`genindex`
* :ref:`search`
