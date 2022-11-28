.. biglist documentation master file, created by
   sphinx-quickstart on Fri Nov 25 22:11:50 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

:tocdepth: 3

===========
``biglist``
===========

.. automodule:: biglist


Installation
============

To install ``biglist`` and use local disk for data persistence, simply do 

::

   pip install biglist

``biglist`` has a couple optional components:

   ``gcs``
      for using Google Cloud Storage for data persistence
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


Creating a ``Biglist``
======================

Create a new ``biglist`` object via the ``classmethod`` ``new``::

   >>> from biglist import Biglist
   >>> mylist = Biglist.new(batch_size=100)

then add data to it, for example,

::

   >>> for x in range(10_023):
   ...     mylist.append(x)

As it happens, this saves a new data file for every 100 elements
accumulated. In the end, there are 23 elements in a memory buffer that are
not yet persisted to disk. The code has no way to know whether we will append
more elements, hence it does not save this partial batch.
Suppose we're done, we call ``flush`` to persist the content of the memory buffer
to disk::

   >>> mylist.flush()

If, after a while, we decide to append more data to ``mylist``, we just call ``append`` again.
We can continue to add more data as long as the disk has space.
New data files will be saved. The smaller file containing 23 elements will stay there
among larger files, and that is fine.

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
which is a temporary one because we did not tell ``Biglist.new`` where to save our data.
When the object ``mylist`` dies, this directory will be deleted automatically.
This has its use cases, but often we want to save the data for later. In that case, just pass ``new``
a non-existent directory, e.g.

   >>> yourlist = Biglist.new('/project/data/store-a', batch_size=100)

Later, initiate a ``Biglist`` object for reading::

   >>> yourlist = Biglist('/project/data/store-a')

This time, we're not using ``Biglist.new`` because we are reading an existing dataset
at the known location.

If we want to persist the data in Google Cloud Storage, we would specify a path in the
'gs://bucket-name/path/to/data' format.


Reading a ``Biglist``
=====================

Random element access
---------------------

``Biglist`` implements the ``Sequence`` interface, hence we can access any element as like we do a list::

   >>> mylist[18]
   18
   >>> mylist[-3]
   10020

It does not support slicing directly.
However, the method ``view`` returns an object that supports element access by a single index, by a slice, or by a list of indices::

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
Instead, it returns another ``ListView`` object, which, naturally, can be used the same way,
including slicing further.

A ``ListView`` object is both a ``Sequence`` and an ``Iterable``, hence we can gather all of its elements in a list::

   >>> list(v[100:104])
   [100, 101, 102, 103]

``ListView`` provides a convenience method ``collect`` to do the same thing::

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
**Random element access for ``Biglist`` is inefficient!**
The reason is that it needs to load any file that contains the element of interest.
(However, consecutive random accesses to elements residing in the same file will not load the file
repeatedly.)

The preferred way to consume the data of a ``Biglist`` is to iterate over it. For example,

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

Under the hood, this iterates the data files, and then simply iterates the elements in each data file.
In other words,

::

   >>> for x in mylist: 
   ...     print(x)    

is actually implemented by

::

   >>> for batch in mylist.iter_files():
   ...     for x in batch:
   ...         print(x)

There are cases where we want to use ``iter_files`` directly.


Reading from a ``Biglist`` in multiple processes
------------------------------------------------

To **collectively** consume a ``Biglist`` object from multiple processes,
distribute ``FileView``'s to the processes.


Reading from a ``Biglist`` in multiple machines
-----------------------------------------------

Writing to a ``Biglist`` in multiple processes or machines
----------------------------------------------------------


Creating a ``ParquetBiglist``
=============================

Reading a ``ParquetBiglist``
============================


API reference
=============

``BiglistBase``
---------------

.. autoclass:: biglist._base.BiglistBase
   :members:

``Biglist``
-----------

.. autoclass:: biglist.Biglist
   :members:

``BiglistFileData``
-------------------

.. autoclass:: biglist.BiglistFileData
   :members:

``ParquetBiglist``
------------------

.. autoclass:: biglist.ParquetBiglist
   :members:

``ParquetFileData``
-------------------

.. autoclass:: biglist.ParquetFileData
   :members:

``ParquetBatchData``
--------------------

.. autoclass:: biglist.ParquetBatchData
   :members:

``ListView``
------------

.. autoclass:: biglist.ListView
   :members:


.. Indices and tables
.. ==================

.. * :ref:`genindex`
.. * :ref:`modindex`
.. * :ref:`search`
