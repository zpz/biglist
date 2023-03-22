biglist
=======

The package ``biglist`` provides persisted, out-of-memory Python data structures
that implement the ``Sequence`` and ``Iterable`` interfaces with the capabilities of
concurrent and distributed reading and writing.
The main use case is processing large amounts of data that can not fit in memory.
Persistence can be on local disk or in cloud storage.


Read the `documentation <https://biglist.readthedocs.io/en/latest/>`_.


Reference
---------

A very early version of this work is described in `a blog post <https://zpz.github.io/blog/biglist/>`_.

Status
------

Production ready.


Python version
--------------

Development and testing were conducted in Python 3.8 until version 0.7.8.
Starting with 0.7.9, development and testing happen in Python 3.10.
Code continues to NOT intentionally use features beyond Python 3.8.
I intend to test agsint versions 3.8, 3.9, 3.10, 3.11 once I find time to set that up.
