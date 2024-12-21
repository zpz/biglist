biglist
=======

UPDATE: after 0.9.6, development of this package continues in the repo 
`cloudly <https://github.com/zpz/cloudly>`_
as part of the ``cloudly`` package.
The main APIs are defined in ``cloudly.biglist``.

Users are recommended to use ``cloudly`` directly.

----END OF UPDATE----


The package ``biglist`` provides persisted, out-of-memory Python data structures
that implement the ``Sequence`` and ``Iterable`` interfaces with the capabilities of
concurrent and distributed reading and writing.
The main use case is sequentially processing large amounts of data that can not fit in memory.
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

3.10 or newer.
