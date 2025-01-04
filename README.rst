biglist
=======

----UPDATE----

After 0.9.6 (December 2024), development of this package continues in the repo 
`cloudly <https://github.com/zpz/cloudly>`_
as part of the ``cloudly`` package.

This move is related to the similar move for `upathlib <https://github.com/zpz/upathlib>`_.
When you use `biglist`, you are typically dealing with large amounts of data and saving
the biglist in a cloud blob store. `biglist` builds on `upathlib`, which provides a consistent
API across local and cloud storages. As such, depending on what cloud provider you use,
you also need to pull in corresponding dependencies that `upathlib` requires for your cloud.
This includes authentication and cloud storage utilities for the said cloud.
These utilities, in turn, are in the scope of `cloudly`. For these reasons, I decided to
merge both `upathlib` and `biglist` into `cloudly`.

`biglist` has been used in production for several years, and I feel it's pretty stable.
But who knows how much better it can be? You may want to switch to using it from `cloudly`.


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
