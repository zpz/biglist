# biglist

`biglist` provides a class `Biglist`, which implements a persisted, out-of-memory Python data structure operations by the familiar *list* interface. The main use case is processing large amounts of data that can not fit in memory.

Persistence can be on local disk or in a cloud blob store.

Mutation is append-only. Updating existing elements of the list is not supported.

Random element access by index and slice is supported, but not optimized. The recommended way of consumption is by iteration, which is optimized for speed.

Distributed reading and writing are supported. This means appending to or reading from a `Biglist` by multiple workers concurrently. In the case of reading, the data of the `Biglist` is split between the workers. When the storage is local, the workers are multiple threads or processes. When the storage is remote (i.e. in a cloud blob store), the workers are multiple threads or processes on one or more machines.

Of course, reading the entire list concurrently by a number of independent workers is also possible. That, however, is not called "distributed" reading.

A very early version of this work is described in [a blog post](https://zpz.github.io/blog/biglist/).

## Status

Production ready.
