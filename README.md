# biglist

`biglist` implements a persisted, out-of-memory list for Python.

Persistence can happen on local disk or in a cloud blob store, when appropriate storage "engine" is used.

Mutation is by appending only. Update to existing data elements is not supported.

Random element access by index and slice is supported, but not optimized. Iteration is optimized, which is the main target scenario of consumption.

Distributed reading and writing are supported. This means appending to or reading from a `Biglist` by multiple workers concurrently. In the case of reading, the data of the `Biglist` is split between the workers. When the storage is local, the workers are multiple threads or processes. When the storage is remote (i.e. in a cloud blob store), the workers are multiple threads or processes on one or more machines.

A very early version of this work is described in [a blog post](https://zpz.github.io/blog/biglist/).
