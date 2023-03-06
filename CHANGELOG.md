# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).


## [0.7.7] - in progress

### Removed

- Removed many deprecated methods.
- `Biglist.register_storage_format` lost parameter `overwrite`, that is, it no longer allows changing
  the definition of a "built-in" format.

### Deprecated

- Deprecated "concurrent_iter" methods from `FileSeq` and "multiplexer" methods from `Biglist`.
- Deprecated function `write_parquet_file` (use `write_parquet_file_from_arrays` instead).

### Added

- `BiglistBase.new` gets a new parameter `init_info`.
- `Biglist.new` accepts extra parameters for (de)serialization.
- New class `Multiplexer`.
- Allow schema spec when 'storage_format' is 'parquet' for `Biglist`.
- Function `write_parquet_file` was renamed `write_parquet_file_from_arrays`; added new function `write_parquet_file_from_list`.


## [0.7.6] - 2022-03-01

- Fix a bug introduced in 0.7.5 in `Biglist.__init__` backcompat fix. 


## [0.7.5] - 2022-02-28

- Fix a bug in 0.7.4 about `Biglist.info['data_files_info']`.
- `arrow` became a mandatory (rather than optional) dependency, hence Parquet-related functionalities are available in regular install.


## [0.7.4] - 2022-02-25

This release contains a large refactor, creating classes `Seq` and `FileSeq` and using them in many places
in the code.

`BiglistBase` gets a new method `files`, returning a `FileSeq`.
The functions related to iterating over `FileReader`s (sequentially or concurrently) are moved to
`FileSeq`. Many related methods in `BiglistBase` are deprecated.

There are other deprecations and renamings, for example,

- Class renamings: `ListView` -> `Slicer`; `ChainedList` -> `Chain`.
- Deprecated the parameter `thread_pool_executor` to `__init__` and `__new__`.

The new class `Seq` and the renamed classes `Chain` and `Slicer` are in the module `_util`.

### Breaking changes

Previously, as new data items are `append`ed to a `Biglist`,
data items that are not yet `flush`ed, i.e. not persisted, hence only in memory buffer,
are immediately included in item access (by `__getitem__`), iteration (by `__iter__`),
and counted in the length of the Biglist. Now these elements are not included in these operations.

### Removed

- `BiglistBase.{resolve_path, lockfile}`. These methods are replaced by direct calls to functions from `upathlib`.
- Parameter `require_exists` to `BiglistBase.__init__`.

### Added

- `Biglist` gets a new `storage_format`--'parquet'--for simple data structures.


## [0.7.3] - 2022-12-27

- Upgrade dependency `upathlib`, removing formats 'json-z' and 'json-zstd'.


## [0.7.2] - 2022-12-14

### Enhanced

- Enhancement and refinement of type annotations.
- Refine the "generic type" annotations for the classes.


## [0.7.1] - 2022-12-03

### Removed

- Removed `BiglistBase.{new_concurrent_iter, concurrent_iter, concurrent_iter_stat, concurrent_iter_done}`. Please use `new_concurrent_file_iter` and the related methods.
- `ParquetBiglist.iter_batches` was removed because it was simple and unnecessary.

### Changed and enhanced

- `BiglistBase.load_data_file` losts parameter `mode`. Now it has only one parameter, which is the file path.
- Enhanced `FileView` and related code. Both `ParquetFileData` and `BiglistFileData` are now subclasses of `FileView`.
- Enhanced the treatment of `{ParquetFileData, ParquetBatchData}.scalar_as_py`.
- Class renamings: `FileView` -> `FileReader`; `ParquetFileData` -> `ParquetFileReader`; `BiglistFileData` -> `BiglistFileReader`.
  The old names will be available for a period of deprecation.
- Method renamings: `file_view` -> `file_reader`; `file_views` -> `file_readers`.
  The old names will be available for a period of deprecation.

### Added

- New public method (property) `datafiles_info`.
- Created Sphinx documentation and started hosting it on "Read the Docs". This is the first version to have such documentation. This is the main work of this release.

### Fixed

- Fixed a bug in `ParquetBiglist.get_gcsfs` regarding token expiration.



## [0.7.0] - 2022-11-18

### Added

- Added "external" Biglist variant---class `ParquetBiglist`---backed by Parquet data. This exposes a few new classes as part of the public API. To this purpose, there was a code re-org.
- New class `Chain`.
- New class `BiglistFileData`. Although currently the data file content of a `Biglist` is always a Python `list`, the class prepares for possible changes in the future. In addition, this achieves consistency between `Biglist`/`BiglistFileData` and `ParquetBiglist`/`ParquetFileData`.

### Removed

- Methods `Biglist.pre_serialize` and `Biglist.post_deserialize` are removed. Alternatives to achieve similar effects are documented. However, the kind of customization facilitated by `pre_serialize` and `post_deserialize` is *discouraged*. The recommendation is to persist in Python built-in types (such as `dict`, `tuple`, etc), and do conversion to/from custom types in application code.
- Removed method `Biglist.destroy`. Persisted data of a temporary `Biglist` (i.e. you did not specify its path when calling `new`) is deleted upon the instance's garbage collection. If you want to delete the storage of a non-temporary `Biglist`, call `rmrf` on its `.path` attribute.

### Changed

- More systematic and thoughtful use of `ListView`.
- `Biglist.load_data_file` returns an object of the new class `BiglistFileData`.
- `Biglist.DEFAULT_STORAGE_FORMAT` changed from `pickle` to `pickle-zstd`.
- Reformatted `CHANGELOG`.


## [0.6.9] - 2022-10-29

- Add `multiplexer` methods.
- Add parameter `require_exists` to `__init__`.
- Add parameter `thread_pool_executor` to `__init__`, hence allowing user to control the number of threads created by this class. This is useful when a large number of `Biglist` instances are active at the same time.


## [0.6.8] - 2022-07-30

- Removed the "data_info_file" which contains names and lengths of the data files,
  as well as indicating their order. Added a new file containing just the number
  of data files.

  Changed the file name pattern to contain timestamp and length in name,
  so that the info previously stored in and loaded from the
  "data_info_file" are now parsed out by on-demand iteration over available files.
  This change is meant to improve speed during "concurrent writing" to a biglist
  with a large number of data files.
- Removed methods `concurrent_append` and `concurrent_extend`. Use `append` and
  `extend` instead.
- Made `Biglist` a "generic" class, that is, can take a type parameter in annotations,
  like `MyBiglist[MyClass]`.


## [0.6.7] - 2022-07-18

- Bug fixes.
- Simplified bookkeeping for 'concurrent_iter'.


## [0.6.6] - 2022-07-10

- Upgrade for `upathlib` 0.6.4, which has a breaking change about serializers.


## [0.6.5] - 2022-06-30

- Minor changes to prepare for `upathlib` upgrade.
- Make Orjson-based storage formats optional.
- Dependency on `upathlib` specifies default `upathlib` only.
  User who needs enhanced `upathlib` can simply install that explicitly
  prior to installing `biglist`, e.g. `pip install upathlib[gcs]`.


## [0.6.4] - 2022-04-10

- Change default format from 'orjson' to 'pickle'.


## [0.6.3] - 2022-03-08

- Minor refinements and speed improvements.
- Explicitly separate concurrent (multi-worker) and non-concurrent
  (uni-worker) writing, so as to improve speed in the non-concurrent case.
  - New methods `concurrent_append`, `concurrent_extend`.
- Renamings:
  - `reset_file_iter` -> `new_concurrent_iter`
  - `iter_files` -> `concurrent_iter`
  - `file_iter_stat` -> `concurrent_iter_stat`
- Refactor and simplify test and build processes.


## [0.6.2] - 2021-11-11

- Fix and fine-tuning related to threading.


## [0.6.1] - 2021-08-14

- Fix related to finalizing the object in __del__.
- Use `upathlib.serializer`.


## [0.5.7] - 2021-07-03

- Fixes following `upathlib` upgrade.


## [0.5.5] - 2021-06-23

- Allow custom file lock.


## [0.5.1]

- Added `CompressedOrjsonSerializer`, with file extension 'orjson_z'.


## [0.5.0] - 2021-06-06

First public release. Most APIs are in place.
