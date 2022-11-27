# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).


## [0.7.1] - unreleased

### Removed

### Changed

- Methods `dump_data_file` and `load_data_file` are renamed `_dump_data_file` and `_load_data_file`. Subclasses could have a need to override these, but that's not encouraged.


### Added

- New public method (property) `datafiles_info`.
- Created Sphinx documentation and started hosting it on "Read the Docs". This is the first version to have such  documentation.

### Fixed, enhanced, changed

- Fixed a bug in `ParquetBiglist.get_gcsfs` regarding token expiration.
- Enhanced `FileView` and related methods.


## [0.7.0] - 2022-11-18

### Added

- Added "external" Biglist variant---class `ParquetBiglist`---backed by Parquet data. This exposes a few new classes as part of the public API. To this purpose, there was a code re-org.
- New class `ChainedList`.
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
