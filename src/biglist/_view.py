# from __future__ import annotations

# Will no longer be needed at Python 3.10.

import logging

from typing import (
    Union,
    Sequence,
    List,
    Callable,
    TypeVar,
)

from upathlib import Upath  # type: ignore



logger = logging.getLogger(__name__)




T = TypeVar("T")


class FileView(Sequence[T]):
    def __init__(self, file: Upath, loader: Callable):
        # TODO: make `Upath` safe to pass across processes.
        self._file = file
        self._loader = loader
        self._data = None

    @property
    def data(self) -> List[T]:
        if self._data is None:
            self._data = self._loader(self._file)
        return self._data

    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, idx: Union[int, slice]) -> T:
        return self.data[idx]

    def __iter__(self):
        return iter(self.data)


class ListView(Sequence[T]):
    def __init__(self, list_: Sequence[T], range_: range = None):
        """
        This provides a "window" into the sequence `list_`,
        which is often a `Biglist` or another `ListView`.

        An object of `ListView` is created by `Biglist.view()` or
        by slicing a `ListView`.
        User should not attempt to create an object of this class directly.

        The main purpose of this class is to provide slicing over `Biglist`.

        During the use of this object, it is assumed that the underlying
        `list_` is not changing. Otherwise the results may be incorrect.
        """
        self._list = list_
        self._range = range_

    def __len__(self) -> int:
        if self._range is None:
            return len(self._list)
        return len(self._range)

    def __bool__(self) -> bool:
        return len(self) > 0

    def __getitem__(self, idx: Union[int, slice]) -> T:
        """
        Element access by a single index or by slice.
        Negative index and standard slice syntax both work as expected.

        Sliced access returns a new `ListView` object.
        """
        if isinstance(idx, int):
            if self._range is None:
                return self._list[idx]
            return self._list[self._range[idx]]

        if isinstance(idx, slice):
            if self._range is None:
                range_ = range(len(self._list))[idx]
            else:
                range_ = self._range[idx]
            return self.__class__(self._list, range_)

        raise TypeError(
            f"{self.__class__.__name__} indices must be integers or slices, not {type(idx).__name__}"
        )

    def __iter__(self):
        if self._range is None:
            yield from self._list
        else:
            for i in self._range:
                yield self._list[i]

    @property
    def raw(self) -> Sequence[T]:
        return self._list
