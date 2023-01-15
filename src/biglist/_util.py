from __future__ import annotations

import bisect
import itertools
from collections.abc import Iterator, Sequence
from typing import Generic, Optional, Protocol, TypeVar, runtime_checkable


def locate_idx_in_chunked_seq(
    idx: int,
    len_cumsum: Sequence[int],
    last_chunk: Optional[tuple[int, int, int]] = None,
):
    """
    Suppose a sequence is composed of a number of member sequences.
    This function finds which member sequence contains the requested item.

    `idx`: index of the item of interest.
    `len_cumsum`: cumulative lengths of the member sequences.
    `last_chunk`: info about the last call to this function, consisting of
        ('index of member sequence chosen',
         'starting index of the chosen sequence',
         'finishing index (plus 1) of the chosen sequence',
        )
    """
    if idx < 0:
        idx = len_cumsum[-1] + idx
    if idx < 0 or idx > len_cumsum[-1]:
        raise IndexError(idx)

    igrp0 = None
    igrp = None
    last_group = last_chunk
    if last_group is not None:
        igrp0 = last_group[0]
        if idx < last_group[1]:
            igrp = bisect.bisect_right(len_cumsum, idx, hi=igrp0)
        elif idx < last_group[2]:
            igrp = igrp0
        else:
            igrp = bisect.bisect_right(len_cumsum, idx, lo=igrp0 + 1)
    else:
        igrp = bisect.bisect_right(len_cumsum, idx)
    if igrp != igrp0:
        if igrp == 0:
            last_chunk = (
                0,  # group index
                0,  # item index lower bound
                len_cumsum[0],  # item index upper bound
            )
        else:
            last_chunk = (
                igrp,
                len_cumsum[igrp - 1],
                len_cumsum[igrp],
            )

    # index of chunk, item index in chunk, book-keeping for the chosen chunk
    # `last_chunk` is the value to be passed in to this function in the next call.
    return igrp, idx - last_chunk[1], last_chunk


Element = TypeVar("Element")
"""
This type variable is used to annotate the type of a data element.
"""


@runtime_checkable
class Seq(Protocol[Element]):
    """
    The protocol ``Seq`` is simpler and broader than the standard ``collections.abc.Sequence``.
    The former requires only ``__len__``, ``__getitem__``, and ``__iter__``,
    whereas the latter would add ``__contains__``, ``__reversed__``, ``index`` and ``count``
    to these three. Although the extra methods can be implemented using the three basic methods,
    they could be massively inefficient in particular cases, and that is the case
    in the applications targeted by ``biglist``.
    For this reason, the classes defined in this package implement the protocol ``Seq``
    rather than ``Sequence``, to prevent the illusion that methods ``__contains__``, etc.,
    are usable.

    A class that implements this protocol is sized, iterable, and subscriptable.
    This is a subset of the methods provided by Sequence.
    In particular, Sequence implements this protocol, hence is considered a subclass
    of this protocol class for type checking purposes:

    >>> from biglist import Seq
    >>> from collections.abc import Sequence
    >>> issubclass(Sequence, Seq)
    True

    The type parameter ``Element`` indicates the type of each data element.
    """

    # The subclass check is not exactly right.
    # This protocol requires the method ``__getitem__``
    # to take an int key and return an element, but
    # the subclass check does not enforce this signature.
    # For example, dict would pass this check but it is not
    # an intended subclass.

    # An alternative definition is a `Subscriptable` following examples
    # in "cpython/Lib/_collections_abc.py", then a `Seq` inheriting from
    # Sized, Iterable, and Subscriptable.

    def __len__(self) -> int:
        ...

    def __getitem__(self, idx: int) -> Element:
        ...

    def __iter__(self) -> Iterator[Element]:
        # A reference, or naive, implementation.
        for i in range(self.__len__()):
            yield self[i]


SeqType = TypeVar("SeqType", bound=Seq)


# Can not use ``Sequence[T]`` as base class. See
# https://github.com/python/mypy/issues/5264


class ChainedSeq(Generic[SeqType]):
    """
    This class tracks a series of |Sequence|_ to provide
    random element access and iteration on the series as a whole.
    A call to the method :meth:`view` further returns an :class:`SeqView` that
    supports slicing.

    This class operates with zero-copy.

    Note that :class:`SeqView` and :class:`ChainedSeq` are |Sequence|_, hence could be
    members of the series.

    This class is generic with a parameter indicating the type of the member sequences.
    For example,

    ::

        def func(x: ChainedSeq[list[int] | Biglist[int]]):
            ...
    """

    def __init__(self, *lists: SeqType):
        self._lists = lists
        self._lists_len: Optional[list[int]] = None
        self._lists_len_cumsum: Optional[list[int]] = None
        self._len: Optional[int] = None

        # Records info about the last call to `__getitem__`
        # to hopefully speed up the next call, under the assumption
        # that user tends to access consecutive or neighboring
        # elements.
        self._get_item_last_list = None

    def __repr__(self):
        return "<{} with {} elements in {} member lists>".format(
            self.__class__.__name__,
            self.__len__(),
            len(self._lists),
        )

    def __str__(self):
        return self.__repr__()

    def __len__(self) -> int:
        if self._len is None:
            if self._lists_len is None:
                self._lists_len = [len(v) for v in self._lists]
            self._len = sum(self._lists_len)
        return self._len

    def __getitem__(self, idx: int):
        if self._lists_len_cumsum is None:
            if self._lists_len is None:
                self._lists_len = [len(v) for v in self._lists]
            self._lists_len_cumsum = list(itertools.accumulate(self._lists_len))
        ilist, idx_in_list, list_info = locate_idx_in_chunked_seq(
            idx, self._lists_len_cumsum, self._get_item_last_list
        )
        self._get_item_last_list = list_info
        return self._lists[ilist][idx_in_list]

    def __iter__(self):
        for v in self._lists:
            yield from v

    @property
    def raw(self) -> tuple[SeqType, ...]:
        """
        Return the underlying list of |Sequence|_\\s.

        A member sequence could be a :class:`SeqView`. The current method
        does not follow a SeqView to its "raw" component, b/c
        that could represent a different set of elements than the SeqView
        object.
        """
        return self._lists

    def view(self) -> SeqView[ChainedSeq[SeqType]]:
        # The returned object supports slicing.
        return SeqView(self)


class SeqView(Generic[SeqType]):
    """
    This class wraps a :class:`Seq` and enables access by slice or index array,
    in addition to single-index access.

    A SeqView object does "zero-copy"---it keeps track of
    indices of selected elements along with a reference to
    the underlying Seq. This object may be sliced again in a repeated "zoom in" fashion.
    Only when a single-element access or an iteration is performed, the relevant elements
    are retrieved from the underlying Seq.

    This class is generic with a parameter indicating the type of the underlying Seq.
    For example, you can write::

        def func(x: SeqView[Biglist[int]]):
            ...
    """

    def __init__(self, list_: SeqType, range_: Optional[range | Seq[int]] = None):
        """
        This provides a "window" into the Seq ``list_``,
        which may be another :class:`SeqView` (which *is* a Seq, hence
        no special treatment is needed).

        During the use of this object, the underlying ``list_`` must remain unchanged.

        If ``range_`` is ``None``, the "window" covers the entire ``list_``.
        """
        self._list = list_
        self._range = range_

    def __repr__(self):
        return f"<{self.__class__.__name__} into {self.__len__()}/{len(self._list)} of {self._list}>"

    def __str__(self):
        return self.__repr__()

    def __len__(self) -> int:
        """Number of elements in the current window."""
        if self._range is None:
            return len(self._list)
        return len(self._range)

    def __getitem__(self, idx: int | slice | Seq[int]):
        """
        Element access by a single index, slice, or an index array.
        Negative index and standard slice syntax work as expected.

        Single-index access returns the requested data element.
        Slice and index-array access return a new :class:`SeqView` object.
        """
        if isinstance(idx, int):
            # Return a single element.
            if self._range is None:
                return self._list[idx]
            return self._list[self._range[idx]]

        # Return a new `SeqView` object below.

        if isinstance(idx, slice):
            if self._range is None:
                range_ = range(len(self._list))[idx]
            else:
                range_ = self._range[idx]
            return self.__class__(self._list, range_)

        # `idx` is a list of indices.
        if self._range is None:
            return self.__class__(self._list, idx)
        return self.__class__(self._list, [self._range[i] for i in idx])

    def __iter__(self):
        """Iterate over the elements in the current window."""
        if self._range is None:
            yield from self._list
        else:
            # This could be inefficient, depending on
            # the random-access performance of `self._list`.
            for i in self._range:
                yield self._list[i]

    @property
    def raw(self) -> SeqType:
        """The underlying data :class:`Seq`_."""
        return self._list

    @property
    def range(self) -> range | Seq[int]:
        """The current "window" represented by a `range <https://docs.python.org/3/library/stdtypes.html#range>`_ or a list of indices."""
        return self._range

    def collect(self) -> list:
        """
        Return a list containing the elements in the current window.
        This is equivalent to using the object to initialize a list.

        Warning: don't do this on "big" data!
        """
        return list(self)


ListView = SeqView
"""
An alias to :class:`SeqView` for back compat.
This alias will be removed in version 0.8.0.
"""

ChainedList = ChainedSeq
"""
An alias to :class:`ChainedSeq` for back compat.
This alias will be removed in version 0.8.0.
"""
