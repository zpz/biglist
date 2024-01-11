from __future__ import annotations

from collections.abc import Iterable, Sequence, Sized

import pytest
from biglist._base import BiglistBase
from biglist._parquet import ParquetBatchData
from biglist._util import (
    Chain,
    Seq,
    Slicer,
    locate_idx_in_chunked_seq,
    FileReader,
    FileSeq,
)


def test_locate_idx_in_chunked_seq():
    len_cumsum = [3, 5, 10, 11, 14]
    x = locate_idx_in_chunked_seq(3, len_cumsum)
    assert x[0] == 1
    assert x[1] == 0
    assert x[2] == (1, 3, 5)

    x = locate_idx_in_chunked_seq(8, len_cumsum, x[2])
    assert x[0] == 2
    assert x[1] == 3
    assert x[2] == (2, 5, 10)

    x = locate_idx_in_chunked_seq(12, len_cumsum, x[2])
    assert x[0] == 4
    assert x[1] == 1
    assert x[2] == (4, 11, 14)


def test_seq():
    assert issubclass(Sequence, Seq)
    assert issubclass(list, Seq)
    assert issubclass(tuple, Seq)
    assert issubclass(Seq, Sized)
    assert issubclass(Seq, Iterable)

    assert issubclass(Chain, Seq)
    assert issubclass(Slicer, Seq)
    assert issubclass(BiglistBase, Seq)
    assert issubclass(FileReader, Seq)
    assert issubclass(FileSeq, Seq)
    assert issubclass(ParquetBatchData, Seq)

    chain = Chain([1, 2, 3], ['a', 'b'])
    assert isinstance(chain, Seq)

    s = Slicer(chain)
    assert isinstance(s, Seq)

    class Numbers(Seq[int]):
        def __len__(self):
            return 4

        def __getitem__(self, idx):
            if idx < -4 or idx >= 4:
                raise KeyError(idx)
            return 3 + idx

    assert issubclass(Numbers, Seq)
    assert not issubclass(Numbers, Sequence)
    nn = Numbers()
    assert [v for v in nn] == [3, 4, 5, 6]

    class Letters:
        def __len__(self):
            return 3

        def __getitem__(self, idx):
            if idx < -3 or idx > 2:
                raise KeyError(idx)

    assert not issubclass(Letters, Seq)

    Letters.__iter__ = Seq.__iter__
    assert not issubclass(Letters, Seq)
    # TODO: why assigning __iter__ this way does not work?

    class Letters:
        def __len__(self):
            return 3

        def __getitem__(self, idx):
            if idx < -3 or idx > 2:
                raise KeyError(idx)

        def __iter__(self):
            for i in range(self.__len__()):
                yield self[i]

    assert issubclass(Letters, Seq)


def test_slicer():
    x = list(range(20))
    datalv = Slicer(x)

    assert datalv.raw is x
    assert datalv.range is None

    data = list(range(20))
    assert list(datalv) == data

    assert datalv[8] == data[8]
    assert datalv[17] == data[17]

    lv = datalv[:9]
    assert lv.range == range(9)

    assert isinstance(lv, Slicer)
    assert list(lv) == data[:9]
    assert lv[-1] == data[8]
    assert lv[3] == data[3]

    lv = lv[:2:-2]
    assert lv.range == range(8, 2, -2)
    assert list(lv) == data[8:2:-2]

    lv = datalv[10:17]
    assert lv.range == range(10, 17)
    assert lv[3] == data[13]
    assert lv[3:6].collect() == data[13:16]
    assert lv[-3:].collect() == data[14:17]
    assert lv[::2].collect() == data[10:17:2]
    assert lv.collect() == data[10:17]

    lv = datalv[::-2]
    assert [v for v in lv] == data[::-2]
    assert list(lv[:3]) == [data[-1], data[-3], data[-5]]
    assert lv[2] == data[-5]
    assert list(lv[::-3]) == data[1::6]

    lv = datalv[[1, 3, 5, 7, 9, 10]]
    assert lv[2] == 5
    llv = lv[::2]
    assert llv[2] == 9
    assert list(llv) == [1, 5, 9]

    x = list(range(20))
    z: Slicer[list[int]] = Slicer(x, [2, 3, 5, 6, 13])
    assert z[3] == 6
    assert z[1:4].collect() == [3, 5, 6]
    assert len(z) == 5


def test_chain():
    mylist1 = list(range(0, 8))
    mylist2 = list(range(8, 18))
    mylist3 = list(range(18, 32))
    mylist: Chain[list[int]] = Chain(mylist1, mylist2, mylist3)
    data = list(range(32))

    assert len(mylist) == len(data)
    assert list(mylist) == data
    assert mylist[3] == data[3]
    assert mylist[12] == data[12]
    assert mylist[17] == data[17]
    assert mylist[23] == data[23]
    assert mylist[-8] == data[-8]
    assert mylist[-17] == data[-17]

    with pytest.raises(IndexError):
        _ = mylist[len(data) + 2]

    with pytest.raises(IndexError):
        _ = mylist[-len(data) - 3]

    ch = Chain([], [], [])
    assert len(ch) == 0

    with pytest.raises(IndexError):
        _ = ch[2]
