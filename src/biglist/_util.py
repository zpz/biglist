from __future__ import annotations

import bisect
from collections.abc import Sequence
from typing import Optional


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
         'finishing index (exclusive) of the chosen sequence',
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
                0,  # row-group index
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
    return igrp, idx - last_chunk[1], last_chunk
