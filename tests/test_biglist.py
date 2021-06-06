import os
import os.path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from shutil import rmtree

from biglist._biglist import Biglist, ListView


def test_numbers():
    PATH = os.path.join(os.environ.get('TMPDIR', '/tmp'), 'test', 'biglist')
    if os.path.isdir(PATH):
        rmtree(PATH)

    mylist = Biglist.new(PATH, batch_size=5)
    for i in range(21):
        mylist.append(i)

    mylist.extend([21, 22, 23, 24, 25])
    mylist.extend([26, 27, 28])
    mylist.flush()

    data = list(range(len(mylist)))
    n = 0
    for x in mylist:
        assert x == data[n], f"n: {n}, x: {x}, data[n]: {data[n]}"
        n += 1

    assert list(mylist) == data


def test_view():
    bl = Biglist.new()
    bl.extend(range(20))
    bl.flush()
    datalv = bl.view()

    data = list(range(20))
    assert list(datalv) == data

    assert datalv[8] == data[8]
    assert datalv[17] == data[17]

    lv = datalv[:9]
    assert isinstance(lv, ListView)
    assert list(lv) == data[:9]
    assert lv[-1] == data[8]
    assert lv[3] == data[3]

    lv = lv[:2:-2]
    assert list(lv) == data[8:2:-2]

    lv = datalv[10:17]
    assert lv[3] == data[13]
    assert list(lv[3:6]) == data[13:16]
    assert list(lv[-3:]) == data[14:17]
    assert list(lv[::2]) == data[10:17:2]
    assert list(lv) == data[10:17]

    lv = datalv[::-2]
    assert list(lv) == data[::-2]
    assert list(lv[:3]) == [data[-1], data[-3], data[-5]]
    assert lv[2] == data[-5]
    assert list(lv[::-3]) == data[1::6]


def test_fileview():
    bl = Biglist.new(batch_size=4)
    bl.extend(range(22))
    bl.flush()
    assert len(bl.get_data_files()) == 6

    assert list(bl.file_views()[1]) == [4, 5, 6, 7]

    vs = bl.file_views()
    list(vs[2]) == [8, 9, 10, 11]

    vvs = vs[2][1:3]
    assert list(vvs) == [9, 10]


def add_to_biglist(path, prefix, length):
    try:
        bl = Biglist(path)
        for i in range(length):
            bl.append(f'{prefix}-{i}')
        bl.flush()
        return prefix, length
    except Exception as e:
        print('error:', repr(e), str(e))
        import traceback
        traceback.print_exc()
        raise


def test_multi_workers():
    sets = [('a', 10), ('b', 8), ('c', 22), ('d', 17), ('e', 24)]
    bl = Biglist.new(batch_size=6, keep_files=True)
    # print('bl at', bl.path)

    prefix, ll = sets[0]
    for i in range(ll):
        bl.append(f'{prefix}-{i}')
    bl.flush()

    pool1 = ThreadPoolExecutor(2)
    future1 = [
        pool1.submit(add_to_biglist, bl.path, *sets[1]),
        pool1.submit(add_to_biglist, bl.path, *sets[2]),
    ]
    pool2 = ProcessPoolExecutor(2)
    future2 = [
        pool2.submit(add_to_biglist, bl.path, *sets[3]),
        pool2.submit(add_to_biglist, bl.path, *sets[4]),
    ]

    for t in as_completed(future1 + future2):
        pass

    data = []
    for prefix, ll in sets:
        data.extend(f'{prefix}-{i}' for i in range(ll))
    assert sorted(data) == sorted(bl)
