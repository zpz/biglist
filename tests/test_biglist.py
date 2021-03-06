import asyncio
import os
import os.path
import multiprocessing
import random
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed, wait
from shutil import rmtree

import pytest
from boltons import iterutils
from biglist._biglist import Biglist, ListView


def test_numbers():
    class MyBiglist(Biglist[int]):
        pass

    PATH = os.path.join(os.environ.get('TMPDIR', '/tmp'), 'test', 'biglist')
    if os.path.isdir(PATH):
        rmtree(PATH)

    mylist = MyBiglist.new(PATH, batch_size=5)
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


def test_existing_numbers():
    PATH = os.path.join(os.environ.get('TMPDIR', '/tmp'), 'test', 'biglist')
    if os.path.isdir(PATH):
        rmtree(PATH)
    yourlist = Biglist.new(PATH)
    yourlist.extend(range(29))
    yourlist.flush()

    mylist = Biglist(PATH)
    mylist.append(29)
    mylist.append(30)
    mylist.append(31)
    mylist.extend([32, 33, 34, 35, 36])

    data = list(range(len(mylist)))
    assert list(mylist) == data

    rmtree(PATH)


def _test_view(datalv):
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


def test_listview():
    _test_view(ListView(list(range(20))))


def test_biglistview():
    bl = Biglist.new(storage_format='json')
    bl.extend(range(20))
    bl.flush()
    datalv = bl.view()
    _test_view(datalv)


def test_fileview():
    bl = Biglist.new(batch_size=4, storage_format='pickle')
    bl.extend(range(22))
    bl.flush()
    assert len(bl.get_data_files()) == 6

    assert list(bl.file_views()[1]) == [4, 5, 6, 7]

    vs = bl.file_views()
    list(vs[2]) == [8, 9, 10, 11]

    vvs = vs[2][1:3]
    assert list(vvs) == [9, 10]


def test_iter_cancel():
    bl = Biglist.new(batch_size=7)
    bl.extend(range(27))
    n = 0
    total = 0
    for x in bl:
        total += x
        n += 1
        if n == 9:
            break
    assert total == sum(range(9))


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


def test_multi_appenders():
    sets = [('a', 10), ('b', 8), ('c', 22), ('d', 17), ('e', 24)]
    bl = Biglist.new(batch_size=6, keep_files=True, storage_format='pickle-z')
    # print('bl at', bl.path)

    prefix, ll = sets[0]
    for i in range(ll):
        bl.append(f'{prefix}-{i}')
    bl.flush()

    pool1 = ThreadPoolExecutor(2)
    t1 = pool1.submit(add_to_biglist, bl.path, *sets[1])
    t2 = pool1.submit(add_to_biglist, bl.path, *sets[2])
    pool2 = ProcessPoolExecutor(2)
    t3 = pool2.submit(add_to_biglist, bl.path, *sets[3])
    t4 = pool2.submit(add_to_biglist, bl.path, *sets[4])

    wait([t1, t2, t3, t4])

    data = []
    for prefix, ll in sets:
        data.extend(f'{prefix}-{i}' for i in range(ll))
    try:
        assert sorted(data) == sorted(bl)
    except:
        print('data:', sorted(data))
        print('bl:', sorted(bl))
        raise


def iter_file(path, task_id):
    bl = Biglist(path)
    data = []
    for x in bl.concurrent_iter(task_id):
        data.append(x)
    return data


def test_file_views():
    bl = Biglist.new(batch_size=5, storage_format='pickle-z')
    nn = 567
    bl.extend(range(nn))
    bl.flush()
    task_id = bl.new_concurrent_iter()
    print(bl.concurrent_iter_done(task_id))

    executor = ProcessPoolExecutor(6)
    tasks = [
        executor.submit(iter_file, bl.path, task_id)
        for _ in range(6)
    ]
    print(bl.concurrent_iter_done(task_id))

    data = []
    for t in as_completed(tasks):
        data.extend(t.result())

    assert sorted(data) == list(bl)
    assert bl.concurrent_iter_done(task_id)
    print(bl.concurrent_iter_done(task_id))


def square_sum(x):
    print('process', multiprocessing.current_process())
    z = 0
    for v in x:
        z += v * v
    return z


def test_mp1():
    data = [random.randint(1, 1000) for _ in range(3245)]
    biglist = Biglist.new(batch_size=128)
    biglist.extend(data)
    biglist.flush()

    print('')
    assert len(biglist.get_data_files()) == len(data) // biglist.batch_size + 1

    results = []
    for batch in iterutils.chunked_iter(biglist, biglist.batch_size):
        results.append(square_sum(batch))

    with ProcessPoolExecutor(3) as pool:
        jobs = [
            pool.submit(square_sum, v)
            for v in biglist.file_views()
        ]
        for j, result in zip(jobs, results):
            assert j.result() == result


def find_big(mylist):
    z = Biglist.new(batch_size=20, keep_files=True)
    for v in mylist:
        if v > 40:
            z.append(v)
    z.flush()
    return z.path


def test_mp2():
    data = [random.randint(1, 1000) for _ in range(3245)]
    biglist = Biglist.new(batch_size=128)
    biglist.extend(data)
    biglist.flush()

    yourlist = Biglist.new(batch_size=33)
    with multiprocessing.Pool(10) as pool:
        for path in pool.imap_unordered(find_big, biglist.file_views()):
            z = Biglist(path)
            yourlist.extend(z)
            z.destroy()
    yourlist.flush()

    assert sorted(yourlist) == sorted(v for v in data if v > 40)
    biglist.destroy()
    yourlist.destroy()


async def sum_square(mylist):
    z = 0
    for v in mylist:
        await asyncio.sleep(0.1)
        z += v * v
    return z


@pytest.mark.asyncio
async def test_async():
    data = [random.randint(1, 1000) for _ in range(3245)]
    biglist = Biglist.new(batch_size=128)
    biglist.extend(data)
    biglist.flush()

    tasks = (sum_square(x) for x in biglist.file_views())
    results = await asyncio.gather(*tasks)
    assert sum(results) == sum(v*v for v in biglist)
    biglist.destroy()
