import asyncio
import os
import os.path
import multiprocessing
import random
import threading
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed, wait
from shutil import rmtree
from time import sleep

import pytest
from boltons import iterutils
from biglist import Biglist


def test_numbers():
    class MyBiglist(Biglist[int]):
        pass

    PATH = os.path.join(os.environ.get('TMPDIR', '/tmp'), 'test', 'biglist-numbers')
    if os.path.isdir(PATH):
        rmtree(PATH)

    mylist = MyBiglist.new(PATH, batch_size=5)
    for i in range(21):
        mylist.append(i)

    mylist.extend([21, 22, 23, 24, 25])
    mylist.extend([26, 27, 28])
    mylist.flush()

    n = len(mylist.files)
    z = mylist.files.data_files_info
    print('')
    print('num datafiles:', n)
    print('datafiles:\n', z)

    assert len(z) == n
    assert all(isinstance(v[0], str) for v in z)
    print('')

    data = list(range(len(mylist)))
    n = 0
    for x in mylist:
        assert x == data[n], f"n: {n}, x: {x}, data[n]: {data[n]}"
        n += 1

    assert list(mylist) == data


def test_existing_numbers():
    PATH = os.path.join(os.environ.get('TMPDIR', '/tmp'), 'test', 'biglist-existing-numbers')
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


def test_FileReader():
    bl = Biglist.new(batch_size=4, storage_format='pickle')
    bl.extend(range(22))
    bl.flush()

    vs = bl.files
    assert len(vs) == 6
    assert list(vs[1]) == [4, 5, 6, 7]
    assert list(vs[2]) == [8, 9, 10, 11]
    assert list(vs[2][1:3]) == [9, 10]


def test_iter_cancel():
    bl = Biglist.new(batch_size=7)
    bl.extend(range(27))
    bl.flush()
    n = 0
    total = 0
    for x in bl:
        total += x
        n += 1
        if n == 9:
            break
    assert total == sum(range(9))


def add_to_biglist(path, prefix, length):
    name = f"{multiprocessing.current_process().name} {threading.current_thread().name}"
    print('entering', name)
    try:
        bl = Biglist(path)
        for i in range(length):
            bl.append(f'{prefix}-{i}')
        bl.flush()
        print('leaving', name)
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

    print('')
    pool1 = ThreadPoolExecutor(2)
    t1 = pool1.submit(add_to_biglist, bl.path, *sets[1])
    t2 = pool1.submit(add_to_biglist, bl.path, *sets[2])
    pool2 = ProcessPoolExecutor(2, mp_context=multiprocessing.get_context('spawn'))
    t3 = pool2.submit(add_to_biglist, bl.path, *sets[3])
    t4 = pool2.submit(add_to_biglist, bl.path, *sets[4])

    wait([t1, t2, t3, t4])
    bl.reload()

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
    for batch in bl.concurrent_iter_files(task_id):
        data.extend(batch)
    return data


def test_file_readers():
    bl = Biglist.new(batch_size=5, storage_format='pickle-z')
    nn = 567
    bl.extend(range(nn))
    bl.flush()
    task_id = bl.files.new_concurrent_iter()
    print(bl.files.concurrent_iter_done(task_id))

    executor = ProcessPoolExecutor(6)
    tasks = [
        executor.submit(iter_file, bl.path, task_id)
        for _ in range(6)
    ]
    print(bl.files.concurrent_iter_done(task_id))

    data = []
    for t in as_completed(tasks):
        data.extend(t.result())

    try:
        assert sorted(data) == list(bl)
    except AssertionError:
        bl.keep_files = True
        print('\npath:', bl.path, '\n')
        raise
    assert bl.files.concurrent_iter_done(task_id)


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
    assert len(biglist.files) == len(data) // biglist.batch_size + 1

    results = []
    for batch in iterutils.chunked_iter(biglist, biglist.batch_size):
        results.append(square_sum(batch))

    with ProcessPoolExecutor(3, mp_context=multiprocessing.get_context('spawn')) as pool:
        jobs = [
            pool.submit(square_sum, v)
            for v in biglist.files
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
    with multiprocessing.get_context('spawn').Pool(10) as pool:
        for path in pool.imap_unordered(find_big, biglist.files):
            z = Biglist(path)
            yourlist.extend(z)
            del z
    yourlist.flush()

    assert sorted(yourlist) == sorted(v for v in data if v > 40)


async def sum_square(mylist):
    z = 0
    for v in mylist:
        await asyncio.sleep(0.02)
        z += v * v
    return z


@pytest.mark.asyncio
async def test_async():
    data = [random.randint(1, 1000) for _ in range(3245)]
    biglist = Biglist.new(batch_size=128)
    biglist.extend(data)
    biglist.flush()

    tasks = (sum_square(x) for x in biglist.files)
    results = await asyncio.gather(*tasks)
    assert sum(results) == sum(v*v for v in biglist)


def mult_worker(path, task_id, q):
    mux = Biglist(path)
    worker_id = multiprocessing.current_process().name
    total = 0
    for x in mux.multiplex_iter(task_id, worker_id):
        print(worker_id, 'got', x)
        total += x * x
        sleep(0.1)
    print(worker_id, 'finishing with total', total)
    q.put(total)

    
def test_multiplex():
    N = 30
    mux = Biglist.new(batch_size=4)
    mux.extend(range(1, 1 + N))
    mux.flush()
    task_id = mux.new_multiplexer()

    ctx = multiprocessing.get_context('spawn')
    q = ctx.Queue()
    workers = [
        ctx.Process(target=mult_worker, args=(mux.path, task_id, q))
        for _ in range(5)
    ]
    for w in workers:
        w.start()
    for w in workers:
        w.join()

    total = 0
    while not q.empty():
        total += q.get()
    assert total == sum(x*x for x in range(1, 1 + N))

    s = mux.multiplex_stat(task_id)
    print(s)
    assert mux.multiplex_done(task_id)


