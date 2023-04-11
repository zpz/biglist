import asyncio
import multiprocessing
import os
import os.path
import queue
import random
import threading
import time
import uuid
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
    wait,
)
from shutil import rmtree
from time import sleep

import pyarrow
import pytest
from biglist import Biglist, Multiplexer, ParquetBiglist, Slicer
from biglist._biglist import (
    JsonByteSerializer,
    ParquetSerializer,
)
from boltons import iterutils


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
    PATH = os.path.join(
        os.environ.get('TMPDIR', '/tmp'), 'test', 'biglist-existing-numbers'
    )
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
    mylist.flush()

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


def iter_file(q_files):
    data = []
    while True:
        try:
            batch = q_files.get_nowait()
        except queue.Empty:
            break
        data.extend(batch)
    return data


def test_file_readers():
    bl = Biglist.new(batch_size=5, storage_format='pickle-z')
    nn = 567
    bl.extend(range(nn))
    bl.flush()

    q_files = multiprocessing.Manager().Queue()
    for f in bl.files:
        q_files.put(f)

    executor = ProcessPoolExecutor(6)
    tasks = [executor.submit(iter_file, q_files) for _ in range(6)]

    data = []
    for t in as_completed(tasks):
        data.extend(t.result())

    try:
        assert sorted(data) == list(bl)
    except AssertionError:
        bl.keep_files = True
        print('\npath:', bl.path, '\n')
        raise
    assert q_files.empty()


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

    with ProcessPoolExecutor(
        3, mp_context=multiprocessing.get_context('spawn')
    ) as pool:
        jobs = [pool.submit(square_sum, v) for v in biglist.files]
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


def slow_appender(path):
    bl = Biglist(path)
    for x in range(100):
        time.sleep(random.random() * 0.001)
        bl.append(x)
    bl.flush()


def test_mp3():
    print('')
    cache = Biglist.new(batch_size=1000)
    ctx = multiprocessing.get_context('spawn')
    workers = [ctx.Process(target=slow_appender, args=(cache.path,)) for _ in range(6)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()

    cache.reload()
    print(str(cache))
    print('cache len:', len(cache))

    bl = Biglist(cache.path)
    print('bl len:', len(bl))


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
    assert sum(results) == sum(v * v for v in biglist)


def mult_worker(path, task_id, q):
    worker_id = multiprocessing.current_process().name
    total = 0
    for x in Multiplexer(path, task_id, worker_id):
        print(worker_id, 'got', x)
        total += x * x
        sleep(0.1)
    print(worker_id, 'finishing with total', total)
    q.put(total)


def test_multiplexer(tmp_path):
    N = 30
    mux = Multiplexer.new(range(1, 1 + N), tmp_path, batch_size=4)
    task_id = mux.start()

    ctx = multiprocessing.get_context('spawn')
    q = ctx.Queue()
    workers = [
        ctx.Process(target=mult_worker, args=(mux.path, task_id, q)) for _ in range(5)
    ]
    for w in workers:
        w.start()
    for w in workers:
        w.join()

    total = 0
    while not q.empty():
        total += q.get()
    assert total == sum(x * x for x in range(1, 1 + N))

    s = mux.stat()
    print(s)
    assert mux.done()


def test_parquet():
    data = [
        {
            'name': str(uuid.uuid1()),
            'age': random.randint(20, 100),
            'address': {
                'state': str(uuid.uuid4()),
                'city': str(uuid.uuid4()),
            },
            'hobbies': [random.random(), random.random()],
        }
        for _ in range(56)
    ]
    bl = Biglist.new(batch_size=13, storage_format='parquet')
    bl.extend(data)
    bl.flush()

    print('')
    print(data[:3])
    print('')
    print('')
    print(Slicer(bl)[:3].collect())
    print('')

    assert list(bl) == data

    print('len:', len(bl))
    assert len(bl) == len(data)
    print('num_data_files:', bl.num_data_files)

    bl2 = ParquetBiglist.new(bl.data_path)
    assert len(bl2) == len(data)
    assert bl2.num_data_files == bl.num_data_files
    assert list(bl2) == data

    data = [
        {'name': 'tom', 'age': 38, 'income': {'concurrency': 'YEN', 'amount': 10000}},
        {
            'name': 'jane',
            'age': 38,
            'income': {'amount': 'a lot'},
            'hobbies': ['soccer', 'swim'],
        },
        {'age': 38, 'hobbies': ['tennis', 'baseball', 0]},
        {'name': 'john', 'age': 38, 'income': {}, 'hobbies': ['soccer', 'swim']},
        {'name': 'paul', 'age': 38, 'income': {'amount': 200}, 'hobbies': ['run']},
    ]
    b2 = Biglist.new(storage_format='parquet')
    b2.extend(data)
    with pytest.raises(pyarrow.lib.ArrowInvalid):
        b2.flush()

    data = [
        {'name': 'tom', 'age': 38, 'income': {'concurrency': 'YEN', 'amount': 10000}},
        {
            'name': 'jane',
            'age': 38,
            'income': {'amount': 250},
            'hobbies': ['soccer', 'swim'],
        },
        {'age': 38, 'hobbies': ['tennis', 'baseball', 0]},
        {'name': 'john', 'age': 20, 'income': {}, 'hobbies': ['soccer', 'swim']},
        {'name': 'paul', 'age': 38, 'income': {'amount': 200}, 'hobbies': ['run']},
    ]
    b2 = Biglist.new(
        storage_format='parquet',
        serialize_kwargs={
            'schema_spec': [
                ('name', 'string', False),
                ('age', 'uint64'),
                (
                    'income',
                    ('struct', [('currency', 'string'), ('amount', 'float64', True)]),
                ),
                ('hobbies', ('list_', 'string')),
            ]
        },
    )
    b2.extend(data)
    with pytest.raises(pyarrow.lib.ArrowTypeError):
        b2.flush()
        # If schema is not specified, this would not raise, because 'hobbies' is not in the first entry,
        # hence not in the inferred schema, and will be simply ignored.

    data = [
        {'name': 'tom', 'age': 38, 'income': {'concurrency': 'YEN', 'amount': 10000}},
        {
            'name': 'jane',
            'age': 38,
            'income': {'amount': 250},
            'hobbies': ['soccer', 'swim'],
        },
        {'age': 38, 'hobbies': ['tennis', 'baseball']},
        {'name': 'john', 'age': 20, 'income': {}, 'hobbies': ['soccer', 'swim']},
        {'name': 'paul', 'age': 38, 'income': {'amount': 200}, 'hobbies': ['run']},
    ]
    schema_spec = [
        ['name', 'string', False],
        ['age', 'uint64'],
        ['income', ['struct', [['currency', 'string'], ['amount', 'float64', True]]]],
        ['hobbies', ['list_', 'string']],
    ]
    b2 = Biglist.new(
        storage_format='parquet',
        serialize_kwargs={'schema_spec': schema_spec},
    )
    b2.extend(data)
    b2.flush()
    for row in b2:
        print(row)

    print('')
    b3 = Biglist(b2.path)
    s = b3.info.get('serialize_kwargs')
    print(s)
    assert s['schema_spec'] == schema_spec
    for irow, row in enumerate(b3):
        print(row)
        assert row == b2[irow]


def test_serializers():
    data = [12, 23.8, {'a': [9, 'xyz'], 'b': {'first': 3, 'second': 2.3}}, None]
    for serde in (JsonByteSerializer,):
        y = serde.serialize(data)
        z = serde.deserialize(y)
        assert z == data

    data = [
        {'a': [9, 10], 'b': {'first': 3, 'second': 2.3}},
        {'a': [11, None], 'b': {'first': 8, 'second': 3.3}},
    ]
    serde = ParquetSerializer
    y = serde.serialize(data)
    z = serde.deserialize(y)
    assert z == data
