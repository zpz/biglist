import asyncio
import multiprocessing
import os
import os.path
import pickle
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
from uuid import uuid4

import pyarrow
import pytest
from boltons import iterutils
from cloudly.upathlib import LocalUpath

from biglist import (
    Biglist,
    ParquetBiglist,
    Slicer,
    read_parquet_file,
    write_arrays_to_parquet,
)
from biglist._biglist import (
    ParquetSerializer,
)


def test_custom_file_name():
    out = Biglist.new()
    try:
        print('')
        for _ in range(5):
            buffer_len = random.randint(100, 200)
            fn = out.make_file_name(buffer_len)
            print(fn)
            assert fn.count('_') == 2

        _make_file_name = out.make_file_name
        out.make_file_name = lambda buffer_len: _make_file_name(buffer_len, 'myworker')
        for _ in range(5):
            buffer_len = random.randint(100, 200)
            fn = out.make_file_name(buffer_len)
            print(fn)
            assert fn.count('_') == 3
            assert 'myworker' in fn
    finally:
        out.destroy()


def test_numbers():
    class MyBiglist(Biglist[int]):
        pass

    PATH = os.path.join(os.environ.get('TMPDIR', '/tmp'), 'test', 'biglist-numbers')
    if os.path.isdir(PATH):
        rmtree(PATH)

    mylist = MyBiglist.new(PATH, batch_size=5)
    try:
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
            assert x == data[n], f'n: {n}, x: {x}, data[n]: {data[n]}'
            n += 1

        assert list(mylist) == data
    finally:
        mylist.destroy()


def test_existing_numbers():
    PATH = os.path.join(
        os.environ.get('TMPDIR', '/tmp'), 'test', 'biglist-existing-numbers'
    )
    if os.path.isdir(PATH):
        rmtree(PATH)
    yourlist = Biglist.new(PATH)
    try:
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
    finally:
        yourlist.destroy()


def test_FileReader():
    bl = Biglist.new(batch_size=4, storage_format='pickle')
    try:
        bl.extend(range(22))
        bl.flush()

        vs = bl.files
        assert len(vs) == 6
        assert list(vs[1]) == [4, 5, 6, 7]
        assert list(vs[2]) == [8, 9, 10, 11]
        assert list(vs[2][1:3]) == [9, 10]
    finally:
        bl.destroy()


def test_iter_cancel():
    bl = Biglist.new(batch_size=7)
    try:
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
    finally:
        bl.destroy()


def add_to_biglist(path, prefix, length):
    name = f'{multiprocessing.current_process().name} {threading.current_thread().name}'
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
    bl = Biglist.new(batch_size=6, storage_format='pickle-zstd')
    try:
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
    finally:
        bl.destroy()


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
    bl = Biglist.new(batch_size=5, storage_format='pickle-zstd')
    try:
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
            print('\npath:', bl.path, '\n')
            raise
        assert q_files.empty()
    finally:
        bl.destroy()


def square_sum(x):
    print('process', multiprocessing.current_process())
    z = 0
    for v in x:
        z += v * v
    return z


def test_mp1():
    data = [random.randint(1, 1000) for _ in range(3245)]
    biglist = Biglist.new(batch_size=128)
    try:
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
    finally:
        biglist.destroy()


def find_big(mylist):
    z = Biglist.new(batch_size=20)
    for v in mylist:
        if v > 40:
            z.append(v)
    z.flush()
    return z.path


def test_mp2():
    data = [random.randint(1, 1000) for _ in range(3245)]
    biglist = Biglist.new(batch_size=128)
    try:
        biglist.extend(data)
        biglist.flush()

        yourlist = Biglist.new(batch_size=33)
        try:
            with multiprocessing.get_context('spawn').Pool(10) as pool:
                for path in pool.imap_unordered(find_big, biglist.files):
                    z = Biglist(path)
                    yourlist.extend(z)
                    z.destroy()
            yourlist.flush()

            assert sorted(yourlist) == sorted(v for v in data if v > 40)
        finally:
            yourlist.destroy()
    finally:
        biglist.destroy()


def slow_appender(path):
    bl = Biglist(path)
    for x in range(100):
        time.sleep(random.random() * 0.001)
        bl.append(x)
    bl.flush()


def test_mp3():
    print('')
    cache = Biglist.new(batch_size=1000)
    try:
        ctx = multiprocessing.get_context('spawn')
        workers = [
            ctx.Process(target=slow_appender, args=(cache.path,)) for _ in range(6)
        ]
        for w in workers:
            w.start()
        for w in workers:
            w.join()

        cache.reload()
        print(str(cache))
        print('cache len:', len(cache))

        bl = Biglist(cache.path)
        print('bl len:', len(bl))
    finally:
        cache.destroy()


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
    try:
        biglist.extend(data)
        biglist.flush()

        tasks = (sum_square(x) for x in biglist.files)
        results = await asyncio.gather(*tasks)
        assert sum(results) == sum(v * v for v in biglist)
    finally:
        biglist.destroy()


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
    try:
        bl.extend(data)
        bl.flush()

        print(data[:3])
        print(Slicer(bl)[:3].collect())

        assert list(bl) == data

        assert len(bl) == len(data)
        print('num_data_files:', bl.num_data_files)

        bl2 = ParquetBiglist.new(bl.data_path)
        try:
            assert len(bl2) == len(data)
            assert bl2.num_data_files == bl.num_data_files
            assert list(bl2) == data
        finally:
            bl2.destroy()
    finally:
        bl.destroy()

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
    try:
        b2.extend(data)
        with pytest.raises(pyarrow.lib.ArrowInvalid):
            b2.flush()
    finally:
        b2.destroy()

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
    try:
        b2.extend(data)
        with pytest.raises(pyarrow.lib.ArrowTypeError):
            b2.flush()
            # If schema is not specified, this would not raise, because 'hobbies' is not in the first entry,
            # hence not in the inferred schema, and will be simply ignored.
    finally:
        b2.destroy()

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
    try:
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
    finally:
        b2.destroy()


def test_serializers():
    data = [
        {'a': [9, 10], 'b': {'first': 3, 'second': 2.3}},
        {'a': [11, None], 'b': {'first': 8, 'second': 3.3}},
    ]
    serde = ParquetSerializer
    y = serde.serialize(data)
    z = serde.deserialize(y)
    assert z == data


def test_pickle():
    bb = Biglist.new(batch_size=100)
    try:
        bb.append(25)
        bb.append(46)
        assert len(bb._append_buffer) == 2

        pickled = pickle.dumps(bb)
        unpickled = pickle.loads(pickled)
        assert unpickled.path == bb.path
        assert len(unpickled._append_buffer) == 0
    finally:
        bb.destroy()


def test_parquet_biglist(tmp_path):
    path = LocalUpath(tmp_path)
    path.rmrf(quiet=True)
    path.path.mkdir(parents=True)

    N = 10000

    # key = pyarrow.array([random.randint(0, 10000) for _ in range(N)])
    # val = pyarrow.array([str(uuid4()) for _ in range(N)])
    # tab = pyarrow.Table.from_arrays([key, val], names=['key', 'value'])
    # parquet.write_table(tab, str(path / 'data_1.parquet'))

    write_arrays_to_parquet(
        [
            [random.randint(0, 10000) for _ in range(N)],
            [str(uuid4()) for _ in range(N)],
        ],
        path / 'data_1.parquet',
        names=['key', 'value'],
    )

    # key = pyarrow.array([random.randint(0, 10000) for _ in range(N)])
    # val = pyarrow.array([str(uuid4()) for _ in range(N)])
    # tab = pyarrow.Table.from_arrays([key, val], names=['key', 'value'])
    (path / 'd2').path.mkdir()

    # parquet.write_table(tab, str(path / 'd2' / 'data_2.parquet'))

    write_arrays_to_parquet(
        [
            [random.randint(0, 10000) for _ in range(N)],
            [str(uuid4()) for _ in range(N)],
        ],
        path / 'd2' / 'data_2.parquet',
        names=['key', 'value'],
    )

    biglist = ParquetBiglist.new(path)
    try:
        assert len(biglist) == N + N
        assert len(biglist.files) == 2

        print('')
        print('datafiles')
        z = biglist.files.data_files_info
        print(z)
        print('datafiles_info:\n', z)
        assert all(isinstance(v[0], str) for v in z)
        print('')

        print(biglist[0])
        print(biglist[999])
        print('')

        k = 0
        for z in biglist:
            print(z)
            k += 1
            if k > 20:
                break

        print('')
        z = Slicer(biglist)[100:130:2]
        assert len(z) == 15
        print(z)
        print(z[2])
        print('')
        print(list(z[::3]))

        print('')
        print(biglist)
        print(Slicer(biglist))
        print(biglist.files[0])
        print(biglist.files[1].data)
        print('')
        print(biglist.files[1].data())

        # specify columns
        print('')
        p = biglist.files.data_files_info[0][0]
        d = read_parquet_file(p)
        d1 = d.columns(['key', 'value'])
        print(d1[3])
        d2 = d1.columns(['value'])
        assert isinstance(d2[2], str)
        print(d2[2])
        with pytest.raises(ValueError):
            d2.columns(['key'])
        print(Slicer(d.columns(['key']))[7:17].collect())
        print(list(Slicer(d)[:7]))

        #
        print('')
        d = read_parquet_file(p)
        d.scalar_as_py = False
        assert d.num_columns == 2
        assert d.column_names == ['key', 'value']
        z = d[3]
        print(z)
        assert isinstance(z['key'], pyarrow.Int64Scalar)
        assert isinstance(z['value'], pyarrow.StringScalar)

        # The `pyarrow.Scalar` types compare unequal to Python native types
        assert z['key'] != z['key'].as_py()
        assert z['value'] != z['value'].as_py()

        print('')
        d = read_parquet_file(p)
        z = d[3]
        print(z)
        assert isinstance(z['key'], int)
        assert isinstance(z['value'], str)

        print('')
        d = read_parquet_file(p)
        for k, row in enumerate(d):
            print(row)
            if k > 3:
                break

        print('')
        d = read_parquet_file(p)
        for k, row in enumerate(d):
            print(row)
            if k > 3:
                break

        print('')
        d = read_parquet_file(p)
        d.scalar_as_py = False
        d.load()
        for k, row in enumerate(d):
            print(row)
            if k > 3:
                break
    finally:
        biglist.destroy()


def worker_writer(path, idx):
    bl = Biglist(path)
    for x in range(idx * 100, idx * 100 + 100):
        bl.append(x)
        if (x + 1) % 20 == 0:
            bl.flush(eager=True)


def test_eager_flush():
    bl = Biglist.new(batch_size=5)
    try:
        workers = [
            multiprocessing.Process(target=worker_writer, args=(bl.path, idx))
            for idx in range(4)
        ]
        for w in workers:
            w.start()

        for w in workers:
            w.join()

        assert len(list((bl.path / '_flush_eager').iterdir())) == 4
        assert not bl
        bl.flush()
        assert len(list((bl.path / '_flush_eager').iterdir())) == 0
        assert len(bl) == 400
        assert sorted(bl) == list(range(400))
    finally:
        bl.destroy()
