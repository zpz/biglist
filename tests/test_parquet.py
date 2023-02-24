import io
import pickle
import random
from types import SimpleNamespace
from uuid import uuid4
import pyarrow
from upathlib import LocalUpath
from biglist import ParquetBiglist, ParquetFileReader, write_parquet_file, read_parquet_file, Slicer, ParquetBatchData
import pytest


def test_idx_locator():
    def row_group_sizes(i):
        if i == 0:
            return SimpleNamespace(num_rows=3)
        if i == 1:
            return SimpleNamespace(num_rows=4)
        if i == 2:
            return SimpleNamespace(num_rows=5)
        if i == 3:
            return SimpleNamespace(num_rows=1)
        if i == 4:
            return SimpleNamespace(num_rows=2)
        raise ValueError(i)

    class My:
        def __init__(self):
            self.num_row_groups = 5
            self.metadata = SimpleNamespace(
                row_group=row_group_sizes
            )
            self._row_groups_num_rows = None
            self._row_groups_num_rows_cumsum = None
            self._getitem_last_row_group = None

        def __getitem__(self, idx):
            return ParquetFileReader._locate_row_group_for_item(self, idx)
        
    me = My()
    assert me[0] == (0, 0)
    assert me[1] == (0, 1)
    assert me[2] == (0, 2)
    assert me[3] == (1, 0)
    assert me[4] == (1, 1)
    assert me[5] == (1, 2)
    assert me[6] == (1, 3)
    assert me[7] == (2, 0)
    assert me[8] == (2, 1)
    assert me[9] == (2, 2)
    assert me[10] == (2, 3)
    assert me[11] == (2, 4)
    assert me[12] == (3, 0)
    assert me[13] == (4, 0)
    assert me[14] == (4, 1)
    
    # jump around
    assert me[2] == (0, 2)
    assert me[11] == (2, 4)
    assert me[3] == (1, 0)
    assert me[14] == (4, 1)
    assert me[4] == (1, 1)
    assert me[0] == (0, 0)


def test_parquet_table(tmp_path):
    data = [
        {'key': 'tom', 'value': {'age': 38, 'income': 100, 'address': "NYC"}},
        {'key': 'jane', 'value': {'age': 39, 'income': 200, 'address': "LA"}},
        {'key': 'frank', 'value': {'age': 40, 'income': 300.2, 'address': 'SF'}},
        {'key': 'john', 'value': {'age': 22, 'income': 40, 'address': "DC"}},
    ]
    table = pyarrow.Table.from_pylist(data)
    print(table)
    pdata = ParquetBatchData(table)
    assert len(pdata) == len(data)
    for row in pdata:
        print(row)

    sink = io.BytesIO()
    pw = pyarrow.parquet.ParquetWriter(sink, table.schema)
    pw.write_table(table)
    pw.close()

    # with pyarrow.ipc.new_stream(sink, table.schema) as writer:
    #     print('--- writing ---')
    #     for batch in table.to_batches():
    #         writer.write_batch(batch)

    # sink = io.BytesIO()
    # with pyarrow.output_stream(sink) as writer:
    #     print('--- writing ---')
    #     pyarrow.parquet.write_table(table, writer)
    #     writer.flush()

    # with pyarrow.output_stream(buffer) as stream:  # what about BufferOutputStream?
    #     pyarrow.parquet.write_table(table, stream)

    buf = sink.getvalue()
    # print(buf)
    print(len(buf))

    f = LocalUpath(tmp_path / 'out.parquet')
    f.write_bytes(buf, overwrite=True)

    data3 = read_parquet_file(f)
    for row in data3:
        print(row)

    assert data3.data().data().to_pylist() == data

    print('--- reading ---')
    data2 = pyarrow.parquet.ParquetFile(io.BytesIO(buf)).read()
    assert data2.to_pylist() == data


def test_parquet_biglist(tmp_path):
    path = LocalUpath(tmp_path)
    path.rmrf(quiet=True)
    path.path.mkdir(parents=True)

    N = 10000

    # key = pyarrow.array([random.randint(0, 10000) for _ in range(N)])
    # val = pyarrow.array([str(uuid4()) for _ in range(N)])
    # tab = pyarrow.Table.from_arrays([key, val], names=['key', 'value'])
    # parquet.write_table(tab, str(path / 'data_1.parquet'))

    write_parquet_file(
        path / 'data_1.parquet',
        [
            [random.randint(0, 10000) for _ in range(N)],
            [str(uuid4()) for _ in range(N)],
        ],
        names=['key', 'value']
    )

    # key = pyarrow.array([random.randint(0, 10000) for _ in range(N)])
    # val = pyarrow.array([str(uuid4()) for _ in range(N)])
    # tab = pyarrow.Table.from_arrays([key, val], names=['key', 'value'])
    (path / 'd2').path.mkdir()

    # parquet.write_table(tab, str(path / 'd2' / 'data_2.parquet'))

    write_parquet_file(
        path / 'd2' / 'data_2.parquet',
        [ 
         [random.randint(0, 10000) for _ in range(N)],
         [str(uuid4()) for _ in range(N)],
         ],
        names=['key', 'value']
    )

    biglist = ParquetBiglist.new(path)
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
        d3 = d2.columns(['key'])
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