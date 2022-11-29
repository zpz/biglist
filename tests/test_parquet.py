import random
from types import SimpleNamespace
from uuid import uuid4
import pyarrow
from upathlib import LocalUpath
from biglist import ParquetBiglist, ParquetFileData, write_parquet_file, read_parquet_file
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
            return ParquetFileData._locate_row_group_for_item(self, idx)
        
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


def test_big_parquet_list():
    path = LocalUpath('/tmp/test-biglist/parquet')
    path.rmrf(quiet=True)
    path.localpath.mkdir(parents=True)
    
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
    (path / 'd2').localpath.mkdir()
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
    assert biglist.num_datafiles == 2
    
    print('')
    print('datafiles')
    z = biglist.datafiles
    print(z)
    print('datafiles_info:\n', biglist.datafiles_info)
    assert isinstance(z, list)
    assert all(isinstance(v, str) for v in z)
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
    z = biglist.view()[100:130:2]
    assert len(z) == 15
    print(z)
    print(z[2])
    print('')
    print(list(z[::3]))

    print('')
    for batch in biglist.iter_batches(2000):
        print(batch)

    print('')
    print(biglist)
    print(biglist.view())
    print(biglist.file_view(0))
    print(biglist.file_view(1).data)
    print('')
    print(biglist.file_view(1).data())

    # specify columns
    print('')
    p = biglist._get_data_files()[0][0]['path']
    d = read_parquet_file(p)
    d1 = d.columns(['key', 'value'])
    print(d1[3])
    d2 = d1.columns(['value'])
    assert isinstance(d2[2], str)
    print(d2[2])
    with pytest.raises(ValueError):
        d3 = d2.columns(['key'])
    print(d.columns(['key']).view()[7:17].collect())
    print(list(d.view()[:7]))

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