import random
from uuid import uuid4
import pyarrow
from pyarrow import parquet
from upathlib import LocalUpath
from biglist import ParquetBiglist, ParquetFileData, ListView
from biglist._base import FileLoaderMode

def test_big_parquet_list():
    path = LocalUpath('/tmp/test-biglist/parquet')
    path.rmrf(quiet=True)
    path.localpath.mkdir(parents=True)
    
    N = 10000

    key = pyarrow.array([random.randint(0, 10000) for _ in range(N)])
    val = pyarrow.array([str(uuid4()) for _ in range(N)])
    tab = pyarrow.Table.from_arrays([key, val], names=['key', 'value'])
    parquet.write_table(tab, str(path / 'data_1.parquet'))

    key = pyarrow.array([random.randint(0, 10000) for _ in range(N)])
    val = pyarrow.array([str(uuid4()) for _ in range(N)])
    tab = pyarrow.Table.from_arrays([key, val], names=['key', 'value'])
    (path / 'd2').localpath.mkdir()
    parquet.write_table(tab, str(path / 'd2' / 'data_2.parquet'))

    biglist = ParquetBiglist.new(path)
    assert len(biglist) == N + N
    assert biglist.num_datafiles == 2
    
    print('')
    print('datafiles')
    z = biglist.datafiles
    print(z)
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
    print(biglist.file_view(1).data.data)

    # specify columns
    print('')
    p = biglist.get_data_files()[0][0]['path']
    d = ParquetFileData(biglist.read_parquet_file(p)).select_columns(['key', 'value'])
    print(d[3])
    d.select_columns('value')
    assert isinstance(d[2], str)
    print(d[2])
    print(list(ListView(d)[:7]))

    #
    print('')
    d = ParquetFileData(biglist.read_parquet_file(p), scalar_as_py=False)
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
    d = ParquetFileData(biglist.read_parquet_file(p))
    z = d[3]
    print(z)
    assert isinstance(z['key'], int)
    assert isinstance(z['value'], str)

    print('')
    d = ParquetFileData(biglist.read_parquet_file(p), eager_load=False)
    for k, row in enumerate(d):
        print(row)
        if k > 3:
            break

    print('')
    d = ParquetFileData(biglist.read_parquet_file(p), eager_load=True)
    for k, row in enumerate(d):
        print(row)
        if k > 3:
            break

    print('')
    d = ParquetFileData(biglist.read_parquet_file(p), eager_load=True, scalar_as_py=False)
    for k, row in enumerate(d):
        print(row)
        if k > 3:
            break