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
    d = ParquetFileData(biglist.read_parquet_file(p)).select_columns('value')
    assert isinstance(d[2], pyarrow.lib.StringScalar)
    print(d[2])
    print(list(ListView(d)[:7]))

    #
    print('')
    d = ParquetFileData(biglist.read_parquet_file(p))
    print(d[3])

    print('')
    d = ParquetFileData(biglist.read_parquet_file(p),mode=FileLoaderMode.RAND)
    for k, row in enumerate(d):
        print(row)
        if k > 3:
            break

    print('')
    d = ParquetFileData(biglist.read_parquet_file(p),mode=FileLoaderMode.ITER)
    for k, row in enumerate(d):
        print(row)
        if k > 3:
            break
