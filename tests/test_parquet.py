import random
from uuid import uuid4
import pyarrow
from pyarrow import parquet
from upathlib import LocalUpath
from biglist import BigParquetList


def test_big_parquet_list():
    path = LocalUpath('/tmp/test-biglist/parquet')
    path.rmrf()
    path.localpath.mkdir(parents=True)
    
    N = 10000

    key = pyarrow.array([random.randint(0, 10000) for _ in range(N)])
    val = pyarrow.array([str(uuid4()) for _ in range(N)])
    tab = pyarrow.Table.from_arrays([key, val], names=['key', 'value'])
    parquet.write_table(tab, str(path / 'data_1.parquet'))
    
    key = pyarrow.array([random.randint(0, 10000) for _ in range(N)])
    val = pyarrow.array([str(uuid4()) for _ in range(N)])
    tab = pyarrow.Table.from_arrays([key, val], names=['key', 'value'])
    parquet.write_table(tab, str(path / 'data_2.parquet'))

    biglist = BigParquetList.new(path)
    assert len(biglist) == N + N
    print(biglist[0])
    print(biglist[999])
    
    k = 0
    for z in biglist:
        print(z)
        k += 1
        if k > 20:
            break
