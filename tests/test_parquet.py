import io
from types import SimpleNamespace

import pyarrow
from upathlib import LocalUpath

from biglist import (
    ParquetBatchData,
    ParquetFileReader,
    make_parquet_schema,
    make_parquet_type,
    read_parquet_file,
    write_pylist_to_parquet,
)


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
            self.metadata = SimpleNamespace(row_group=row_group_sizes)
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
        {'key': 'tom', 'value': {'age': 38, 'income': 100, 'address': 'NYC'}},
        {'key': 'jane', 'value': {'age': 39, 'income': 200, 'address': 'LA'}},
        {'key': 'frank', 'value': {'age': 40, 'income': 300.2, 'address': 'SF'}},
        {'key': 'john', 'value': {'age': 22, 'income': 40, 'address': 'DC'}},
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


def test_parquet_schema():
    type_spec = (
        'struct',
        [
            ('name', 'string', False),
            ('age', 'uint8', True),
            (
                'income',
                ('struct', (('currency', 'string'), ('amount', 'uint64'))),
                False,
            ),
        ],
    )
    s = make_parquet_type(type_spec)
    assert type(s) is pyarrow.StructType
    print(s)

    type_spec = ('map_', 'string', ('list_', 'int64'), True)
    s = make_parquet_type(type_spec)
    assert type(s) is pyarrow.MapType
    print(s)

    type_spec = ('list_', 'int64')
    s = make_parquet_type(type_spec)
    assert type(s) is pyarrow.ListType
    print(s)

    type_spec = ('list_', ('time32', 's'), 5)
    s = make_parquet_type(type_spec)
    assert type(s) is pyarrow.FixedSizeListType
    print(s)

    schema_spec = [
        ('name', 'string', False),
        ('age', 'uint8', False),
        (
            'income',
            ('struct', (('concurrency', 'string', True), ('amount', 'uint64'))),
            True,
        ),
        ('hobbies', ('list_', 'string'), True),
    ]
    schema = make_parquet_schema(schema_spec)
    assert type(schema) is pyarrow.Schema
    print('')
    print(schema)


def test_write_parquet_file(tmp_path):
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
    pp = tmp_path / 'data.parquet'
    write_pylist_to_parquet(data, pp, schema_spec=schema_spec)
    f = read_parquet_file(pp)
    for row in f:
        print(row)
