from biglist import ListView, Biglist


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

    lv = datalv[[1, 3, 5, 7, 9, 10]]
    assert lv[2] == 5
    llv = lv[::2]
    assert llv[2] == 9
    assert list(llv) == [1, 5, 9]
    

def test_listview():
    _test_view(ListView(list(range(20))))


def test_biglistview():
    bl = Biglist.new(storage_format='json')
    bl.extend(range(20))
    bl.flush()
    datalv = bl.view()
    _test_view(datalv)


def test_listview2():
    x = list(range(20))
    z = ListView(x, [2, 3, 5, 6, 13])
    print(z)
    assert z[3] == 6
    assert list(z[1:4]) == [3, 5, 6]
