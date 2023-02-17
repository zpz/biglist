import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from biglist import _base, Biglist


def worker(ctx):
    print(mp.current_process().name, list(_base._global_thread_pool_.items()))
    assert len(_base._global_thread_pool_) == 0


def test_mp():
    print(mp.current_process().name, list(_base._global_thread_pool_.items()))

    bl = Biglist.new(batch_size=3)
    bl.extend(range(40))
    bl.flush()
    print('')
    print(mp.current_process().name, list(_base._global_thread_pool_.items()))
    assert len(_base._global_thread_pool_) == 1

    for ctx in ('fork', 'spawn'):
        print('\n----------\nctx', ctx, '\n----------\n')
        with ProcessPoolExecutor(mp_context=mp.get_context(ctx)) as pool:
            t = pool.submit(worker, ctx=ctx)
            t.result()
            assert len(_base._global_thread_pool_) == 1


# if __name__ == '__main__':
#     test_mp()
