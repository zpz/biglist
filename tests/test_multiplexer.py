import multiprocessing
from time import sleep

from biglist import Multiplexer


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
