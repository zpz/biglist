import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from biglist import Biglist
from upathlib import LocalUpath


def worker(path, idx):
    yourlist = Biglist(path)
    for i in range(idx):
        yourlist.append(100 * idx + i)
    yourlist.flush()


def main():
    path = LocalUpath('/tmp/a/b/c/d')
    path.rmrf()
    yourlist = Biglist.new(path, batch_size=6)

    with ProcessPoolExecutor(10, mp_context=multiprocessing.get_context('spawn')) as pool:
        tasks = [pool.submit(worker, path, idx) for idx in range(10)]
        for t in tasks:
            _ = t.result()


if __name__ == '__main__':
    main()

