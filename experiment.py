from On_Off_Sketch import PE, FPI, SI_FPI, SI_PE
from math import ceil

from dataclasses import InitVar, dataclass, field
from typing import Dict, List, Set
from collections import defaultdict
from pprint import pprint
from sklearn.metrics import mean_absolute_error, confusion_matrix

DEBUG = 0


def avg(arr):
    if len(arr) == 0:
        return -1

    return sum(arr) / len(arr)


@dataclass
class Real_PE_FPI:
    current_slice: Set[int] = field(default_factory=set)
    persistence: Dict[int, int] = field(default_factory=lambda: defaultdict(int))

    def insert(self, x):
        self.current_slice.add(int(x))

    def new_slice(self):
        for item in self.current_slice:
            self.persistence[item] += 1
        self.current_slice.clear()

    def query(self, x: int):
        return self.persistence[x] + int(int(x) in self.current_slice)

    def find_persistent_above(self, threshold):
        if threshold == 0:
            return NotImplementedError

        ans = []
        for item, persistency in self.persistence:
            if persistency + int(item in self.current_slice) >= threshold:
                ans.append(item)

        return ans

    def total_persistence(self):
        return sum(v for v in self.persistence.values())


@dataclass
class Sliding_PE_FPI:
    sliding_window_size: int
    history: List[Set[int]] = field(default_factory=list)
    total_persistencies_in_single_window: List[int] = field(default_factory=list)

    def __post_init__(self):
        self.history.append(set())

    def insert(self, x: int):
        self.history[-1].add(int(x))

    def new_slice(self):
        if len(self.history) == self.sliding_window_size:
            self.history.pop(0)
        self.total_persistencies_in_single_window.append(
            sum(len(hist) for hist in self.history)
        )
        self.history.append(set())

    def query(self, x):
        return sum(
            int(x in self.history[i])
            for i in range(min(self.sliding_window_size, len(self.history)))
        )

    def find_persistent_above(self, threshold):
        raise NotImplementedError

    def total_persistence(self):
        return avg(self.total_persistencies_in_single_window)


@dataclass
class Benchmark:
    unique_users: List[int]
    threshold: int
    pe_maes: List[int] = field(default_factory=list)
    fpi_maes: List[int] = field(default_factory=list)
    fpi_fps: List[int] = field(default_factory=list)
    fpi_fns: List[int] = field(default_factory=list)
    queried_in_current_slice: bool = field(default=False)

    def print_results(self):
        print(f"PE_MAE: {avg(self.pe_maes)}")
        print(f"FPI_MAE: {avg(self.fpi_maes)}")
        print(f"FPI_FP: {avg(self.fpi_fps)}")
        print(f"FPI_FN: {avg(self.fpi_fns)}")

    def query_all(self, real, pe, fpi):
        # only query once in a time window
        if self.queried_in_current_slice:
            return

        # PE : query for MAE
        real_pe = [real.query(user) for user in self.unique_users]
        est_pe = [pe.query(user) for user in self.unique_users]
        pe_mae = mean_absolute_error(real_pe, est_pe)
        self.pe_maes.append(pe_mae)

        if DEBUG:
            pprint(pe)
            print(f"{self.unique_users=}\n{real_pe=}\n{est_pe=}\n{pe_mae=}")

        # FPI : query for MAE, false positives, false negatives
        real_fpi = [
            int(real.query(user) >= self.threshold) for user in self.unique_users
        ]
        est_fpi = [int(fpi.query(user) >= self.threshold) for user in self.unique_users]
        fpi_mae = mean_absolute_error(real_fpi, est_fpi)
        _, fp, fn, _ = confusion_matrix(real_fpi, est_fpi, labels=[0, 1]).ravel()
        num_persistent = sum(real_fpi)
        num_non_persistent = len(self.unique_users) - num_persistent

        if DEBUG:
            print(
                f"{self.unique_users=}\n{real_fpi=}\nraw_fpi={[real.query(user) for user in self.unique_users]}\nraw_est_fpi={[fpi.query(user) for user in self.unique_users]}\n{est_fpi=}\n{fpi_mae=} {fp=} {fn=}"
            )
            pprint(fpi)
            print(f"{(num_persistent, num_non_persistent)=}")

        self.fpi_maes.append(fpi_mae)
        if num_non_persistent > 0 and num_persistent > 0:
            self.fpi_fps.append(fp / num_non_persistent)
            self.fpi_fns.append(fn / num_persistent)
        self.queried_in_current_slice = True

    def new_slice(self):
        self.queried_in_current_slice = False


def run(
    filename,
    d,
    L,
    w,
    num_slices,
    start_ts,
    end_ts,
    threshold,
    sliding_window_size=1,
    history=1,  # as if there is no history
):
    # state maintained
    slice_size = ceil((end_ts - start_ts) / num_slices)
    slice_start, slice_end = start_ts, start_ts + slice_size
    # only query after 2 sliding window (2N time slice)
    last_query = slice_end + slice_size

    # print params
    print(
        f"{(filename, d, L, w, num_slices, start_ts, end_ts, slice_size, threshold, sliding_window_size, history)=}"
    )

    # read unique users
    with open(f"{filename}-unique.txt", "r") as f:
        unique_users = list(map(lambda s: int(s.strip()), f.readlines()))

    # our things
    if sliding_window_size == 1:
        pe = PE(d, L)
        fpi = FPI(L, w)
        real = Real_PE_FPI()
    else:
        pe = SI_PE(d, L, history, sliding_window_size)
        fpi = SI_FPI(L, w, history, sliding_window_size)
        real = Sliding_PE_FPI(sliding_window_size)

    bench = Benchmark(unique_users, threshold)

    with open(f"{filename}.txt", "r") as f:
        while True:
            line = f.readline()
            if not line:
                break

            user, _, ts = line.strip().split()
            ts = int(ts)

            # query after sliding window moves N/5
            if ts >= last_query + sliding_window_size * slice_size / 5:
                bench.query_all(real=real, pe=pe, fpi=fpi)
                last_query += sliding_window_size * slice_size / 5

            # update window if necessary
            if ts >= slice_end:
                pe.new_slice()
                fpi.new_slice()
                real.new_slice()
                bench.new_slice()

                # update window
                slice_start, slice_end = slice_end, slice_end + slice_size
                if DEBUG:
                    print(f"\n\n{slice_start=} {slice_end=}")

            pe.insert(user)
            fpi.insert(user)
            real.insert(user)

            if DEBUG >= 2:
                print(user, ts)

        print(f"Total persistence: {real.total_persistence()}")
        bench.print_results()


if __name__ == "__main__":
    # d, L, w = 5, 20, 2
    # num_slices = 5
    # # this is hardcoded. need to look what's the last end_ts.
    # start_ts, end_ts = 1217567877, 1217623216
    # threshold = 2

    # run(
    #     "sample20",
    #     d=2,
    #     L=20,
    #     w=2,
    #     num_slices=10,
    #     start_ts=1217567877,
    #     end_ts=1217623216,  # hardcoded
    #     threshold=2,
    # )

    run(
        "sample100",
        d=2,
        L=20,
        w=2,
        num_slices=10,
        start_ts=1217567877,
        end_ts=1217671224,  # hardcoded
        threshold=2,
        sliding_window_size=4,
        history=1,
    )

    # run(
    #     "sample",
    #     d=2,
    #     L=50,
    #     w=8,
    #     num_slices=20,
    #     start_ts=1217567877,
    #     end_ts=1218036494,  # hardcoded
    #     threshold=2,
    #     sliding_window_size=2,
    # )
