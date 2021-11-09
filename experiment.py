from On_Off_Sketch import PE, FPI
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

    return round(sum(arr) / len(arr), 2)


@dataclass
class Real_PE_FPI:
    current_window: Set[int] = field(default_factory=set)
    persistence: Dict[int, int] = field(default_factory=lambda: defaultdict(int))

    def insert(self, x):
        self.current_window.add(int(x))

    def new_slice(self):
        for item in self.current_window:
            self.persistence[item] += 1
        self.current_window.clear()

    def query(self, x):
        return self.persistence[x] + int(int(x) in self.current_window)

    def find_persistent_above(self, threshold):
        if threshold == 0:
            return NotImplementedError

        ans = []
        for item, persistency in self.persistence:
            if persistency + int(item in self.current_window) >= threshold:
                ans.append(item)

        return ans


@dataclass
class Sliding_PE_FPI:
    current_window: Set[int] = field(default_factory=set)
    persistence: Dict[int, int] = field(default_factory=lambda: defaultdict(int))


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
                f"{self.unique_users=}\n{real_fpi=}\nraw_fpi={[fpi.query(user) for user in self.unique_users]}\n{est_fpi=}\n{fpi_mae=} {fp=} {fn=}"
            )
            pprint(fpi)

        self.fpi_maes.append(fpi_mae)
        if num_non_persistent > 0 and num_persistent > 0:
            self.fpi_fps.append(round(fp / num_non_persistent, 2))
            self.fpi_fns.append(round(fn / num_persistent, 2))
        self.queried_in_current_slice = True

    def new_slice(self):
        self.queried_in_current_slice = False


def run(filename, d, L, w, num_windows, start_ts, end_ts, threshold, history=1):
    # state maintained
    slice_size = ceil((end_ts - start_ts) / num_windows)
    slice_start, slice_end = start_ts, start_ts + slice_size

    # print params
    print(
        f"{(filename, d, L, w, num_windows, start_ts, end_ts, slice_size, threshold, history)=}"
    )

    # read unique users
    with open(f"{filename}-unique.txt", "r") as f:
        unique_users = list(map(lambda s: int(s.strip()), f.readlines()))

    # our things
    pe = PE(d, L)
    fpi = FPI(L, w)
    real = Real_PE_FPI()
    bench = Benchmark(unique_users, threshold)

    with open(f"{filename}.txt", "r") as f:
        while True:
            line = f.readline()
            if not line:
                break

            user, _, ts = line.strip().split()
            ts = int(ts)

            # query every after N/5, and get AAE for PE.
            if ts >= slice_start + slice_size / 5:
                bench.query_all(real=real, pe=pe, fpi=fpi)

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

            if DEBUG:
                print(user, ts)

        bench.print_results()


if __name__ == "__main__":
    # d, L, w = 5, 20, 2
    # num_windows = 5
    # # this is hardcoded. need to look what's the last end_ts.
    # start_ts, end_ts = 1217567877, 1217623216
    # threshold = 2

    # run(
    #     "sample",
    #     d=5,
    #     L=20,
    #     w=2,
    #     num_windows=10,
    #     start_ts=1217567877,
    #     end_ts=1217623216,  # hardcoded
    #     threshold=2,
    # )

    run(
        "sample",
        d=5,
        L=20,
        w=2,
        num_windows=20,
        start_ts=1217567877,
        end_ts=1218036494,  # hardcoded
        threshold=2,
    )
