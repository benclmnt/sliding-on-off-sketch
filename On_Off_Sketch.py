"""
CS5234-Mini-Project
Name1: Ma Jiameng (A0198964A)
Name2: Bennett Clement (A0200739J)
Algorithm reference: https://github.com/Sketch-Data-Stream/On-Off-Sketch
"""

from dataclasses import dataclass, field, InitVar
from typing import List, Callable, Dict, Any
from pprint import pprint

# State
ON = 1
OFF = 0


@dataclass
class StateCounter:
    state: int = field(default=ON, compare=False)
    counter: int = 0

    def increment(self):
        if self.state == OFF:
            return self
        self.state, self.counter = OFF, self.counter + 1
        return self

    def reset_state(self):
        """Set state fields to ON"""
        self.state = ON
        return self

    def copy(self) -> "StateCounter":
        """Returns a copy of itself"""
        return StateCounter(self.state, self.counter)


@dataclass
class SlidingStateCounter(StateCounter):
    """
    d: length of history recorded
    history: a record of the last (d - 1) StateCounter(s)
    """

    history: List[StateCounter] = field(default_factory=list, compare=False)
    d: int = field(default=-1, repr=False)

    def __post_init__(self):
        if self.d < 0:
            raise ValueError("Please specify value for d")

    @property
    def counter(self) -> int:
        return sum(sc.counter for sc in self.history)

    def new_day(self):
        """
        New day starts, and all stored information are shifted to be 1 days older
        Current state is reset
        """
        if len(self.history) == (self.d - 1):
            self.history.pop(0)
        self.history.append(StateCounter(self.state, self.counter))

        self.state, self.counter = ON, 0

    def copy(self) -> "SlidingStateCounter":
        # make a deep copy of history
        history_copy = []
        for entry in self.history:
            history_copy.append(entry.copy())
        return SlidingStateCounter(self.state, self.counter, history_copy)


def get_hash_fns(d, l):
    # TODO: populate this method
    return [lambda x: (x + i) % l for i in range(d)]


@dataclass
class PE:
    """
    Persistence Estimation

    d: the number of pairwise independent hash functions.
    l: each hash function h_1 : {1...N} -> {1...l}
    counters: [
        [
            (ON, 0),
            (OFF, 1),
            ...
        ],
        [
            (OFF, 3),
            (OFF, 5),
            ...
        ],
        ...
    ]
    """

    d: int
    l: int
    hash_fns: List[Callable] = field(init=False, repr=False)
    counters: List[List[StateCounter]] = field(init=False)
    h: InitVar[List[Callable]]

    def __post_init__(self, h):
        self.counters = [[StateCounter() for _ in range(self.l)] for _ in range(self.d)]
        self.hash_fns = h or get_hash_fns(self.d, self.l)

    def new_window(self):
        for i in range(self.d):
            for j in range(self.l):
                self.counters[i][j].reset_state()

    def insert(self, x):
        """
        Insert x into counter
        """
        for i in range(self.d):
            hash_val = self.hash_fns[i](x) % self.l
            self.counters[i][hash_val].increment()

    def query(self, x):
        """
        Query x from counter
        """
        return min(
            map(
                lambda x: x.counter,
                [self.counters[i][self.hash_fns[i](x) % self.l] for i in range(self.d)],
            )
        )


@dataclass
class SI_PE(PE):
    """
    k: number of fields in each bucket, recording information in the last d Days. Increases space usage by d times
    N: sliding window size. Each sliding window contains N time windows.
    time_window: which time_window it is currently in?
    """

    k: int
    N: int
    time_window: int = field(init=False)

    def __post_init__(self, h):
        self.counters = [
            [SlidingStateCounter(d=self.k) for _ in range(self.l)]
            for _ in range(self.d)
        ]
        self.hash_fns = h
        self.time_window = 0

    def new_window(self):
        # reset state of all today counters
        super().new_window()

        # new_day for l/N buckets. Assume for now that l > N
        buckets_to_advance = self.time_window * (self.l // self.N) + int(
            self.time_window < self.l % self.N
        )
        for i in range(self.d):
            for j in range(buckets_to_advance):
                self.counters[i][self.time_window + j].new_day()

        self.time_window = (self.time_window + 1) % self.N


@dataclass
class FPI:
    """
    FPI finding persistent items
    """

    l: int
    w: int
    hash_fn: Callable = field(repr=False)
    buckets: List[Dict[Any, StateCounter]] = field(init=False)
    counters: List[StateCounter] = field(init=False)

    def __post_init__(self):
        """
        l: each hash function h_1 : {1...N} -> {1...l}
        counters: [
            (ON, 0),
            (OFF, 1),
            ...
        ]
        buckets: [
                {
                    value1: (ON, 4),
                    value2: (ON, 3),
                }
            ]
        """
        self.counters = [StateCounter() for _ in range(self.l)]
        self.buckets = [dict() for _ in range(self.l)]

    def insert(self, x):
        hash_val = self.hash_fn(x) % self.l

        # Check if x is recorded in bucket
        if x in self.buckets[hash_val]:
            self.buckets[hash_val][x].increment()
            return

        if len(self.buckets[hash_val]) < self.w:
            self.buckets[hash_val][x] = self.counters[hash_val].copy()
            self.buckets[hash_val][x].increment()
            return

        # x is not separately recorded. Increment counter
        self.counters[hash_val].increment()

        # Find min counter currently stored in buckets,
        min_counter, min_counter_key = min(
            map(lambda x: (x[1], x[0]), self.buckets[hash_val].items())
        )

        # if current counter > min counter stored in bucket, swao the counters
        if self.counters[hash_val].counter > min_counter.counter:
            self.buckets[hash_val][x] = self.counters[hash_val]
            self.counters[hash_val] = min_counter
            del self.buckets[hash_val][min_counter_key]

    def find_persistent_above(self, threshold):
        """Get all items that appears > threshold times"""
        res = []
        for i in range(len(self.buckets)):
            for k, v in self.buckets[i].items():
                if v.counter > threshold:
                    res.append(k)
        return res

    def query(self, x) -> int:
        """Check if x is persistent, if so return the number of time windows it appears, otherwise 0"""
        hash_val = self.hash_fn(x) % self.l
        for k in self.buckets[hash_val].keys():
            if k == x:
                return self.buckets[hash_val][x].counter
        return 0

    def new_window(self):
        for i in range(len(self.buckets)):
            for v in self.buckets[i].values():
                v.reset_state()

        for i in range(len(self.counters)):
            self.counters[i].reset_state()


@dataclass
class SI_FPI(FPI):
    """
    k: number of fields in each bucket, recording information in the last d Days. Increases space usage by d times
    N: sliding window size. Each sliding window contains N time windows.
    time_window: which time_window it is currently in?
    """

    k: int
    N: int
    time_window: int = field(init=False)
    buckets: List[Dict[Any, SlidingStateCounter]] = field(init=False)
    counters: List[SlidingStateCounter] = field(init=False)

    def __post_init__(self):
        super().__post_init__()
        self.counters = [SlidingStateCounter(d=self.k) for _ in range(self.l)]
        self.time_window = 0

    def new_window(self):
        # reset state of all today counters
        super().new_window()

        # new_day for l/N buckets. Assume for now that l > N
        buckets_to_advance = self.time_window * (self.l // self.N) + int(
            self.time_window < self.l % self.N
        )

        # advances counter + all buckets
        for j in range(buckets_to_advance):
            self.counters[self.time_window + j].new_day()
            for bucket in self.buckets[self.time_window + j].values():
                bucket.new_day()

        self.time_window = (self.time_window + 1) % self.N


class Benchmark:
    """
    some benchmarks about AAE, F1 Score, and throughput
    """

    def __init__(self):
        pass

    def BenchMark(self):
        """ """
        pass

    def SketchError(self):
        """ """
        pass

    def TopKError(self):
        """ """
        pass

    def Thp(self):
        """ """
        pass


class Hash:
    """
    some benchmarks about AAE, F1 Score, and throughput
    """

    def __init__(self):
        pass

    def BOBHash32(self):
        """ """
        pass

    def BOBHash64(self):
        """ """
        pass


if __name__ == "__main__":
    sipe = SI_PE(5, 3, get_hash_fns(5, 3), 1, 2)
    pprint(sipe.counters)
