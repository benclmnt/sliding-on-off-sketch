"""
CS5234-Mini-Project
Name1: Ma Jiameng (A0198964A)
Name2: Bennett Clement (A0200739J)
Algorithm reference: https://github.com/Sketch-Data-Stream/On-Off-Sketch
"""

from dataclasses import dataclass, field, InitVar
from typing import List, Callable, Dict, Any
from pprint import pprint
import spookyhash

# State
ON = 1
OFF = 0


@dataclass(order=True)
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


@dataclass(order=True)
class SlidingStateCounter(StateCounter):
    """
    d: length of history recorded
    history: a record of the last (d - 1) StateCounter(s)
    """

    history: List[StateCounter] = field(default_factory=list, compare=False)
    d: int = field(default=-1, repr=False)
    _counter: int = field(init=False, repr=False)

    def __post_init__(self):
        if self.d < 0:
            raise ValueError("Please specify value for d")

    # shadows StateCounter's counter.
    @property
    def counter(self) -> int:
        return sum(sc.counter for sc in self.history) + self._counter

    # this function should only be called once by __init__
    @counter.setter
    def counter(self, counter: int):
        # print("Should not be called")
        self._counter = counter

    def increment(self):
        if self.state == OFF:
            return self
        self.state, self._counter = OFF, self._counter + 1
        return self

    def new_day(self):
        """
        New day starts, and all stored information are shifted to be 1 days older
        Current state is reset
        """
        if self.d == 1:
            return

        if len(self.history) == (self.d - 1):
            self.history.pop(0)
        self.history.append(StateCounter(self.state, self._counter))
        self._counter = 0

    def copy(self) -> "SlidingStateCounter":
        # make a deep copy of history
        history_copy = []
        for entry in self.history:
            history_copy.append(entry.copy())
        return SlidingStateCounter(
            state=self.state, counter=self._counter, history=history_copy, d=self.d
        )


def get_hash_fns(d, l):
    # TODO: populate this method
    return [
        lambda x, seed=seed: spookyhash.hash32(str(x).encode(), seed=seed) % l
        for seed in range(d)
    ]


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

    def __post_init__(self):
        self.counters = [[StateCounter() for _ in range(self.l)] for _ in range(self.d)]
        self.hash_fns = get_hash_fns(self.d, self.l)

    def new_slice(self):
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


# Note to self: It is almost always wrong to do self.counter.
# Use self._counter instead!!!
@dataclass
class SI_PE(PE):
    """
    k: number of fields in each bucket, recording information in the last d Days. Increases space usage by d times
    N: sliding window size. Each sliding window contains N time slices.
    next_bucket: which bucket id we should start applying new_day next
    """

    k: int
    N: int
    next_bucket: int = field(init=False)

    def __post_init__(self):
        super().__post_init__()
        self.counters = [
            [SlidingStateCounter(d=self.k) for _ in range(self.l)]
            for _ in range(self.d)
        ]
        self.next_bucket = 0
        self._total_buckets_to_advance = (self.k - 1) * self.l

    def new_slice(self):
        # reset state of all today counters
        super().new_slice()

        # new_day for (k-1)l/N buckets. Assume for now that l > N

        buckets_to_advance = (self._total_buckets_to_advance // self.N) + int(
            self.next_bucket < (self._total_buckets_to_advance % self.N)
        )
        for i in range(self.d):
            for j in range(buckets_to_advance):
                self.counters[i][(self.next_bucket + j) % self.l].new_day()

        self.next_bucket = (self.next_bucket + buckets_to_advance) % self.l


@dataclass
class FPI:
    """
    FPI finding persistent items
    """

    l: int
    w: int
    hash_fn: Callable = field(init=False, repr=False)
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
        self.hash_fn = get_hash_fns(1, self.l)[0]

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
                if v.counter >= threshold:
                    res.append(k)
        return res

    def query(self, x) -> int:
        """Check if x is persistent, if so return the number of time slices it appears, otherwise 0"""
        hash_val = self.hash_fn(x) % self.l
        for k in self.buckets[hash_val].keys():
            if k == str(x):
                return self.buckets[hash_val][str(x)].counter
        return 0

    def new_slice(self):
        for i in range(len(self.buckets)):
            for v in self.buckets[i].values():
                v.reset_state()

        for i in range(len(self.counters)):
            self.counters[i].reset_state()


@dataclass
class SI_FPI(FPI):
    """
    k: number of fields in each bucket, recording information in the last d Days. Increases space usage by d times
    N: sliding window size. Each sliding window contains N time slices.
    next_bucket: which bucket id we should start applying new_day next
    """

    k: int
    N: int
    next_bucket: int = field(init=False)
    buckets: List[Dict[Any, SlidingStateCounter]] = field(init=False)
    counters: List[SlidingStateCounter] = field(init=False)

    def __post_init__(self):
        super().__post_init__()
        self.counters = [SlidingStateCounter(d=self.k) for _ in range(self.l)]
        self.next_bucket = 0
        self._total_buckets_to_advance = (self.k - 1) * self.l

    def new_slice(self):
        # reset state of all today counters
        super().new_slice()

        # new_day for l/N buckets. Assume for now that l > N
        buckets_to_advance = (self._total_buckets_to_advance // self.N) + int(
            self.next_bucket < (self._total_buckets_to_advance % self.N)
        )

        # advances counter + all buckets
        for j in range(buckets_to_advance):
            self.counters[(self.next_bucket + j) % self.l].new_day()
            for bucket in self.buckets[(self.next_bucket + j) % self.l].values():
                bucket.new_day()

        self.next_bucket = (self.next_bucket + buckets_to_advance) % self.l


if __name__ == "__main__":
    sipe = SI_PE(d=2, l=3, k=3, N=2)
    sipe.hash_fns = [lambda x, i=i: (x + i) % 3 for i in range(2)]
    sipe.insert(0)
    print(sipe.counters, "\n")

    sipe.new_slice()  # new_day for x % 3 == 0 and 1
    print("1", sipe.counters, "\n")
    sipe.insert(0)
    print("insert 0", sipe.counters, "\n")
    sipe.new_slice()  # new_day x % 3 == 2
    print("2", sipe.counters, "\n")
    sipe.insert(0)
    print(f"{sipe.query(0)=}")
    sipe.new_slice()  # new_day x % 3 == 0 and 1
    print(f"{sipe.query(0)=}")
    print("3", sipe.counters, "\n")
    sipe.new_slice()  # new_day x % 3 ==
    sipe.new_slice()  # new_day x % 3 == 0 and 1
    print("4", sipe.counters, "\n")
    print(f"{sipe.query(0)=}")

    # sifpi = SI_FPI(l=3, w=2, k=3, N=2)
    # sifpi.hash_fn = lambda x: x % 3
    # sifpi.insert(0)
    # print(sifpi.counters, "\n")

    # sifpi.new_slice()  # new_day for x % 3 == 0 and 1
    # print("1", sifpi.counters, "\n")
    # sifpi.insert(0)
    # print("insert 0", sifpi.counters, "\n")
    # sifpi.new_slice()  # new_day x % 3 == 2
    # print("2", sifpi.counters, "\n")
    # sifpi.insert(0)
    # sifpi.new_slice()  # new_day x % 3 == 0 and 1
    # print("3", sifpi.counters, "\n")
    # sifpi.new_slice()
    # sifpi.new_slice()
    # print("4", sifpi.counters, "\n")
