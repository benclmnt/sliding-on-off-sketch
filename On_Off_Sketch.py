"""
CS5234-Mini-Project
Name1: Ma Jiameng (A0198964A)
Name2: Bennett Clement (A0200739J)
Algorithm reference: https://github.com/Sketch-Data-Stream/On-Off-Sketch
"""

from dataclasses import dataclass, field, InitVar
from typing import List, Callable, Dict, Any

# State
ON = 1
OFF = 0


@dataclass(eq=True, frozen=True)
class StateCounter:
    state: int = ON
    counter: int = 0

    def increment(self) -> "SelfCounter":
        if self.state == OFF:
            return self

        return StateCounter(OFF, self.counter + 1)

    def reset_state(self) -> "SelfCounter":
        return StateCounter(ON, self.counter)

    def reset(self) -> "SelfCounter":
        return StateCounter(ON, 0)


def get_hash_fns(d, l):
    # TODO: populate this method
    return []


@dataclass
class PE:
    """
    PE: persistence estimation

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
        self.counters = [[StateCounter()] * self.l for _ in range(self.d)]
        self.hash_fns = get_hash_fns(self.d, self.l)

    def new_window(self):
        for i in range(self.d):
            for j in range(self.l):
                self.counters[i][j] = self.counters[i][j].reset_state()

    def insert(self, x):
        """
        Insert x into counter
        """
        for i in range(self.d):
            hash_val = self.hash_fns[i](x)
            self.counters[i][hash_val] = self.counters[i][hash_val].increment()

    def query(self, x):
        """
        Query x from counter
        """
        return min(
            map(
                lambda x: x[1],
                [self.counters[i][self.hash_fns[i](x)] for i in range(self.d)],
            )
        )


@dataclass
class FPI:
    """
    FPI: finding persistent items
    """

    l: int
    w: int
    hash_fn: Callable = field(init=False)
    buckets: List[Dict[Any, StateCounter]] = field(init=False)
    counters: List[StateCounter] = field(init=False)
    h: InitVar[Callable] = None

    def __post_init__(self, h):
        """
        l: each hash function h_1 : {1...N} -> {1...l}
        ds: [
                {
                    value1: (ON, 4),
                    value2: (ON, 3),
                }
            ]
        """
        self.hash_fn = h
        self.counters = [StateCounter()] * self.l
        self.buckets = [{}] * self.l

    def insert(self, x):
        hash_val = self.hash_fn(x)
        if x in self.buckets[hash_val]:
            self.buckets[hash_val][x] = self.buckets[hash_val][x].increment()
            return

        self.counters[hash_val] = self.counters[hash_val].increment()
        min_in_buckets = min(
            map(lambda k, v: (v.counter, v.state, k), self.buckets[hash_val].values())
        )
        if self.counters[hash_val][1] > min_in_buckets[0]:
            (
                self.buckets[hash_val][min_in_buckets[2]],
                self.counters[hash_val],
            ) = self.counters[hash_val], StateCounter(
                state=min_in_buckets[1], counter=min_in_buckets[0]
            )

    def query(self, threshold):
        res = []
        for i in range(len(self.buckets)):
            for k, v in self.buckets[i].items():
                if v.counter > threshold:
                    res.append(k)
        return res

    def new_window(self):
        for i in range(len(self.buckets)):
            for k, v in self.buckets[i].items():
                self.buckets[i][k] = v.reset_state()

        for i in range(len(self.counters)):
            self.counters[i] = self.counters[i].reset_state()


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
    pe = FPI(5, 3, lambda x: x)
    print(pe)
