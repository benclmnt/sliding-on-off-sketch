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


@dataclass(eq=True, frozen=True, order=True)
class StateCounter:
    state: int = field(default=ON, compare=False)
    counter: int = 0

    def increment(self) -> "StateCounter":
        if self.state == OFF:
            return self

        return StateCounter(OFF, self.counter + 1)

    def reset_state(self) -> "StateCounter":
        """Set state fields to ON"""
        return StateCounter(ON, self.counter)

    def copy(self) -> "StateCounter":
        """Returns a copy of itself"""
        return StateCounter(self.state, self.counter)


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
                lambda x: x.counter,
                [self.counters[i][self.hash_fns[i](x)] for i in range(self.d)],
            )
        )


@dataclass
class SI_PE(PE):
    pass


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
        buckets: [
                {
                    value1: (ON, 4),
                    value2: (ON, 3),
                }
            ]
        """
        self.hash_fn = h
        self.counters = [StateCounter() for _ in range(self.l)]
        self.buckets = [dict() for _ in range(self.l)]

    def insert(self, x):
        hash_val = self.hash_fn(x)

        # Check if x is recorded in bucket
        if x in self.buckets[hash_val]:
            self.buckets[hash_val][x] = self.buckets[hash_val][x].increment()
            return

        # x is not separately recorded. Increment counter
        self.counters[hash_val] = self.counters[hash_val].increment()

        if len(self.buckets[hash_val]) < self.w:
            self.buckets[hash_val][x] = self.counters[hash_val].copy()
            return

        # Find min counter currently stored in buckets,
        min_counter, min_counter_key = min(
            map(lambda x: (x[1], x[0]), self.buckets[hash_val].items())
        )

        # if current counter > min counter stored in bucket, swao the counters
        if self.counters[hash_val].counter > min_counter.counter:
            self.buckets[hash_val][x] = self.counters[hash_val]
            self.counters[hash_val] = min_counter
            del self.buckets[hash_val][min_counter_key]

    def query(self, threshold):
        """Get all items that appears > threshold times"""
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


def pe_simple_test():
    pe = PE(3, 5)
    pe.insert(1)
    pe.insert(2)
    print(f"-> {pe = }")
    pe.new_window()
    pe.insert(11)
    print(f" -> {pe.query(2) = }")
    print(f" -> {pe.query(3) = }")
    print(f" -> {pe.query(6) = }")


def fpi_simple_test():
    fpi = FPI(5, 1, lambda x: x % 5)
    fpi.insert(1)
    fpi.insert(2)
    print(f"-> {fpi = }")
    fpi.new_window()
    print(f"-> {fpi = }")
    fpi.insert(11)
    print(f"-> {fpi = }")
    fpi.insert(1)
    fpi.insert(1)
    print(f"-> {fpi = }")
    fpi.new_window()
    fpi.insert(1)
    print(f"-> {fpi = }")
    print(f" -> {fpi.query(1) = }")
    print(f" -> {fpi.query(2) = }")


if __name__ == "__main__":
    fpi_simple_test()
