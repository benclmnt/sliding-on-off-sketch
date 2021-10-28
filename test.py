import unittest
from On_Off_Sketch import StateCounter, SlidingStateCounter, ON, OFF, PE, FPI


class StateCounterTest(unittest.TestCase):
    def test_initial_state(self):
        sc = StateCounter()
        self.assertEqual(sc.state, ON)
        self.assertEqual(sc.counter, 0)

    def test_insert(self):
        sc = StateCounter()
        sc.increment()
        self.assertEqual(sc.state, OFF)
        self.assertEqual(sc.counter, 1)
        sc.increment()
        self.assertEqual(sc.state, OFF)
        self.assertEqual(sc.counter, 1)

    def test_reset_state(self):
        sc = StateCounter()
        sc.increment()
        sc.reset_state()
        self.assertEqual(sc.state, ON)
        self.assertEqual(sc.counter, 1)


class SlidingStateCounterTest(unittest.TestCase):
    def test_init(self):
        ssc = SlidingStateCounter(d=2)
        self.assertEqual(ssc.state, ON)
        self.assertEqual(ssc.counter, 0)
        self.assertEqual(ssc.d, 2)


class PE_Test(unittest.TestCase):
    def test_1(self):
        l = 5
        pe = PE(3, l, h=[lambda x: (x + i) % l for i in range(3)])
        pe.insert(1)
        pe.insert(2)
        self.assertEqual(pe.query(6), 1)
        pe.insert(6)
        self.assertEqual(pe.query(6), 1)
        pe.new_window()
        pe.insert(11)
        # print(f"-> {pe = }")
        self.assertEqual(pe.query(1), 2)
        self.assertEqual(pe.query(2), 1)
        self.assertEqual(pe.query(3), 0)
        self.assertEqual(pe.query(1), pe.query(6))


class FPI_Test(unittest.TestCase):
    def test_1(self):
        fpi = FPI(5, 1, lambda x: x % 5)
        fpi.insert(1)
        fpi.insert(2)
        fpi.new_window()
        fpi.insert(11)
        fpi.insert(1)
        fpi.insert(1)
        fpi.insert(7)
        self.assertEqual(fpi.query(1), 2)
        self.assertEqual(fpi.query(2), 1)
        self.assertEqual(fpi.query(7), 0)
        self.assertEqual(fpi.query(3), 0)
        fpi.new_window()
        fpi.insert(1)
        fpi.insert(7)
        self.assertEqual(fpi.query(2), 0)
        self.assertEqual(fpi.query(7), 2)
        self.assertEqual(fpi.find_persistent_above(1), [1, 7])
        self.assertEqual(fpi.find_persistent_above(2), [1])


if __name__ == "__main__":
    unittest.main()
