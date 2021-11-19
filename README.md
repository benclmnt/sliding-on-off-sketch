# nus-cs5234-miniproject

Team:
1. Ma Jiameng (A0198964A)
2. Bennett Clement (A0200739J)

An On-Off-sketch with sliding window implementation in Python

Largely based on:
1. [Zhang, et al. On-Off Sketch: A Fast and Accurate Sketch on
   Persistence (2021)](https://vldb.org/pvldb/vol14/p128-zhang.pdf)
2. [Gou, et al. Sliding Sketches: A Framework using Time Zones for Data Stream
   Processing in Sliding
   Windows (2020)](https://yangtonghome.github.io/uploads/SlidingSketches_kdd2020.pdf)

## Instructions

0. Setup: Install requirements using `pip install -r requirements.txt`
1. Get your data and put into `data/` folder. We use the Stack Overflow data
   obtained from [Stanford's SNAP](https://snap.stanford.edu/data/sx-stackoverflow.html).
   We also provide a subset of that data in the `data/` folder.
2. Run `python unique_users.py data/[FILE_NAME_WITHOUT_EXTENSION]`
3. To run the experiment, you need to change the filename at the end of the
   `experiment.py` file. Then run `python experiment.py`

Note: The implementation is in `On_Off_Sketch.py` and there are some tests in
`test_sketch.py`
