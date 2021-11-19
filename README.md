# nus-cs5234-miniproject

Team:
1. Ma Jiameng (A0198964A)
2. Bennett Clement (A0200739J)

An On-Off-sketch with sliding window implementation in Python

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
