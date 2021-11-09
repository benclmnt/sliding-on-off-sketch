#! /usr/bin/env python3

import sys

with open(sys.argv[1], "r") as fr, open(sys.argv[1] + "-unique", "w") as fw:
    unique_users = set()
    for line in fr.readlines():
        user, _, _ = line.strip().split()
        if user not in unique_users:
            unique_users.add(user)
    fw.writelines(map(lambda x: x + "\n", unique_users))
