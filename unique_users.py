#! /usr/bin/env python3

import sys

filename = sys.argv[1]
with open(f"{filename}.txt", "r") as fr, open(f"{filename}-unique.txt", "w") as fw:
    unique_users = set()
    for line in fr.readlines():
        user, _, _ = line.strip().split()
        if user not in unique_users:
            unique_users.add(user)
    fw.writelines(map(lambda x: x + "\n", unique_users))
