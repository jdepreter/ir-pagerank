import numpy as np

with open("./data/ClueWeb09_WG_50m.graph-txt", "r") as file:
    amount = int(file.readline())
    adj_list = dict()

    for i in range(amount):
        line = file.readline()[:-1]
        if line == "":
            continue
        
        adj_list[str(i)] = [int(k) for k in line.split(" ")]

print(adj_list["0"])
print(adj_list["10"])
print(adj_list["100"])
print(adj_list["10000"])