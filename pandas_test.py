import pandas
import numpy as np


links = pandas.read_table("./data/ClueWeb09_WG_50m.graph-txt", sep="\n", header=None, skip_blank_lines=False, nrows=20, names=["out"])
links["index"] = np.arange(links.shape[0])
links["out"] = links["out"].apply(lambda node: [int(i) for i in str(node).split(" ")] if str(node) != "nan" else [])

count = int(links["out"][0][0])
links = links.iloc[1:] # remove first row 

links = links.assign(rank=1.0/count) # sets default chance

print('---')

# links = links.explode("out")
# test = links.groupby(by="out").agg({"rank": "sum"})


print(links.head())



# # print(links.collect())
# print('---')

# # print(ranks.collect())

# print('---')

# print(count)
# print('---')

# ITERATIONS = 10
# for _ in range(ITERATIONS):
#     ranks = links.join(ranks).flatMap(lambda x : [(i, float(x[1][1])/len(x[1][0])) for i in x[1][0]]).reduceByKey(lambda x,y: x+y)
# # print(ranks.sortBy(lambda rank: rank[1]).collect())
# print('---')
# ranks.take(100)