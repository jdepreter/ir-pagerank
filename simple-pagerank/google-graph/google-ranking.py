import pyspark

path = "/FileStore/shared_uploads/lander.deroeck@student.uantwerpen.be/web_Google.txt"

am_nodes = 875713
am_edges = 5105039

edges = sc.textFile(path)
edges = edges.filter(lambda edge: edge[0] != "#").map(lambda edge: tuple(edge.split("\t")))
group_edges = edges.groupBy(lambda x: x[0])
links = group_edges.map(lambda x: (x[0], [i[1] for i in x[1]]))
ranks = links.map(lambda x: (x[0], 1.0/am_nodes))


print(ranks.take(10))

ITERATIONS = 10
for _ in range(ITERATIONS):
    ranks = links.join(ranks).flatMap(lambda x : [(i, float(x[1][1])/len(x[1][0])) for i in x[1][0]]).reduceByKey(lambda x,y: x+y)
    
print(ranks.sortBy(lambda node: -node[1]).take(10))