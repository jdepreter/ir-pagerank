from os import link
import pprint
import findspark
findspark.init()

import pyspark
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession



config = pyspark.SparkConf().setAll([('spark.executor.memory', '8g'), ('spark.executor.cores', '4'), ('spark.cores.max', '4'), ('spark.driver.memory','8g')]) \
    .setAppName('appName').setMaster('local[*]')

sc = pyspark.SparkContext(conf=config)

# conf = pyspark.SparkConf().setAppName('appName').setMaster('local')
# pyspark.SparkContext.setSystemProperty('spark.executor.memory', '2g')
# pyspark.SparkContext.setSystemProperty('spark.driver.memory', '2g')

# sc = pyspark.SparkContext(conf=conf)

spark = SparkSession(sc)



# path = "/FileStore/shared_uploads/lander.deroeck@student.uantwerpen.be/web_Google.txt"
path = "../../data/web-Google.txt"

am_nodes = 875713
am_edges = 5105039

edges = sc.textFile(path)
edges = edges.filter(lambda edge: edge[0] != "#").map(lambda edge: tuple(edge.split("\t")))
group_edges = edges.groupBy(lambda x: x[0])
links = group_edges.map(lambda x: (x[0], [i[1] for i in x[1]]))

incoming = links.map(lambda x: x[0])
outgoing = links.flatMap(lambda x: [i for i in x[1]])



# nodes = sc.union([incoming, outgoing]).distinct()
no_outgoing = outgoing.subtract(incoming)

ranks_incoming = incoming.map(lambda x: (x, 1.0 / am_nodes))
ranks_no_outgoing = no_outgoing.map(lambda x: (x, 1.0 / am_nodes))


alpha = float(0.15)    # Teleportation probability

epsilon = 0.000001 # 10^-6


# base_ranks = nodes.map(lambda x: (x, alpha / am_nodes))
base_ranks_incoming = incoming.map(lambda x: (x, alpha / am_nodes))
base_ranks_no_outgoing = no_outgoing.map(lambda x: (x, alpha / am_nodes))


iteration = 0
error = 1

while error > epsilon:
    new_ranks = links.join(ranks_incoming).flatMap(lambda x : [(i, (1-alpha) * float(x[1][1])/len(x[1][0])) for i in x[1][0]])
    # print(new_ranks.count())
    new_ranks = incoming.map(lambda x: (x, 0)).join(new_ranks).map(lambda x: (x[0], x[1][1]))
    # print(new_ranks.take(10))
    # print(new_ranks.count())
    
    new_ranks = sc.union([new_ranks, base_ranks_incoming])
    # print(new_ranks.take(10))
    
    new_ranks = new_ranks.reduceByKey(lambda x,y: x+y)
    error_rdd = new_ranks.union(ranks_incoming).reduceByKey(lambda x, y: abs(x-y)).map(lambda x: x[1])
    # print(error_rdd.take(10))
    error = error_rdd.reduce(max)
    # print(error)
    ranks_incoming = new_ranks
    print(f"Iteration {iteration} with error {error}")
    iteration += 1
    

# Calculate rank of nodes without outgoing nodes #
ranks_no_outgoing = links.join(ranks_incoming).flatMap(lambda x : [(i, (1-alpha) * float(x[1][1])/len(x[1][0])) for i in x[1][0]])
ranks_no_outgoing = no_outgoing.map(lambda x: (x, 0)).join(ranks_no_outgoing).map(lambda x: (x[0], x[1][1]))
ranks_no_outgoing = ranks_no_outgoing.union(base_ranks_no_outgoing)
ranks_no_outgoing = ranks_no_outgoing.reduceByKey(lambda x,y: x+y)


ranks = ranks_incoming.union(ranks_no_outgoing)

ranks = ranks.sortBy(lambda node: -node[1])

df = ranks.toDF()
df.repartition(1).write.csv('noout-alpha0.15.csv')