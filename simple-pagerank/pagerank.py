import pprint
import findspark
findspark.init()

import pyspark
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession

inputfile = "../data/ClueWeb09_WG_50m_numbered.graph-txt"
alpha = float(0.15)    # Teleportation probability
epsilon = 0.000001      # 10^-6
outputdir = "big0.15.csv"

config = pyspark.SparkConf().setAll([('spark.executor.memory', '8g'), ('spark.executor.cores', '3'), ('spark.cores.max', '3'), ('spark.driver.memory','8g')]) \
    .setAppName('appName').setMaster('local[*]')

sc = pyspark.SparkContext(conf=config)

spark = SparkSession(sc)

am_nodes = 428136613

# Adjacency list
links = sc.textFile(inputfile)
links = links.map(lambda node: (node.split(".")[0], (node.split(".")[1].split(' ') if node.split(".")[1] != '' else [])))   # [ (source, [destinations]), ... ] 
links = links.filter(lambda node: node[0] != '-1')

ranks = links.map(lambda node: (node[0], 1.0 / am_nodes))

base_ranks = links.map(lambda x: (x[0], alpha / am_nodes))


iteration = 0
error = 1

while error > epsilon:
    new_ranks = links.join(ranks).flatMap(lambda x : [(i, (1-alpha) * float(x[1][1])/len(x[1][0])) for i in x[1][0]])
    # print(new_ranks.take(10))
    
    new_ranks = sc.union([new_ranks, base_ranks])
    # print(new_ranks.take(10))
    
    new_ranks = new_ranks.reduceByKey(lambda x,y: x+y)
    error_rdd = new_ranks.union(ranks).reduceByKey(lambda x, y: abs(x-y)).map(lambda x: x[1])
    # print(error_rdd.take(10))
    error = error_rdd.reduce(max)
    # print(error)
    ranks = new_ranks
    print(f"Iteration {iteration} with error {error}")
    iteration += 1
    break
    
ranks = ranks.sortBy(lambda node: -node[1])
print(ranks.take(10))

df = ranks.toDF()
df.repartition(1).write.csv(outputdir)