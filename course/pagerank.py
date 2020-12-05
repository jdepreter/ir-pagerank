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

count = 428136613

# Adjacency list
links = sc.textFile('../data/ClueWeb09_WG_50m_numbered.graph-txt')

links_ranks = links.map(lambda node: (node.split(".")[0], (node.split(".")[1].split(' ') if node.split(".")[1] != '' else [], 1.0/count)))   # [ (source, [destinations]), ... ] 
links_ranks = links_ranks.filter(lambda node: node[0] != '-1')

# [ (source, ([destinations], rank))]

print(links_ranks.take(10))


ITERATIONS = 1
for _ in range(ITERATIONS):

    links_ranks = links_ranks.flatMap(lambda x : [ (i, float(x[1][1])/len(x[1][0])) for i in x[1][0] ]) #.reduceByKey(lambda x,y: x+y)
    # links_ranks.cache()
    print(links_ranks.take(10))

    links_ranks = links_ranks.reduceByKey(lambda x,y: x+y)

    
print(links_ranks.take(10))

# # print(ranks.sortBy(lambda rank: rank[1]).collect())
# print('---')
# print(ranks.take(100))
# sc.stop()