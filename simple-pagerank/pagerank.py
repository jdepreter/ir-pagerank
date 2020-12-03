import pprint
import findspark
findspark.init()

import pyspark
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession



conf = pyspark.SparkConf().setAppName('appName').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
sc.setSystemProperty('spark.executor.memory', '4g')

spark = SparkSession(sc)

# Adjacency list
links = sc.textFile('partial.txt')
print("yeet")
links = links.zipWithIndex()                                         # (destinations, index)
links = links.map(lambda node: (str(node[1]), node[0].split(" ") if node[0] != '' else []))   # [ (source, [destinations]), ... ] 
count = int(links.first()[1][0])  # (0, ['428136613'])
print("yeet2")

links = links.filter(lambda node: node[0] != '0')          # remove first row 
ranks = links.map(lambda node: (node[0], 1.0/count))    # set default chance.

# print(links.collect())
print('---')

# print(ranks.collect())

print('---')

print(count)
print('---')

ITERATIONS = 10
for _ in range(ITERATIONS):
    ranks = links.join(ranks).flatMap(lambda x : [(i, float(x[1][1])/len(x[1][0])) for i in x[1][0]]).reduceByKey(lambda x,y: x+y)
# print(ranks.sortBy(lambda rank: rank[1]).collect())
print('---')
# print(ranks.take(100))