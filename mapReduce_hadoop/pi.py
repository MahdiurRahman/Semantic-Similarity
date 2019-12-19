import findspark
findspark.init()
import random
from pyspark import SparkContext
sc = SparkContext(appName="Count")

def square (num):
    return num * num

x = sc.parallelize([2, 3, 4]).filter(square).count()
sc.stop()