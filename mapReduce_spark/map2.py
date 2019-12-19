# close Spark
try:
    spark.stop()
except:
    pass
import findspark
findspark.init()
from pyspark import SparkContext, StorageLevel
sc = SparkContext("local", "Simple App")

import sys
import csv
import math
            

inFile = sc.textFile(sys.argv[1])
documents_rdd = sc.parallelize(inFile.map(lambda line: line.split(' ')).collect()).persist(storageLevel=StorageLevel(True, True, False, False, 1))
# used StorageLevel = MEMORY_AND_DISK_SER

# TF
def processDocument_tf (document):
    map = {}
    total = 0
    for word in document:
        if ("dis_" in word or "gene_" in word):
            total += 1
            if word in map:
                map[word] += 1
            else:
                map[word] = 1
    for word in map:
        map[word] = map[word] / total
    return (document[0], map)

term_frequency = sc.parallelize(documents_rdd.collect()).map(lambda document: processDocument_tf(document)).persist(storageLevel=StorageLevel(True, True, False, False, 1))

x = term_frequency.collect()
print(x)

# # IDF
# def processDocument_idf (document):
#     map = {}
#     for word in document:
#         if ("dis_" in word or "gene_" in word):
#             if word not in map:
#                 map[word] = 1
#     return map
# idf_preprocess = documents_rdd.map(lambda document: processDocument_idf(document)).collect()

# total_documents = len(term_frequency)
# def reduce_idf (a, b):
#     map = {}
#     for word in a:
#         if word in b:
#             map[word] = a[word] + b[word]
#             del b[word]
#         else:
#             map[word] = a[word]
#     for word in b:
#         map[word] = b[word]
#     return map
# inverse_document_frequency = sc.parallelize(idf_preprocess).reduce(reduce_idf)
# for word in inverse_document_frequency:
#     inverse_document_frequency[word] = math.log10(total_documents / inverse_document_frequency[word])

# # TF-IDF
# print(inverse_document_frequency)
# def process_tf_idf (map_tuple):
#     return_tuple = map_tuple
#     for entry in return_tuple[1]:
#         return_tuple[1][entry] = return_tuple[1][entry] * inverse_document_frequency[entry]
#     return return_tuple

# tf_idf = sc.parallelize(term_frequency).map(lambda x: process_tf_idf(x)).collect()
# print(tf_idf)







# close Spark
sc.stop()