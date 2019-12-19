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
document_count = documents_rdd.count()
print(document_count)

# Number of words:
def num_words (x):
    count = 0
    for word in x:
        if ("dis_" in word or "gene_" in word):
            count += 1
    return count
words_per_doc = sc.parallelize(documents_rdd.map(lambda x: num_words(x)).collect())
# [TEST]
# print(words_per_doc.collect())

# Unique Words List
def reduce_words (a, b):
    list_ = []
    for word in a:
        if ("dis_" in word or "gene_" in word):
            list_.append(word)
    for word in b:
        if ("dis_" in word or "gene_" in word):
            list_.append(word)
    return list_

TFIDF_matrix = sc.parallelize(documents_rdd.reduce(reduce_words)).distinct().map(lambda x: (x, {}))
# [TEST]
# print(TFIDF_matrix.collect())


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

TF_term_frequency = sc.parallelize(documents_rdd.collect()).map(lambda document: processDocument_tf(document)).persist(storageLevel=StorageLevel(True, True, False, False, 1))

# [Test Output]
# print(TF_term_frequency.collect())

# # IDF
def processDocument_idf (document):
    map = {}
    for word in document:
        if ("dis_" in word or "gene_" in word):
            if word not in map:
                map[word] = 1
    return map
idf_preprocess = sc.parallelize(documents_rdd.map(lambda document: processDocument_idf(document)).collect())

# [Test Output]
# print(idf_preprocess.collect())

total_documents = TF_term_frequency.count()

# [Test Output]
# print(total_documents)

def reduce_idf (a, b):
    map = {}
    for word in a:
        if word in b:
            map[word] = a[word] + b[word]
            del b[word]
        else:
            map[word] = a[word]
    for word in b:
        map[word] = b[word]
    return map
# IDF_frequency = sc.parallelize(idf_preprocess.reduce(reduce_idf))

# [Test Output]
# print(IDF_frequency.collect())

# for word in IDF_frequency:
#     IDF_frequency[word] = math.log10(total_documents / IDF_frequency[word])

# # TF-IDF
# print(IDF_frequency)
# def process_tf_idf (map_tuple):
#     return_tuple = map_tuple
#     for entry in return_tuple[1]:
#         return_tuple[1][entry] = return_tuple[1][entry] * IDF_frequency[entry]
#     return return_tuple

# tf_idf = sc.parallelize(TF_term_frequency).map(lambda x: process_tf_idf(x)).collect()
# print(tf_idf)







# close Spark
sc.stop()