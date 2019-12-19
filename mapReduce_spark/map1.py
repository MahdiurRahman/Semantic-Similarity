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
document_count = documents_rdd.count()
print(document_count)
####################################

# Number of words:
def num_words (x):
    count = 0
    for word in x:
        if ("dis_" in word or "gene_" in word):
            count += 1
    return count
words_per_doc = sc.parallelize(documents_rdd.map(lambda x: num_words(x)).collect()).persist(storageLevel=StorageLevel(True, True, False, False, 1))
# [TEST]
print(words_per_doc.collect())

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
print(TFIDF_matrix.collect())

###############################

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

term_frequency = documents_rdd.map(lambda document: processDocument_tf(document))

# IDF
def processDocument_idf (document):
    map = {}
    for word in document:
        if ("dis_" in word or "gene_" in word):
            if word not in map:
                map[word] = 1
    return map
idf_preprocess = documents_rdd.map(lambda document: processDocument_idf(document)).collect()
# print(idf_preprocess)

total_documents = term_frequency.count()
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
inverse_document_frequency = sc.parallelize(idf_preprocess).reduce(reduce_idf)
# print(inverse_document_frequency)

for word in inverse_document_frequency:
    inverse_document_frequency[word] = math.log10(total_documents / inverse_document_frequency[word])

# print(inverse_document_frequency)

# TF-IDF
def process_tf_idf (map_tuple):
    return_tuple = map_tuple
    for entry in return_tuple[1]:
        return_tuple[1][entry] = return_tuple[1][entry] * inverse_document_frequency[entry]
    return return_tuple

tf_idf = term_frequency.map(lambda x: process_tf_idf(x)).persist(storageLevel=StorageLevel(True, True, False, False, 1))

# Locally:
tf_idf_local = tf_idf.collect()
TFIDF_matrix_local = TFIDF_matrix.collect()

def form2 (tfidf, matrix):
    semantics = []
    thing = tf_idf.collect()
    for entry in matrix:
        map_ = {}
        for document in thing:
            if (entry[0] in document[1]):
                map_[document[0]] = document[1][entry[0]]
        semantics.append((entry[0], map_))
    return semantics

use_this = form2(tf_idf_local, TFIDF_matrix_local)
print(use_this)

user_input = "dis_breast_cancer_dis"

def formAnswer (word, use_this):
    answer = []
    tuple_ = ()
    for entry in use_this:
        if word != entry[0]:
            tuple_ = entry
    for entry in use_this:
        if word != entry[0]:
            map1 = tuple_[1]
            map2 = entry[1]
            sum = 0
            for item in map1:
                if item in map2:
                    sum += (map1[item] * map2[item])
            square1 = 0
            for item in map1:
                square1 += (map1[item] * map1[item])
            square2 = 0
            for item in map2:
                square2 += (map2[item] * map2[item])
            final = sum / (math.sqrt(square1) * math.sqrt(square2))
            answer.append((entry[0], final))
    return answer

answer = formAnswer(user_input, use_this)
print(answer)


    
    


# def form (word_tuple):
#     list_ = tf_idf.map(lambda x: (x, x[word_tuple[0]]))
#     print(list_.collect())
#     return list_.collect()

# TFIDF_matrix = TFIDF_matrix.map(lambda x: (x, form(x)))
# print(TFIDF_matrix.collect())






# close Spark
sc.stop()