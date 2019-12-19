#!/usr/bin/python

import sys
import csv

print("Starting map1.py...")

map = {}

infile = sys.stdin

# Create map
for line in infile:
    data = line.split(' ')
    document = int(data[0])
    for word in data:
        if ("dis_" in word or "gene_" in word):
            found = map.get(word)
            if found:
                found = map[word].get(document)
                if found:
                    map[word][document] += 1
                else:
                    map[word][document] = 1
            else:
                map[word] = {document: 1}

# Output map
for entry in map:
    output = entry + " "
    for item in map[entry]:
        output += (str(item) + " " + str(map[entry][item]) + " ")
    print(output)