import MapReduce
import sys
import re

"""
Frequent Itemsets Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()


# =============================
# Do not modify above this line

def mapper(record):
    elements = record
    for eleindex in range(len(elements)):
        for elepairindex in range(eleindex + 1, len(elements)):
            mr.emit_intermediate((elements[eleindex], elements[elepairindex]), 1)


def reducer(key, list_of_values):
    total = 0
    for v in list_of_values:
        total += v
    if total > 100:
        mr.emit(key)


# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
