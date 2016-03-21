import MapReduce
import sys
import re

"""
Matrix 5*5 multiplication example in the Simple Python MapReduce Framework two phase
"""

mr = MapReduce.MapReduce()


# =============================
# Do not modify above this line

def mapper(record):
    mr.emit_intermediate((record[0], record[1]), record[2])


def reducer(key, list_of_values):
    keylist = []
    total = 0
    for v in list_of_values:
        total += v
    keylist.append(key)
    keylist.append(total)
    mr.emit(keylist)


# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
