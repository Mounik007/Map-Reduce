import MapReduce
import sys
import re

"""
Squaring a 5*5 matrix example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()


# =============================
# Do not modify above this line

class MapValueObject(object):
    """__init__() functions as class constructor"""

    def __init__(self, mapSet=None, mapCommonColumn=None, mapCellValue=None):
        self.mapSet = mapSet
        self.mapCommonColumn = mapCommonColumn
        self.mapCellValue = mapCellValue


def mapper(record):
    elements = record
    for index in range(0, 5):
        valueObject = MapValueObject()
        valueObject.mapSet = "A"
        valueObject.mapCommonColumn = elements[1]
        valueObject.mapCellValue = elements[2]
        mr.emit_intermediate((elements[0], index), valueObject)

    for indec in range(0, 5):
        valueObject = MapValueObject()
        valueObject.mapSet = "B"
        valueObject.mapCommonColumn = elements[0]
        valueObject.mapCellValue = elements[2]
        mr.emit_intermediate((indec, elements[1]), valueObject)


def reducer(key, list_of_values):
    listA = []
    listB = []
    total = 0
    for index in range(len(list_of_values)):
        if list_of_values[index].mapSet == "A":
            listA.append(list_of_values[index])
        else:
            listB.append(list_of_values[index])
    for indexA in range(len(listA)):
        for indexB in range(len(listB)):
            if (listA[indexA].mapCommonColumn == listB[indexB].mapCommonColumn):
                product = listA[indexA].mapCellValue * listB[indexB].mapCellValue
                total += product

    keyList = []
    keyList.append(key)
    keyList.append(total)
    mr.emit(keyList)


# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
