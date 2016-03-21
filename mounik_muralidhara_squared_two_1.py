import MapReduce
import sys
import re

"""
Matrix 5*5 Multiplication Example in the Simple Python MapReduce Framework using two phase
"""

mr = MapReduce.MapReduce()


# =============================
# Do not modify above this line

class MapValueObject(object):
    """__init__() functions as class constructor"""

    def __init__(self, mapSet=None, mapRowOrColumn = None, mapCellValue = None):
        self.mapSet = mapSet
        self.mapRowOrColumn = mapRowOrColumn
        self.mapCellValue = mapCellValue

def mapper(record):
    elements = record
    mapValueObjA = MapValueObject()
    mapValueObjA.mapSet = "A"
    mapValueObjA.mapRowOrColumn = elements[0]
    mapValueObjA.mapCellValue = elements[2]
    mr.emit_intermediate(elements[1], mapValueObjA)

    mapValueObjB = MapValueObject()
    mapValueObjB.mapSet = "B"
    mapValueObjB.mapRowOrColumn = elements[1]
    mapValueObjB.mapCellValue = elements[2]
    mr.emit_intermediate(elements[0], mapValueObjB)


def reducer(key, list_of_values):
    listA = []
    listB = []
    for index in range(len(list_of_values)):
        if(list_of_values[index].mapSet == "A"):
            listA.append(list_of_values[index])
        else:
            listB.append(list_of_values[index])
    for indexA in range(len(listA)):
        for indexB in range(len(listB)):
            keyValueLst = []
            keyValueLst.append(listA[indexA].mapRowOrColumn)
            keyValueLst.append(listB[indexB].mapRowOrColumn)
            keyValueLst.append((listA[indexA].mapCellValue) * (listB[indexB].mapCellValue))
            mr.emit(keyValueLst)

# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
