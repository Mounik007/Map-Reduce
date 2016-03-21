import MapReduce
import sys
import re

"""
Term Frequency and Document Frequency example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()


# =============================
# Do not modify above this line

# Initialising a mapValue class object
class MapValuesTFDF(object):
    """__init__() functions as class constructor"""

    def __init__(self, fileName=None, wordCount=None):
        self.fileName = fileName
        self.wordCount = wordCount

def mapper(record):
    key = record[0]  # First value of the json object
    value = record[1].lower()  # Second value of the json object
    line = re.findall('\w+', value)
    for word in line:
        mapValueObject = MapValuesTFDF()
        mapValueObject.fileName = key
        mapValueObject.wordCount = line.count(word)
        mr.emit_intermediate(word, mapValueObject)


def reducer(key, list_of_values):
    objectList= []
    total = 0
    uniqueList = []
    for index in range(len(list_of_values)):
        mapObj = MapValuesTFDF()
        mapObj.fileName = list_of_values[index].fileName
        if(mapObj.fileName in uniqueList):
            total += 0
        else:
            total += 1
            objectList.append(list_of_values[index].fileName)
            objectList.append(list_of_values[index].wordCount)
            uniqueList.append(mapObj.fileName)
    mr.emit((key, (total, objectList)))


# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
