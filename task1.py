from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext

def main():
    sc = SparkContext(appName="Ass1")
    lines = sc.textFile(sys.argv[1], 1)

    #Split the csv file using ","
    taxi = lines.map(lambda x: x.split(','))

    count = taxi.map(lambda x: (x[0],x[1])).\
                distinct().\
                map(lambda x: (x[0],1)).\
                reduceByKey(add)
    count = count.top(10, key=lambda x: x[1])
    fh = open(sys.argv[2],"w")
    for i in range(len(count)):
        fh.write(str(count[i]))
        fh.write("\n")
    fh.close()
    return 0

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage:Ass1 <file> <output> ", file=sys.stderr)
        exit(-1)
    main()
