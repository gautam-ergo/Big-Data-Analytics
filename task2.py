from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext

def main():
    sc = SparkContext(appName="Ass1")
    lines = sc.textFile(sys.argv[1], 1)

    #Split the csv file using ","
    taxi = lines.map(lambda x: x.split(','))

    filtered_taxi = taxi.filter(lambda x: int(x[4]) != 0 ) #make sure the denominator is not ZERO
    count = filtered_taxi.map(lambda x: (x[1],(float(x[16]),int(x[4])))).\
            reduceByKey(lambda k1, k2 : (k1[0] + k2[0], k1[1] + k2[1])).\
            map(lambda x: (x[0],(x[1][0]*60)/x[1][1]))
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
