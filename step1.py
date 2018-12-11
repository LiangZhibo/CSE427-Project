import sys;
import re;
import matplotlib;
from pyspark import SparkContext;

if __name__ == "__main__":
    sc = SparkContext();
    data_raw = sc.textFile("device_status.txt") \
        .map(lambda line : line.replace("|", ",")) \
        .map(lambda line : line.replace("/", ","));
    num = data_raw.count();
    data_filter = data_raw.map(lambda line: line.split(',')) \
        .filter(lambda res : len(res) == 14 ) \
        .map(lambda tokens : (tokens[12], tokens[13], tokens[0], u'manufacture ' + tokens[1].split(" ")[0], u'model ' + tokens[1].split(" ")[1] , tokens[2])) \
        .filter(lambda tokens : tokens[0] != u'0' or tokens[2] != u'0' ) \
        .map(lambda res : ','.join(str(d) for d in res));
    data_filter.saveAsTextFile("res");
    