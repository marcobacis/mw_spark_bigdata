
from datetime import *
import pyspark as sk
import pyspark.sql.types as skt
import functools as ft


def get_data(spark: sk.sql.SparkSession, test: bool) -> sk.sql.DataFrame:
    rootdir = 'data' if not test else 'test_data'
    ext = '.csv.bz2' if not test else '.csv'
    sets = [spark.read.csv(rootdir+'/'+str(year)+ext, header=True) for year in range(1994, 2008+1)]
    return ft.reduce(lambda x, y: x.union(y), sets)


def max_and_min_distance(spark: sk.sql.SparkSession, data: sk.sql.DataFrame):
    def process_row(row):
        try:
            dist = float(row['Distance'])
        except:
            return []
        return [row]

    distances = data.rdd.flatMap(process_row)
    emptydistances = data.rdd.filter(lambda row: len(process_row(row)) == 0)
    maxv = distances.reduce(lambda d1, d2: d1 if float(d1['Distance']) > float(d2['Distance']) else d2)
    minv = distances.reduce(lambda d1, d2: d1 if float(d1['Distance']) < float(d2['Distance']) else d2)
    numnone = emptydistances.map(lambda d: 1).reduce(lambda a, b: a+b)
    print(maxv)
    print(minv)
    print(numnone)


def distances_per_path(spark: sk.sql.SparkSession, data: sk.sql.DataFrame):
    def process_row(row):
        try:
            dist = float(row['Distance'])
            origin = row['Origin'].strip()
            dest = row['Dest'].strip()
            if origin > dest:
                tmp = origin
                origin = dest
                dest = tmp
        except:
            return []
        return [(origin+'-'+dest, {dist})]

    def setunion(v1:set, v2:set):
        return v1.union(v2)

    distperpath = data.rdd.flatMap(process_row)
    uniquedist = distperpath.reduceByKey(setunion)
    numdists = uniquedist.mapValues(lambda v: (len(v), v))
    return numdists


if __name__ == '__main__':
    test = True
    if test:
        addr = "local"
    else:
        addr = "spark://remote:7077"

    spark = sk.sql.SparkSession.builder.master(addr).appName("mw spark bigdata tool").getOrCreate()
    data = get_data(spark, test)
    max_and_min_distance(spark, data)
    res = distances_per_path(spark, data)
    onlymultiple = res.filter(lambda kv: kv[1][0] > 1)
    print(onlymultiple.take(10))





