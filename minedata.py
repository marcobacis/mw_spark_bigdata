
import pyspark as sk
import functools as ft


def get_data(sc: sk.sql.SparkSession, test: bool) -> sk.sql.DataFrame:
    rootdir = 'data' if not test else 'test_data'
    ext = '.csv.bz2' if not test else '.csv'
    sets = [sc.read.csv(rootdir+'/'+str(year)+ext) for year in range(1994, 2008-1)]
    return ft.reduce(lambda x, y: x.union(y), sets)


if __name__ == '__main__':
    spark = sk.sql.SparkSession.builder.master("local").appName("mw spark bigdata").getOrCreate()
    data = get_data(spark, True)

    print(data.count())



