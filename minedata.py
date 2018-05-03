
from datetime import *
import pyspark as sk
import pyspark.sql.types as skt
import functools as ft


def get_data(spark: sk.sql.SparkSession, test: bool) -> sk.sql.DataFrame:
    rootdir = 'data' if not test else 'test_data'
    ext = '.csv.bz2' if not test else '.csv'
    sets = [spark.read.csv(rootdir+'/'+str(year)+ext, header=True) for year in range(1994, 2008+1)]
    return ft.reduce(lambda x, y: x.union(y), sets)


def perc_cancelled_flights_per_day(spark: sk.sql.SparkSession, data: sk.sql.DataFrame) -> sk.sql.DataFrame:
    info = data.rdd.map(
        lambda row: (date(int(row['Year']), int(row['Month']), int(row['DayofMonth'])), int(row['Cancelled'])))
    fractioncancelled = info.foldByKey((0, 0), lambda l, r: (l[0]+1, l[1]+r))
    return fractioncancelled.mapValues(lambda v: v[1]/v[0]*100.0).sortByKey()


def perc_weather_cancellations_per_week(spark: sk.sql.SparkSession, data: sk.sql.DataFrame) -> sk.sql.DataFrame:
    onlycancelled = data.select(data['Cancelled'] == 1)
    codeperweek = data.rdd.map(
        lambda row: (
            date(int(row['Year']), int(row['Month']), int(row['DayofMonth'])) - timedelta(int(row['DayOfWeek'])-1),
            row['CancellationCode'].strip()))
    fractioncancelled = codeperweek.foldByKey((0, 0), lambda l, r: (l[0] + 1, l[1] + (1 if r == 'B' else 0)))
    return fractioncancelled.mapValues(lambda v: v[1] / v[0] * 100.0).sortByKey()


if __name__ == '__main__':
    spark = sk.sql.SparkSession.builder.master("local").appName("mw spark bigdata").getOrCreate()
    data = get_data(spark, True)
    print(perc_cancelled_flights_per_day(spark, data).take(10))
    print(perc_weather_cancellations_per_week(spark, data).take(10))


