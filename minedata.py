
from datetime import *
import pyspark as sk
import pyspark.sql.types as skt
import functools as ft


def get_data(spark: sk.sql.SparkSession, test: bool) -> sk.sql.DataFrame:
    rootdir = 'data' if not test else 'test_data'
    ext = '.csv.bz2' if not test else '.csv'
    sets = [spark.read.csv(rootdir+'/'+str(year)+ext, header=True) for year in range(1994, 2008+1)]
    return ft.reduce(lambda x, y: x.union(y), sets)


def week_from_row(row: sk.sql.Row) -> date:
    return date(int(row['Year']), int(row['Month']), int(row['DayofMonth'])) - timedelta(int(row['DayOfWeek'])-1)


def perc_cancelled_flights_per_day(spark: sk.sql.SparkSession, data: sk.sql.DataFrame) -> sk.sql.DataFrame:
    info = data.rdd.map(
        lambda row: (
            date(int(row['Year']), int(row['Month']), int(row['DayofMonth'])),
            (1, int(row['Cancelled']))))
    fractioncancelled = info.reduceByKey(lambda l, r: (l[0]+r[0], l[1]+r[1]))
    return fractioncancelled.mapValues(lambda v: v[1]/v[0]*100.0).sortByKey()


def perc_weather_cancellations_per_week(spark: sk.sql.SparkSession, data: sk.sql.DataFrame) -> sk.sql.DataFrame:
    onlycancelled = data.select(data['Cancelled'] == 1)
    codeperweek = data.rdd.map(
        lambda row: (
            week_from_row(row),
            (1, 1 if (row['CancellationCode'].strip() == 'B') else 0)))
    fractioncancelled = codeperweek.reduceByKey(lambda l, r: (l[0]+r[0], l[1]+r[1]))
    return fractioncancelled.mapValues(lambda v: v[1] / v[0] * 100.0).sortByKey()


def perc_dep_delay_halved_per_group(spark: sk.sql.SparkSession, data: sk.sql.DataFrame) -> sk.sql.DataFrame:
    def process_delay(arrdelay: str, depdelay: str):
        if arrdelay.strip() == 'NA' or depdelay.strip() == 'NA':
            return 0
        return 1 if (float(arrdelay) <= float(depdelay) * 0.5) else 0

    delayhalvedpergroup = data.rdd.map(
        lambda row: (
            max(1, int(row['Distance']) // 100),
            (1, process_delay(row['ArrDelay'], row['DepDelay']))))
    fracdelayhalved = delayhalvedpergroup.reduceByKey(lambda l, r: (l[0]+r[0], l[1]+r[1]))
    return fracdelayhalved.mapValues(lambda v: v[1] / v[0] * 100.0).sortByKey()


def penalty_per_airport(spark: sk.sql.SparkSession, data: sk.sql.DataFrame) -> sk.sql.DataFrame:
    def process_score_term(row: sk.sql.Row):
        week = week_from_row(row)
        res = []
        if row['ArrDelay'].strip() != 'NA':
            res.append(((week, row['Dest'].strip()), 0.5 if float(row['ArrDelay']) >= 15.0 else 0.0))
        if row['DepDelay'].strip() != 'NA':
            res.append(((week, row['Origin'].strip()), 1.0 if float(row['DepDelay']) >= 15.0 else 0.0))
        return res

    termsperweekandport = data.rdd.flatMap(process_score_term)
    scoreperweekandport = termsperweekandport.reduceByKey(lambda l, r: l+r)
    return scoreperweekandport.sortBy(lambda kv: kv[0][0])


if __name__ == '__main__':
    spark = sk.sql.SparkSession.builder.master("local").appName("mw spark bigdata").getOrCreate()
    data = get_data(spark, True)
    print(perc_cancelled_flights_per_day(spark, data).take(10))
    print(perc_weather_cancellations_per_week(spark, data).take(10))
    print(perc_dep_delay_halved_per_group(spark, data).take(10))
    print(penalty_per_airport(spark, data).take(10))

