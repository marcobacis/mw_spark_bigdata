
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


def perc_cancelled_flights_per_day(spark: sk.sql.SparkSession, data: sk.sql.DataFrame) -> sk.RDD:
    info = data.rdd.map(
        lambda row: (
            date(int(row['Year']), int(row['Month']), int(row['DayofMonth'])),
            (1, int(row['Cancelled']))))
    fractioncancelled = info.reduceByKey(lambda l, r: (l[0]+r[0], l[1]+r[1]))
    return fractioncancelled.mapValues(lambda v: v[1]/v[0]*100.0).sortByKey()


def perc_weather_cancellations_per_week(spark: sk.sql.SparkSession, data: sk.sql.DataFrame) -> sk.RDD:
    onlycancelled = data.select(data['Cancelled'] == 1)
    codeperweek = data.rdd.map(
        lambda row: (
            week_from_row(row),
            (1, 1 if (str(row['CancellationCode']).strip() == 'B') else 0)))
    fractioncancelled = codeperweek.reduceByKey(lambda l, r: (l[0]+r[0], l[1]+r[1]))
    return fractioncancelled.mapValues(lambda v: v[1] / v[0] * 100.0).sortByKey()


def perc_dep_delay_halved_per_group(spark: sk.sql.SparkSession, data: sk.sql.DataFrame) -> sk.RDD:
    def process_row(row):
        try:
            distgroup = max(1, int(row['Distance']) // 200)
            arrdelay = float(row['ArrDelay'].strip())
            depdelay = float(row['DepDelay'].strip())
        except:
            return []
        delayfrac = (1, 1 if (arrdelay <= (depdelay * 0.5)) else 0)
        return [(distgroup, delayfrac)]

    delayhalvedpergroup = data.rdd.flatMap(process_row)
    fracdelayhalved = delayhalvedpergroup.reduceByKey(lambda l, r: (l[0]+r[0], l[1]+r[1]))
    return fracdelayhalved.mapValues(lambda v: v[1] / v[0] * 100.0).sortByKey()


def penalty_per_airport(spark: sk.sql.SparkSession, data: sk.sql.DataFrame) -> sk.RDD:
    def process_score_term(row: sk.sql.Row):
        week = week_from_row(row)
        res = []
        if row['ArrDelay'].strip() != 'NA':
            res.append(((week, str(row['Dest']).strip()), 0.5 if float(row['ArrDelay']) >= 15.0 else 0.0))
        if row['DepDelay'].strip() != 'NA':
            res.append(((week, str(row['Origin']).strip()), 1.0 if float(row['DepDelay']) >= 15.0 else 0.0))
        return res

    termsperweekandport = data.rdd.flatMap(process_score_term)
    scoreperweekandport = termsperweekandport.reduceByKey(lambda l, r: l+r)
    return scoreperweekandport.sortBy(lambda kv: kv[0][0])


def to_csv(df: sk.RDD, path: str):
    rows = df.map(lambda p: str(p[0]) + ',' + str(p[1]))
    rows.saveAsTextFile(path)


if __name__ == '__main__':
    import sys

    spark = sk.sql.SparkSession.builder.master("local").appName("mw spark bigdata").getOrCreate()
    data = get_data(spark, True)
    cfpd = perc_cancelled_flights_per_day(spark, data)
    pwcpw = perc_weather_cancellations_per_week(spark, data)
    pddhpg = perc_dep_delay_halved_per_group(spark, data)
    ppa = penalty_per_airport(spark, data)

    print(cfpd.take(10))
    print(pwcpw.take(10))
    print(pddhpg.take(10))
    print(ppa.take(10))

    if len(sys.argv) > 1:
        to_csv(cfpd, sys.argv[1] + '/cfpd.csv')
        to_csv(pwcpw, sys.argv[1] + '/pwcpw.csv')
        to_csv(pddhpg, sys.argv[1] + '/pddhpg.csv')
        to_csv(ppa, sys.argv[1] + '/ppa.csv')


