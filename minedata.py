
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


def path_key(row: sk.sql.Row) -> str:
    origin = row['Origin'].strip()
    dest = row['Dest'].strip()
    if origin > dest:
        tmp = origin
        origin = dest
        dest = tmp
    return origin+'-'+dest


def distances_per_path(spark: sk.sql.SparkSession, data: sk.sql.DataFrame) -> sk.RDD:
    def process_row(row):
        try:
            dist = int(row['Distance'])
            pathkey = path_key(row)
        except:
            return []
        return [(pathkey, dist)]

    def setunion(v1:set, v2:set):
        return v1.union(v2)

    distperpath = data.rdd.flatMap(process_row)
    validdists = distperpath.reduceByKey(lambda v1, v2: v1 if v1 == v2 else -999)
    dists = validdists.filter(lambda kv: kv[1] >= 0)
    return dists


def perc_dep_delay_halved_per_group(spark: sk.sql.SparkSession, data: sk.sql.DataFrame) -> sk.RDD:
    dists = distances_per_path(spark, data)
    dist_and_delay_per_path = data.rdd.map(lambda row: (path_key(row), (row['Distance'], row['ArrDelay'], row['DepDelay'])))
    dist_and_delays_per_path = dist_and_delay_per_path.leftOuterJoin(dists)

    def process(kv):
        val = kv[1]
        fixed_distance = val[1]
        dist_arrdelay_depdelay = val[0]
        if not (fixed_distance is None):
            distance = fixed_distance
        else:
            try:
                distance = int(dist_arrdelay_depdelay[0])
            except:
                return []
        try:
            arrdelay = float(dist_arrdelay_depdelay[1].strip())
            depdelay = float(dist_arrdelay_depdelay[2].strip())
        except:
            return []
        distgroup = distance // 200 + 1
        delayfrac = (1, 1 if (arrdelay <= (depdelay * 0.5)) else 0)
        return [(distgroup, delayfrac)]

    delayhalvedpergroup = dist_and_delays_per_path.flatMap(process)
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


def flights_per_path_monthly(spark: sk.sql.SparkSession, data: sk.sql.DataFrame) -> sk.RDD:

    def process_row(row):
        return ((path_key(row), row['Year'], row['Month']), 1)

    def unpack(row):
        return (row[0][0], row[0][1], row[0][2], row[1])

    flighttopath = data.rdd.map(process_row)
    res = flighttopath.reduceByKey(lambda acc, n : acc+n)
    return res


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
    fppm = flights_per_path_monthly(spark,data)

    print(cfpd.take(10))
    print(pwcpw.take(10))
    print(pddhpg.take(10))
    print(ppa.take(10))
    print(fppm.take(10))

    if len(sys.argv) > 1:
        to_csv(cfpd, sys.argv[1] + '/cfpd.csv')
        to_csv(pwcpw, sys.argv[1] + '/pwcpw.csv')
        to_csv(pddhpg, sys.argv[1] + '/pddhpg.csv')
        to_csv(ppa, sys.argv[1] + '/ppa.csv')
        to_csv(fppm, sys.argv[1] + '/fppm.csv')

