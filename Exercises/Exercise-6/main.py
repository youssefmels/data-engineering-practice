from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import zipfile
import pandas as pd
from datetime import timedelta
import os
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

def read_csv(spark,path1, csvname1, path2, csvname2):
    with zipfile.ZipFile(path1, 'r') as z:
        with z.open(csvname1) as f:
            csv1 = pd.read_csv(f)
    with zipfile.ZipFile(path2, 'r') as z:
        with z.open(csvname2) as f:
            csv2 = pd.read_csv(f)

    schemaQ1 = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", StringType(), True),
    StructField("ended_at", StringType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("start_lat", DoubleType(), True),
    StructField("start_lng", DoubleType(), True),
    StructField("end_lat", DoubleType(), True),
    StructField("end_lng", DoubleType(), True),
    StructField("member_casual", StringType(), True)
])
    schemaQ4 = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("bikeid",IntegerType(), True),
    StructField("tripduration", StringType(), True),
    StructField("from_station_id", IntegerType(), True),
    StructField("from_station_name", StringType(), True),
    StructField("to_station_id", IntegerType(), True),
    StructField("to_station_name", StringType(), True),
    StructField("usertype", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthyear", DoubleType(), True)
])

    df2020Q1 = spark.createDataFrame(csv1, schemaQ1)
    df2019Q4 = spark.createDataFrame(csv2, schemaQ4)

    df2020Q1 = df2020Q1.withColumn("end_station_name", df2020Q1["end_station_name"].cast("string"))
    df2019Q4 = df2019Q4.withColumn("to_station_name", df2019Q4["to_station_name"].cast("string"))
    df2020Q1 = df2020Q1.fillna({"end_station_name": "Unknown"})
    df2019Q4 = df2019Q4.fillna({"to_station_name": "Unknown"})
    
    return df2020Q1, df2019Q4

def saveAsCSV(df, file_name, tag):
    output_dir = "reports"
    output = f"{output_dir}/{file_name}_{tag}"
    df.coalesce(1).write.csv(output, header=True, mode="overwrite")


def getAverageTrip(df, tag):
    df = df.withColumn("start_time",F.to_timestamp("start_time", "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("end_time", F.to_timestamp("end_time", "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("trip_duration", (F.unix_timestamp("end_time") - F.unix_timestamp("start_time")) /60)
    df = df.withColumn("trip_date", F.to_date("start_time"))
    daily_avg_duration = df.groupBy("trip_date").agg(F.avg("trip_duration").alias("average_trip_duration"))

    saveAsCSV(daily_avg_duration,"average_trip_duration_per_day", tag)

def getTripsEachDay(df, tag):
    trips_per_day = df.groupBy(F.to_date(F.to_timestamp("start_time", "yyyy-MM-dd HH:mm:ss")).alias("trip_day")).count()
    saveAsCSV(trips_per_day, "trips_per_day", tag)

def getMostPopStartingTripStationEachMonth(df, tag):
    df = df.withColumn("year", F.year(F.to_timestamp("start_time", "yyyy-MM-dd HH:mm:ss")))
    df = df.withColumn("month", F.month(F.to_timestamp("start_time", "yyyy-MM-dd HH:mm:ss")))

    station_counts= df.groupBy("year", "month", "from_station_name").count()
    window_spec = Window.partitionBy('year','month').orderBy(F.desc('count'))
    most_popular_stations = station_counts.withColumn('rank', F.row_number().over(window_spec))
    most_popular_stations = most_popular_stations.filter(F.col('rank') == 1).drop('rank')

    saveAsCSV(most_popular_stations, "most_popular_stations", tag)

def getTop3TripStationsEachDayLast2Weeks(df, tag):
    df = df.withColumn("date", F.to_date(F.to_timestamp("start_time", "yyyy-MM-dd HH:mm:ss")))
    most_recent_date_row = df.agg(F.max('date').alias('most_recent_date')).collect()[0]
    most_recent_date = most_recent_date_row["most_recent_date"]

    start_date = most_recent_date - timedelta(days = 14)
    filtered_df = df.filter(F.col('date') >= start_date)
    station_counts = filtered_df.groupBy('date', "to_station_name").count()
    top_stations = station_counts.withColumn('rank', F.row_number().over(Window.partitionBy('date').orderBy(F.desc('count')))).filter(F.col('rank') <= 3)
    finalDF = top_stations.select("date","to_station_name", "rank", "count")

    saveAsCSV(finalDF, 'Top3StationsEachDayLast2Weeks', tag)

def doMalesOrFemalesTakeLongerTripsOnAverage(df, tag):
    if "gender" in df.columns:
        df = df.na.replace("NaN", "Unknown").filter(F.col("gender") != "Unknown")
        df = df.withColumn('trip_duration', F.unix_timestamp(F.to_timestamp('end_time', 'yyyy-MM-dd HH:mm:ss')) - F.unix_timestamp(F.to_timestamp('start_time', 'yyyy-MM-dd HH:mm:ss')))
        avg_M_or_F = df.groupBy('gender').agg(F.avg('trip_duration').alias('average_trip_duration'))
        saveAsCSV(avg_M_or_F, 'AvgTripDurationByGender', tag)
    else:
        print("gender column not found.")
    
def top10AgesLongestTripTakers(df, tag):
    if "birthyear" in df.columns:
        df = df.withColumn("date", F.to_date(F.to_timestamp("start_time", "yyyy-MM-dd HH:mm:ss")))
        most_recent_date = df.agg(F.max('date').alias('most_recent_date')).collect()[0]["most_recent_date"]
        df = df.fillna({"birthyear": -1}).filter(F.col("birthyear") != -1)
        df = df.withColumn('age', F.lit(most_recent_date.year) - F.col('birthyear'))
        df = df.filter((F.col("age") > 0) & (F.col("age") <= 120))

        df = df.withColumn('trip_duration', F.unix_timestamp(F.to_timestamp('end_time', 'yyyy-MM-dd HH:mm:ss')) - F.unix_timestamp(F.to_timestamp('start_time', 'yyyy-MM-dd HH:mm:ss')))
        grouped_df = df.groupBy('age').agg(F.avg('trip_duration').alias('avg_trip_duration'))
        sorted_df = grouped_df.orderBy(F.desc('avg_trip_duration'))
        top10Longest = sorted_df.limit(10)
        saveAsCSV(top10Longest, 'Top10LongestTripAges', tag)
    else:
        "Birthyear not found"

def top10AgesShortestTripTakers(df, tag):
    if "birthyear" in df.columns:
        df = df.withColumn("date", F.to_date(F.to_timestamp("start_time", "yyyy-MM-dd HH:mm:ss")))
        most_recent_date = df.agg(F.max('date').alias('most_recent_date')).collect()[0]["most_recent_date"]
        df = df.fillna({"birthyear": -1}).filter(F.col("birthyear") != -1)
        df = df.withColumn('age', F.lit(most_recent_date.year) - F.col('birthyear'))
        df = df.filter((F.col("age") > 0) & (F.col("age") <= 120))

        df = df.withColumn('trip_duration', F.unix_timestamp(F.to_timestamp('end_time', 'yyyy-MM-dd HH:mm:ss')) - F.unix_timestamp(F.to_timestamp('start_time', 'yyyy-MM-dd HH:mm:ss')))
        grouped_df = df.groupBy('age').agg(F.avg('trip_duration').alias('avg_trip_duration'))
        sorted_df = grouped_df.orderBy(F.asc('avg_trip_duration'))
        top10Shortest = sorted_df.limit(10)
        saveAsCSV(top10Shortest, 'Top10ShortestTripAges', tag)
    else:
        "Birthyear not found"

def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    df2020Q1, df2019Q4 = read_csv(spark,'/app/data/Divvy_Trips_2020_Q1.zip', 'Divvy_Trips_2020_Q1.csv', '/app/data/Divvy_Trips_2019_Q4.zip', 'Divvy_Trips_2019_Q4.csv')
    df2020Q1 = df2020Q1.selectExpr(
        "ride_id as trip_id", 
        "rideable_type", 
        "started_at as start_time", 
        "ended_at as end_time",
        "start_station_name as from_station_name", 
        "start_station_id as from_station_id", 
        "end_station_name as to_station_name", 
        "end_station_id as to_station_id",
        "start_lat", 
        "start_lng", 
        "end_lat", 
        "end_lng", 
        "member_casual as usertype"
    )
    df2020Q1 = df2020Q1.fillna({"to_station_name": "Unknown"})
    df2019Q4 = df2019Q4.fillna({"to_station_name": "Unknown"})
    print("-------------------------Placeholder-------------------------")
    #2020 Q1 data
    getAverageTrip(df2020Q1, "2020Q1")
    getTripsEachDay(df2020Q1, "2020Q1")
    getMostPopStartingTripStationEachMonth(df2020Q1, "2020Q1")
    getTop3TripStationsEachDayLast2Weeks(df2020Q1, "2020Q1")
    doMalesOrFemalesTakeLongerTripsOnAverage(df2020Q1, "2020Q1")
    top10AgesLongestTripTakers(df2020Q1, "2020Q1")
    top10AgesShortestTripTakers(df2020Q1, "2020Q1")
    print("-------------------------Placeholder-------------------------")
    #2019 Q4 data
    getAverageTrip(df2019Q4, "2019Q4")
    getTripsEachDay(df2019Q4, "2019Q4")
    getMostPopStartingTripStationEachMonth(df2019Q4, "2019Q4")
    getTop3TripStationsEachDayLast2Weeks(df2019Q4, "2019Q4")
    doMalesOrFemalesTakeLongerTripsOnAverage(df2019Q4, "2019Q4")
    top10AgesLongestTripTakers(df2019Q4, "2019Q4")
    top10AgesShortestTripTakers(df2019Q4, "2019Q4")
    print("-------------------------Placeholder-------------------------")


if __name__ == "__main__":
    main()
