import polars as pl

def read_csv_lazily(csv_path):
    lazy_df = pl.scan_csv(
        csv_path,
        dtypes={
            "ride_id": pl.Utf8,
            "rideable_type": pl.Utf8,
            "started_at": pl.Datetime,
            "ended_at": pl.Datetime,
            "start_station_name": pl.Utf8,
            "start_station_id": pl.Int32,
            "end_station_name": pl.Utf8,
            "end_station_id": pl.Int32,
            "start_lat": pl.Float32,
            "start_lng": pl.Float32,
            "end_lat": pl.Float32,
            "end_lng": pl.Float32,
            "member_casual": pl.Utf8,
        }
    )
    return lazy_df

def count_bike_rides_per_day(lf):
    lf = lf.with_columns(pl.col("started_at").dt.date().alias("Date"))
    lf = lf.groupby("Date").agg(pl.col("rideable_type").count().alias("Ride_count"))

    return lf

def avg_max_min_rides_per_week(lf):
    lf = lf.with_columns(pl.col("started_at").dt.date().alias("Date"))
    daily_counts = lf.groupby("Date").agg([
        pl.col("rideable_type").count().alias("Ride_count")
    ])

    daily_counts = daily_counts.with_columns(pl.col("Date").dt.week().alias("Week"))

    week_stats = daily_counts.groupby("Week").agg([
        pl.col("Ride_count").mean().alias("Average_rides"),
        pl.col("Ride_count").max().alias("Maximum_rides"),
        pl.col("Ride_count").min().alias("Minimum_rides")
    ])
    
    return week_stats

def rides_above_or_below_last_week(lf):
    daily_rides = lf.with_columns(pl.col("started_at").dt.date().alias("Date"))
    daily_rides = daily_rides.groupby("Date").agg(pl.col("ride_id").count().alias("Ride_count"))
    daily_rides_last_week = daily_rides.with_columns((pl.col("Date")-pl.duration(weeks = 1)).alias("Last_week_date"))
    
    last_week_ride = daily_rides.select(pl.col("Date").alias("Last_week_date"), pl.col("Ride_count").alias("Last_week_ride_count"))
    diff = daily_rides_last_week.join(last_week_ride, on = "Last_week_date", how = "left")

    diff = diff.with_columns(pl.col("Last_week_ride_count").fill_null(0))
    diff = diff.with_columns((pl.col("Ride_count")-pl.col("Last_week_ride_count")).abs().alias("Ride_count_difference"))

    return diff.select(["Date", "Ride_count", "Last_week_ride_count", "Ride_count_difference"])

def main():
    lazy_df = read_csv_lazily("/app/data/202306-divvy-tripdata.csv")
    rides_per_day = count_bike_rides_per_day(lazy_df).head(5).collect()
    print("Rides per Day:")
    print(rides_per_day)
    
    avg_rides = avg_max_min_rides_per_week(lazy_df).head(5).collect()
    print("Average Rides per Week:")
    print(avg_rides)
    
    rides_last_week = rides_above_or_below_last_week(lazy_df).head(5).collect()
    print("Rides Above or Below Last Week:")
    print(rides_last_week)
    pass


if __name__ == "__main__":
    main()
