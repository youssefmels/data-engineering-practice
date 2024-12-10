from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import zipfile
from pyspark.sql.functions import lit, to_date, regexp_extract
import pandas as pd
from io import BytesIO
import os


def read_csv(spark, zip_path, csvname):
    import zipfile
    from io import StringIO

    with zipfile.ZipFile(zip_path, 'r') as z:
        with z.open(csvname) as f:
            csv_content = f.read().decode("utf-8")

    csv_file_like = StringIO(csv_content)

    df = spark.read.csv(spark.sparkContext.parallelize(csv_file_like.getvalue().splitlines()),
                        header=True, inferSchema=True)

    return df


def add_filename_column(df, column_name, value):
    df = df.withColumn(column_name, lit(value))
    return df

def pull_date(df, new_column_name = "file_date", old_column_name = "source_file"):
    date_pattern = r"(\d{4}-\d{2}-\d{2})"
    df = df.withColumn(new_column_name, to_date(regexp_extract(old_column_name, date_pattern, 1), "yyyy-MM-dd"))
    return df

def brand(df):
    df = df.withColumn("brand", F.when(F.col("model").contains(" "), F.element_at(F.split(F.col("model"), " "), 1)).otherwise("unknown"))
    return df

def storage_ranking(df):
    df2 = df.groupBy("model").agg(F.sum("capacity_bytes").alias("total_capacity"))
    window_spec = Window.orderBy(F.desc("total_capacity"))
    df2 = df2.withColumn("storage_ranking", F.dense_rank().over(window_spec))
    df = df.join(df2.select("model", "storage_ranking"), on = "model", how = "left")
    return df

def primary_key(df, hash_columns):
    df = df.withColumn("primary_key", F.md5(F.concat_ws('_',*[F.col(col).cast("string") for col in hash_columns])))
    return df

def main():
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()
    df = read_csv(spark, "/app/data/hard-drive-2022-01-01-failures.csv.zip", "hard-drive-2022-01-01-failures.csv")


    #1. Add the file name as a column to the DataFrame and call it source_file.
    df = add_filename_column(df, "source_file", "hard-drive-2022-01-01-failures")

    #2. Pull the date located inside the string of the source_file column. Final data-type must be date or timestamp, not a string. Call the new column file_date.
    df = pull_date(df)
    
    #3. Add a new column called brand. It will be based on the column model. If the column model has a space ... aka in it, split on that space.
    # The value found before the space   will be considered the brand. If there is no space to split on, fill in a value called unknown for the brand.
    df = brand(df)

    #4. Inspect a column called capacity_bytes. Create a secondary DataFrame that relates capacity_bytes to the model column, create "buckets" / "rankings"
    #  for those models with the most capacity to the least. Bring back that data as a column called storage_ranking into the main dataset.
    df = storage_ranking(df)

    #5. Create a column called primary_key that is hash of columns that make a record unique in this dataset.
    hash = ["serial_number", "model", "smart_1_raw", "smart_9_raw"]
    df = primary_key(df, hash)

    df.show(5)
    
if __name__ == "__main__":
    main()
