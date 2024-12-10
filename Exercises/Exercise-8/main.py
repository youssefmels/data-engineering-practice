import duckdb as dd
import os

def create_table(con, csv_path):
    con.sql("""
            CREATE OR REPLACE TABLE electric_cars
          (VIN VARCHAR,
           County VARCHAR,
           City VARCHAR,
           State VARCHAR,
           Postal_Code INTEGER,
           Model_Year INTEGER,
           Make VARCHAR,
           Model VARCHAR,
           Electric_Vehicle_Type VARCHAR,
           CAFV_Eligibility VARCHAR,
           Electric_Range INTEGER,
           Base_MSRP INTEGER,
           Legislative_District INTEGER,
           DOL_Vehicle_ID INTEGER,
           Vehicle_Location VARCHAR,
           Electric_Utility VARCHAR,
           Census_Tract_2020 BIGINT);
                                        """)
    try:
        con.sql(f"COPY electric_cars FROM '{csv_path}' (AUTO_DETECT TRUE);")
        print("Loading data successful")
    except Exception as e:
        print(f"Error loading data into table: {e}")
    
def count_electric_cars_per_city(con):
    return con.sql("SELECT City, COUNT(*) as Units FROM electric_cars WHERE Electric_Vehicle_Type ILIKE 'Battery Electric Vehicle (BEV)' GROUP BY City;").df()

def top_3_most_pop_electric_cars(con):
    return con.sql("SELECT Model, Make FROM electric_cars WHERE Electric_Vehicle_Type ILIKE 'Battery Electric Vehicle (BEV)' GROUP BY Model, Make ORDER BY COUNT(*) DESC LIMIT 3;").df()

def most_pop_electric_car_in_each_postal_code(con):
    return con.sql("""SELECT Postal_Code, Make, Model FROM 
            (SELECT Postal_Code, Make, Model, COUNT(*) as Units,
            RANK() OVER(PARTITION BY Postal_Code ORDER BY COUNT(*) DESC) AS Rank FROM electric_cars WHERE Electric_Vehicle_Type ILIKE 'Battery Electric Vehicle (BEV)'
             GROUP BY Postal_Code, Make, Model) WHERE Rank = 1;""").df()
    
def count_electric_cars_by_year_parquet(con, output_path):
    result = con.sql("""
            SELECT Model_Year, COUNT(*) as Units FROM electric_cars GROUP BY Model_Year;
            """).df()
    import os
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    result.to_parquet(output_path, partition_cols = ["Model_Year"])
    print(f"Parquet files written to {output_path}")

def main():
    con = dd.connect(':memory:')
    csv_path = "/app/data/Electric_Vehicle_Population_Data.csv"
    output_path = "/app/data/Parquet/Count_Electric_Partitioned_Year_Parquet.parquet"
    create_table(con, csv_path)
    print(count_electric_cars_per_city(con))
    print(top_3_most_pop_electric_cars(con))
    print(most_pop_electric_car_in_each_postal_code(con))
    print(count_electric_cars_by_year_parquet(con, output_path))
    pass


if __name__ == "__main__":
    main()
