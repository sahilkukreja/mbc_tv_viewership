from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, when, date_trunc, row_number, count,lag,least,lead,when,to_date,
    concat, lit, to_timestamp, sum as spark_sum
)
from pyspark.sql.functions import unix_timestamp

from pyspark.sql.window import Window
import os

def initialize_spark(app_name="TV Viewership ETL Pipeline"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_data(spark, file_paths, program_file_path):
    # Read event JSON files into a single DataFrame
    events_df = spark.read.json(file_paths)

    # Read program mapping data (tab-delimited text file with headers: program_id, program_name)
    program_df = spark.read.option("header", "true").option("delimiter", "\t").csv(program_file_path)

    return events_df, program_df

def clean_and_enrich_data(events_df, program_df):
    # Split geo_location into latitude and longitude
    events_df = events_df.withColumn("latitude", 
                                     when(col("geo_location").isNotNull(), split(col("geo_location"), ",").getItem(0)).cast("double")) \
                         .withColumn("longitude", 
                                     when(col("geo_location").isNotNull(), split(col("geo_location"), ",").getItem(1)).cast("double")) \
                         .drop("geo_location")

    # Join with program mapping data
    enriched_df = events_df.join(program_df, on="program_id", how="left")

    # Add eventdatetime column
    enriched_df = enriched_df.withColumn(
        "eventdatetime", 
        to_timestamp(concat(col("eventdate"), lit(" "), col("eventtime")), "yyyy-MM-dd HH:mm:ss")
    )

    return enriched_df

def transform_data(enriched_df):
    # Create minute_bucket
    enriched_df = enriched_df.withColumn("minute_bucket", date_trunc("minute", col("eventdatetime")))

    # Define window specification to get the latest record for each minute per MAC ID
    window_spec = Window.partitionBy("mac", "minute_bucket").orderBy(col("eventtime").desc())

    # Add row number column and filter to retain the latest record
    ranked_df = enriched_df.withColumn("row_num", row_number().over(window_spec))
    latest_minute_viewership_df = ranked_df.filter(col("row_num") == 1).drop("row_num")

    # Select final columns
    final_df = latest_minute_viewership_df.select(
        col("mac").alias("MAC_ID"),
        col("minute_bucket").alias("datetime"),
        col("chname").alias("channel"),
        col("program_name"),
        col("code")
    )

    return final_df

def calculate_viewership_hours(final_df):
    """
    Calculate total viewership hours by channel and date in PySpark, explicitly handling ELWS transitions.
    """
    # Define a window to calculate the previous and next timestamps
    window_spec = Window.partitionBy("MAC_ID").orderBy("datetime")

    # Add previous and next datetime columns
    final_df = final_df.withColumn("prev_datetime", lag("datetime").over(window_spec))
    final_df = final_df.withColumn("next_datetime", lead("datetime").over(window_spec))

    # Calculate viewership duration in minutes
    final_df = final_df.withColumn(
        "duration_minutes",
        when(
            col("code") == "SSTAND",
            when(col("prev_datetime").isNotNull(),
                 (unix_timestamp("datetime") - unix_timestamp("prev_datetime")) / 60
                ).otherwise(0)
        ).otherwise(
            when(col("next_datetime").isNotNull(),
                 least((unix_timestamp("next_datetime") - unix_timestamp("datetime")) / 60, lit(10))
                ).otherwise(0)
        )
    )


    final_df = final_df.withColumn("date", to_date("datetime")).filter(col("channel").isNotNull() & (col("channel") != ""))

    # Aggregate viewership hours by channel and date
    viewership_hours_df = final_df.groupBy("channel", "date").agg(
        spark_sum(col("duration_minutes") / 60).alias("total_viewership_hours")
    ).orderBy("date", col("total_viewership_hours").desc())


    return viewership_hours_df

def calculate_top_channels(viewership_hours_df):
    # Define a window specification to rank channels by total viewership hours per date
    ranking_window = Window.partitionBy("date").orderBy(col("total_viewership_hours").desc())

    # Add a rank column and filter the top 10 channels for each day
    top_channels_df = viewership_hours_df.withColumn("rank", dense_rank().over(ranking_window)) \
                                         .filter(col("rank") <= 10)

    return top_channels_df

def save_data(viewership_hours_df, final_df, data_dir):
    # Define output paths
    viewership_output_path = os.path.join(data_dir, "viewership_hours")
    user_viewship_output_path = os.path.join(data_dir, "user_viewership_data")

    # Save viewership hours data
    viewership_hours_df.write.mode("overwrite").option("delimiter", "\t").option("header", "true").csv(viewership_output_path)

    # Save combined raw and processed data
    final_df.write.mode("overwrite").option("delimiter", "\t").option("header", "true").csv(user_viewship_output_path)

    print(f"Viewership hours data written to {viewership_output_path}")

    print(f"Combined raw and processed data written to {user_viewship_output_path}")

    

    # Calculate total viewing duration per user
    user_viewing_duration_df = final_df.groupBy("MAC_ID").agg(count("datetime").alias("total_viewing_minutes"))
    
    # Calculate the average viewing duration across all users
    avg_viewing_duration_df = user_viewing_duration_df.agg(
        (spark_sum(col("total_viewing_minutes")) / count("*")).alias("average_viewing_duration_minutes")
    )
    
    return user_viewing_duration_df, avg_viewing_duration_df


def main():
    # Initialize Spark session
    spark = initialize_spark()

    try:
        # Directory containing the event files
        data_dir = "data"

        # Generate file paths dynamically
        file_paths = [
            os.path.join(data_dir, file_name) for file_name in [
                "events_20240101.json", "events_20240102.json", "events_20240103.json",
                "events_20240104.json", "events_20240105.json", "events_20240106.json",
                "events_20240107.json"
            ]
        ]

        # File path for program mapping data
        program_file_path = os.path.join(data_dir, "program_data.txt")

        # Read data
        events_df, program_df = read_data(spark, file_paths, program_file_path)

        # Clean and enrich data
        enriched_df = clean_and_enrich_data(events_df, program_df)

        # Transform data
        final_df = transform_data(enriched_df)

        # Calculate viewership hours
        viewership_hours_df = calculate_viewership_hours(final_df)

        # Save data
        save_data(viewership_hours_df, final_df, data_dir)

    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()