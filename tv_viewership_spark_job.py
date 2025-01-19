import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, when, date_trunc, row_number, lead, lag, least, to_date,
    concat, lit, to_timestamp, sum as spark_sum, dense_rank, countDistinct, expr, explode, sequence,unix_timestamp
)
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize SparkSession
def initialize_spark(app_name="TV Viewership ETL Pipeline"):
    logging.info("Initializing SparkSession...")
    return SparkSession.builder.appName(app_name).getOrCreate()

# Read and process nested JSON files
def process_nested_events(spark, file_paths):
    try:
        logging.info("Processing nested JSON files...")
        raw_data = spark.read.option("multiline", "true").json(file_paths)
        keys = raw_data.schema.fieldNames()

        if not keys:
            raise ValueError("No valid keys found in the JSON files.")

        combined_data = None
        for key in keys:
            try:
                nested_data = raw_data.selectExpr(f"explode({key}) as event")
                flattened_data = nested_data.select(
                    "event.mac", "event.eventdate", "event.eventtime", "event.chname",
                    "event.program_id", "event.geo_location", "event.code", "event.sat", "event.ts", "event.indextime"
                )
                combined_data = flattened_data if combined_data is None else combined_data.union(flattened_data)
            except Exception as e:
                logging.warning(f"Error processing key '{key}': {str(e)}")
                continue

        logging.info("Nested JSON files processed successfully.")
        return combined_data
    except Exception as e:
        logging.error(f"Failed to process JSON files: {str(e)}")
        raise

# Read program mapping data
def read_data(spark, program_file_path):
    try:
        logging.info(f"Reading program mapping data from {program_file_path}...")
        program_df = spark.read.option("header", "true").option("delimiter", "\t").csv(program_file_path)
        logging.info("Program mapping data read successfully.")
        return program_df
    except Exception as e:
        logging.error(f"Failed to read program mapping data: {str(e)}")
        raise

# Clean and enrich event data
def clean_and_enrich_data(events_df, program_df):
    try:
        logging.info("Cleaning and enriching event data...")

        events_df = events_df.withColumn(
            "latitude", when(col("geo_location").isNotNull(), split(col("geo_location"), ",").getItem(0)).cast("double")
        ).withColumn(
            "longitude", when(col("geo_location").isNotNull(), split(col("geo_location"), ",").getItem(1)).cast("double")
        ).drop("geo_location")

        enriched_df = events_df.join(program_df, on="program_id", how="left")

        enriched_df = enriched_df.withColumn(
            "eventdatetime", to_timestamp(concat(col("eventdate"), lit(" "), col("eventtime")), "yyyy-MM-dd HH:mm:ss")
        ).withColumn("minute_bucket", date_trunc("minute", col("eventdatetime")))

        window_spec = Window.partitionBy("mac", "minute_bucket").orderBy(col("eventdatetime").desc())
        ranked_df = enriched_df.withColumn("row_num", row_number().over(window_spec))
        latest_minute_viewership_df = ranked_df.filter(col("row_num") == 1).drop("row_num")

        time_window = Window.partitionBy("mac").orderBy("minute_bucket")
        latest_minute_viewership_df = latest_minute_viewership_df.withColumn(
            "next_minute_bucket", lead("minute_bucket").over(time_window)
        ).withColumn(
            "minute_sequence", sequence(
                col("minute_bucket"),
                when(col("next_minute_bucket").isNotNull(), col("next_minute_bucket") - expr("INTERVAL 1 MINUTE"))
                .otherwise(col("minute_bucket")),
                expr("INTERVAL 1 MINUTE")
            )
        ).withColumn("minute", explode(col("minute_sequence")))

        final_df = latest_minute_viewership_df.select(
            col("mac").alias("MAC_ID"), col("minute").alias("datetime"),
            col("chname").alias("channel"), "program_id", "latitude", "longitude",
            "code", "sat", "ts", "indextime", "program_name", "genre"
        )

        logging.info("Data cleaning and enrichment completed.")
        return final_df
    except Exception as e:
        logging.error(f"Failed to clean and enrich data: {str(e)}")
        raise

# Calculate viewership hours
def calculate_viewership_hours(final_df):
    try:
        logging.info("Calculating viewership hours...")
        window_spec = Window.partitionBy("MAC_ID").orderBy("datetime")

        final_df = final_df.withColumn("prev_datetime", lag("datetime").over(window_spec))
        final_df = final_df.withColumn("next_datetime", lead("datetime").over(window_spec))

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

        final_df = final_df.withColumn("date", to_date("datetime")).filter(col("channel").isNotNull())

        viewership_hours_df = final_df.groupBy("channel", "date").agg(
            spark_sum(col("duration_minutes") / 60).alias("total_viewership_hours")
        ).orderBy("date", col("total_viewership_hours").desc())

        logging.info("Viewership hours calculated successfully.")
        return viewership_hours_df
    except Exception as e:
        logging.error(f"Failed to calculate viewership hours: {str(e)}")
        raise

# Calculate top channels
def calculate_top_channels(viewership_hours_df):
    try:
        logging.info("Calculating top channels...")
        ranking_window = Window.partitionBy("date").orderBy(col("total_viewership_hours").desc())
        top_channels_df = viewership_hours_df.withColumn("rank", dense_rank().over(ranking_window)) \
                                             .filter(col("rank") <= 10)
        logging.info("Top channels calculated successfully.")
        return top_channels_df
    except Exception as e:
        logging.error(f"Failed to calculate top channels: {str(e)}")
        raise

# Calculate average viewing duration
def calculate_average_viewing_duration(final_df):
    try:
        logging.info("Calculating average viewing duration...")
        final_df = final_df.withColumn("date", to_date("datetime"))
        window_spec = Window.partitionBy("MAC_ID").orderBy("datetime")

        final_df = final_df.withColumn("prev_datetime", lag("datetime").over(window_spec))
        final_df = final_df.withColumn("next_datetime", lead("datetime").over(window_spec))

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

        user_viewing_duration_df = final_df.groupBy("MAC_ID", "date").agg(
            spark_sum(col("duration_minutes")).alias("total_viewing_minutes")
        )

        avg_viewing_duration_df = user_viewing_duration_df.groupBy("date").agg(
            (spark_sum(col("total_viewing_minutes")) / countDistinct("MAC_ID")).alias("average_viewing_duration_minutes")
        )

        logging.info("Average viewing duration calculated successfully.")
        return avg_viewing_duration_df
    except Exception as e:
        logging.error(f"Failed to calculate average viewing duration: {str(e)}")
        raise

# Calculate Reach and TRP
# MAC_ID	datetime	channel	program_id	latitude	longitude	code	sat	ts	indextime	program_name	genre

def calculate_reach_and_trp(minute_level_viewership):
    try:
        logging.info("Calculating Reach and TRP...")
        daily_viewership = minute_level_viewership.withColumn(
            "view_date", to_date("datetime")
        ).groupBy("view_date").agg(
            countDistinct("MAC_ID").alias("total_viewers")
        )

        channel_viewership = minute_level_viewership.withColumn(
            "view_date", to_date("datetime")
        ).groupBy("channel", "view_date").agg(
            countDistinct("MAC_ID").alias("unique_viewers_per_channel")
        )

        minute_ratings = minute_level_viewership.withColumn(
            "view_date", to_date("datetime")
        ).groupBy("channel", "view_date", "datetime").agg(
            countDistinct("MAC_ID").alias("unique_viewers_per_minute")
        )

        trp_calculation = minute_ratings.groupBy("channel", "view_date").agg(
            spark_sum("unique_viewers_per_minute").alias("trp")
        )

        reach_and_trp = channel_viewership.join(
            daily_viewership, on="view_date", how="inner"
        ).join(
            trp_calculation, on=["channel", "view_date"], how="inner"
        ).withColumn(
            "reach", col("unique_viewers_per_channel") / col("total_viewers")
        ).select(
            col("channel").alias("channel"),
            "view_date",
            col("reach").alias("reach"),
            "trp"
        )

        logging.info("Reach and TRP calculated successfully.")
        return reach_and_trp.orderBy("view_date", "channel")
    except Exception as e:
        logging.error(f"Failed to calculate Reach and TRP: {str(e)}")
        raise

# Save DataFrame to disk
def save_data(df, base_dir, file_name):
    try:
        file_path = os.path.join(base_dir, file_name)
        df.write.mode("overwrite").option("delimiter", "\t").option("header", "true").csv(file_path)
        logging.info(f"Data successfully written to {file_path}")
    except Exception as e:
        logging.error(f"Failed to save data: {str(e)}")
        raise

# Main function
def main():
    spark = initialize_spark()
    try:
        # Directories
        data_dir = "data"
        output_dir = "output"

        # File paths
        file_paths = [os.path.join(data_dir, f"events_2024010{i}.json") for i in range(1, 8)]
        program_file_path = os.path.join(data_dir, "program_data.txt")

        # Read and process data
        events_df = process_nested_events(spark, file_paths)
        program_df = read_data(spark, program_file_path)
        save_data(events_df, output_dir, "raw_data")

        # Clean and enrich data
        minute_level_viewership = clean_and_enrich_data(events_df, program_df)
        save_data(minute_level_viewership, output_dir, "minute_level_viewership")

        # Calculate viewership hours
        viewership_hours_df = calculate_viewership_hours(minute_level_viewership)
        save_data(viewership_hours_df, output_dir, "total_viewership_hours")

        # Calculate average viewing duration
        avg_viewing_duration_df = calculate_average_viewing_duration(minute_level_viewership)
        save_data(avg_viewing_duration_df, output_dir, "average_viewing_duration")

        # Calculate top channels
        top_channels_df = calculate_top_channels(viewership_hours_df)
        save_data(top_channels_df, output_dir, "top_channels")

        # Calculate Reach and TRP
        reach_and_trp_df = calculate_reach_and_trp(minute_level_viewership)
        save_data(reach_and_trp_df, output_dir, "reach_and_trp")
    finally:
        spark.stop()
        logging.info("Spark session stopped.")

if __name__ == "__main__":
    main()
