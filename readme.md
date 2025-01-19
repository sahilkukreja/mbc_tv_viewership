# TV Viewership ETL Pipeline

This project processes TV viewership event data and enriches it with program details to create a cleaned and transformed dataset suitable for analytics. The pipeline is built using PySpark and designed for scalability and performance.


## Project Structure

- **Python Script**: `tv_viewership_etl_pipeline.py`
  - Contains the ETL logic for processing JSON files, enriching them with program data, and saving the transformed dataset.
- **Input Data**:
  - Event data in JSON format.
  - Program mapping in a tab-delimited `.txt` file.
- **Output**:
  - Processed data stored in Parquet format for efficient querying and analysis.

## Prerequisites

Ensure the following software and tools are installed:

- Python 3.7+
- Java (JDK 8 or higher)
- Apache Spark
- Required Python packages (see `requirements.txt`)

## Setup

### 1. Create a Virtual Environment

```bash
python3 -m venv tv_viewership_env
source tv_viewership_env/bin/activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Spark Environment

Set up environment variables:

#### Linux/Mac:
```bash
export SPARK_HOME=/path/to/spark
export PATH=$SPARK_HOME/bin:$PATH
```

#### Windows:
Add `SPARK_HOME` to System Properties > Environment Variables, and include `SPARK_HOME\bin` in the `PATH`.

## Running the Pipeline

1. Update file paths in the script:
   - Event data: Replace `file_paths` with the paths to your JSON files.
   - Program mapping: Replace `program_file_path` with the path to your `.txt` file.

2. Run the script:

```bash
python tv_viewership_etl_pipeline.py
```

3. Verify the output:
   - The processed data will be saved in Parquet format at the specified `output_path`.

## Output Schema

The final dataset includes the following columns:

- **mac**: Device MAC address.
- **eventdate**: Event date.
- **eventtime**: Event time.
- **chname**: Channel name.
- **program_id**: Program identifier.
- **program_name**: Program name (enriched from mapping).
- **latitude**: Geographic latitude.
- **longitude**: Geographic longitude.
- **code**: Event code.
- **sat**: Satellite name.
- **ts**: Timestamp.
- **indextime**: Index time.

## Troubleshooting

- Ensure the JSON files and the program mapping file exist and have the correct format.
- Check environment variable configurations for Spark.
- Review logs for errors during execution.

## License

This project is licensed under the MIT License. See the LICENSE file for details.



temp code 


        # Show the processed data
        processed_data.show(truncate=False)

        # Identify keys matching the pattern
        matching_keys = [col for col in raw_data.columns if col.startswith(event_key_pattern.rstrip('*'))]

        if not matching_keys:
            raise KeyError(f"No keys matching the pattern '{event_key_pattern}' found in the JSON file(s).")
        
        # Initialize an empty DataFrame for combining results
        combined_df = None

        for key in matching_keys:
            # Explode the array under the key and flatten the structure
            events_df = raw_data.select(explode(raw_data[key]).alias("event")).select("event.*")
            
            # Add latitude and longitude columns if `geo_location` exists
            if "geo_location" in events_df.columns:
                events_df = events_df.withColumn("latitude", split(col("geo_location"), ",")[0].substr(2, 100)) \
                                     .withColumn("longitude", split(col("geo_location"), ",")[1].substr(0, 100).substr(1, 100))
            
            # Drop `_corrupt_record` column if it exists
            if "_corrupt_record" in events_df.columns:
                events_df = events_df.drop("_corrupt_record")

            # Add calculated eventdatetime column
            events_df = events_df.withColumn("eventdatetime", col("eventdate") + " " + col("eventtime"))

            # Combine DataFrames
            if combined_df is None:
                combined_df = events_df
            else:
                combined_df = combined_df.union(events_df)
