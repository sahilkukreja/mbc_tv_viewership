# TV Viewership ETL Pipeline

This project processes TV viewership event data and enriches it with program details to create a cleaned and transformed dataset suitable for analytics. The pipeline is built using PySpark and designed for scalability and performance.


## Project Structure
### Run ETL:
1. Execute `TVViewershipETL.py` to process the raw data using Airflow and run pyspark job in AWS EMR.

### Spark Job:
1. Use `tv_viewership_spark_job.py` for large-scale processing.

### Execute SQL Queries:
1. Run scripts in `SQL/` to analyze processed data and generate insights.

## Output
Processed outputs are available in the `output/` directory, including:

- Aggregated metrics (Average viewing duration, total viewership hours, top 10 channels and reach and TRP analysis).
- User-level and program-level statistics.


## Folder Structure
- **`SQL/`**: Contains SQL scripts for various transformations and analytical queries.
- **`data/`**: Holds input data files in JSON and text formats.
- **`output/`**: Stores processed data outputs, categorized by metric or analysis type.
- **`TVViewershipETL.py`**: Python script for the ETL process.
- **`tv_viewership_spark_job.py`**: Spark job implementation for data processing.
- **`Requirements.txt`**: Lists dependencies and libraries required.
- **`readme.md`**: Project documentation (this file).

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

