Data Pipeline Project: NYC Green Taxi Revenue Report

1. hat Are the Steps I Took to Complete the Project?

1) Data Ingestion and GCS Staging (Task 1)
- Scripting: I wrote the data_ingestion_green.py script.
- Execution: I ran the script to download the three months of Green Taxi data and uploaded them as Parquet files to the green_taxi/ folder inside the green-taxi-2025 GCS bucket.

2) Data Processing and Calculation (Task 2 - T)
- Environment Setup: I set up a Google Cloud Dataproc cluster (green-taxi-cluster).
-Script Execution: I ran the PySpark script, revenue_report_green.py, on the cluster.
-Calculation: The script combined all data to calculate the daily total revenue by pickup zone (PULocationID). The results were saved back to GCS in the revenue_reports/daily_revenue folder.

3) Final Report Loading (Task 2 - L)
- BigQuery Setup: I created a BigQuery dataset (nyc_green_taxi_analysis).
- Data Load: I loaded the calculated revenue report file (Parquet) from GCS into the BigQuery table final_daily_revenue_2 for final analysis.


2. What Challenges Did I Face?
The biggest problems involved cloud permissions, argument handling, and ensuring data completeness.

1) Dataproc IAM Permission Failure (Major Hurdle):
- The Dataproc cluster repeatedly failed due to missing GCS read/write permissions.
- Resolution: I explicitly granted the 'Owner' role to the cluster's Compute Engine Service Account.

2) PySpark Argument Passing Error:
- The job failed because the Dataproc submission did not properly handle the argparse requirements.
- Resolution: I fixed this by explicitly adding the --input_green and --output flags in the job submission arguments.

3) BigQuery Data Incompleteness:
- The PySpark code used the coalesce(1) function, forcing the output into only one file.
- When loading into BigQuery, the console UI failed to recognize the folder-level wildcard path (*).
- Result: I was forced to load a single file path, which resulted in incomplete data (only a subset of the three months was loaded).


3. What Would I Do Differently with More Time?
- Remove coalesce(1) for Better Performance: I would remove the coalesce(1) function from revenue_report_green.py. This allows Spark to save results across multiple files, maximizing distributed processing power and avoiding single-file bottlenecks. 
