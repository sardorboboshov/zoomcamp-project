usage_stats_sql = """
CREATE EXTERNAL TABLE  IF NOT EXISTS `{project_id}.{dataset_name}.usage-stats` (
  rental_id STRING,
  duration INT64,
  bike_id STRING,
  end_date DATETIME,
  endstation_id STRING,
  endstation_name STRING,
  start_date DATETIME,
  startstation_id STRING,
  startstation_name STRING
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://{bucket_name}/files/usage-stats/*.parquet']
);


"""

active_travel_counts_programme_sql = """
CREATE EXTERNAL TABLE  IF NOT EXISTS `{project_id}.{dataset_name}.ActiveTravelCountsProgramme1` (
  wave STRING,
  site_id STRING,
  date date,
  weather STRING,
  time time,
  day STRING,
  round STRING,
  direction STRING,
  path STRING,
  mode STRING,
  count int64
)
OPTIONS (
  format = 'parquet',
  uris = ['gs://{bucket_name}/files/ActiveTravelCountsProgramme/ActiveTravelCountsProgramme.parquet']
)
"""