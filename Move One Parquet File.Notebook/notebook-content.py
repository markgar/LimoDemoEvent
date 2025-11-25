# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cdcf88cd-c1e7-4ec0-929e-c79658867c0c",
# META       "default_lakehouse_name": "LimoDemoEventLH2",
# META       "default_lakehouse_workspace_id": "fb48eb04-b6b1-49c9-9b9c-f57dfffcb695",
# META       "known_lakehouses": [
# META         {
# META           "id": "cdcf88cd-c1e7-4ec0-929e-c79658867c0c"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

subject = "/Files/fhvhv/fhvhv_tripdata_2025-09.parquet "

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

subject = subject.strip()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

FileName = subject.split('/')[-1]
print(FileName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, to_date, year, month
import notebookutils

# Concatenate the path to the filename
full_path = 'Files/fhvhv/' + FileName

# Read the Parquet file
df = spark.read.parquet(full_path)

# Perform casting
df = df.withColumn("request_datetime", col("request_datetime").cast("timestamp")) \
       .withColumn("on_scene_datetime", col("on_scene_datetime").cast("timestamp")) \
       .withColumn("pickup_datetime", col("pickup_datetime").cast("timestamp")) \
       .withColumn("dropoff_datetime", col("dropoff_datetime").cast("timestamp")) \
       .withColumn("airport_fee", col("airport_fee").cast("double")) \
       .withColumn("wav_match_flag", col("wav_match_flag").cast("string")) \
       .withColumn("pickup_date", to_date(col("pickup_datetime"))) \
       .withColumn("pickup_year", year(col("pickup_datetime"))) \
       .withColumn("pickup_month", month(col("pickup_datetime"))) \
       .withColumn("PULocationID", col("PULocationID").cast("long")) \
       .withColumn("DOLocationID", col("DOLocationID").cast("long"))

# Write to Delta table
df.write.format("delta").mode("append").saveAsTable("`ForHireHVTrips`")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
