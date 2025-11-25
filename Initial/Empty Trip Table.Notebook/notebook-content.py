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

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS forhirehvtrips (
  hvfhs_license_num        STRING,
  dispatching_base_num     STRING,
  originating_base_num     STRING,
  request_datetime         TIMESTAMP,
  on_scene_datetime        TIMESTAMP,
  pickup_datetime          TIMESTAMP,
  dropoff_datetime         TIMESTAMP,
  PULocationID             BIGINT,
  DOLocationID             BIGINT,
  trip_miles               DOUBLE,
  trip_time                BIGINT,
  base_passenger_fare      DOUBLE,
  tolls                    DOUBLE,
  bcf                      DOUBLE,
  sales_tax                DOUBLE,
  congestion_surcharge     DOUBLE,
  airport_fee              DOUBLE,
  tips                     DOUBLE,
  driver_pay               DOUBLE,
  shared_request_flag      STRING,
  shared_match_flag        STRING,
  access_a_ride_flag       STRING,
  wav_request_flag         STRING,
  wav_match_flag           STRING,
  cbd_congestion_fee       DOUBLE,
  pickup_date              DATE,
  pickup_year              INT,
  pickup_month             INT
)
USING delta
""")

spark.sql(f"TRUNCATE TABLE forhirehvtrips")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
