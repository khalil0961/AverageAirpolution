
import pandas as pd
from pyspark.sql.functions import avg
from pyspark.sql.types import FloatType
import time

air_quality_url = "https://datastorageuxklz.blob.core.windows.net/iot-sensordata/Melbourne_Air_Quality_Data.csv"
energy_url = "https://datastorageuxklz.blob.core.windows.net/iot-sensordata/Melbourne_Energy_Consumption_Data.csv"
traffic_url = "https://datastorageuxklz.blob.core.windows.net/iot-sensordata/Melbourne_Traffic_Data.csv"


air_quality_df = spark.createDataFrame(pd.read_csv(air_quality_url))
energy_df = spark.createDataFrame(pd.read_csv(energy_url))
traffic_df = spark.createDataFrame(pd.read_csv(traffic_url))

print("Air Quality Data Columns:", air_quality_df.columns)
print("Energy Data Columns:", energy_df.columns)
print("Traffic Data Columns:", traffic_df.columns)


if 'air_quality_index' in air_quality_df.columns and 'location' in air_quality_df.columns:
    air_quality_df = air_quality_df.withColumn("air_quality_index", air_quality_df["air_quality_index"].cast(FloatType()))
    air_quality_df = air_quality_df.filter(air_quality_df["air_quality_index"].isNotNull())
    avg_air_quality_df = air_quality_df.groupBy("location").agg(avg("air_quality_index").alias("AvgAirQualityIndex"))
else:
    raise ValueError("Required columns not found in air_quality_df")


if 'energy_consumed_kwh' in energy_df.columns and 'location' in energy_df.columns:
    energy_df = energy_df.withColumn("energy_consumed_kwh", energy_df["energy_consumed_kwh"].cast(FloatType()))
    energy_df = energy_df.filter(energy_df["energy_consumed_kwh"].isNotNull())
    avg_energy_consumption_df = energy_df.groupBy("location").agg(avg("energy_consumed_kwh").alias("AvgEnergyConsumption"))
else:
    raise ValueError("Required columns not found in energy_df")


if 'vehicle_count' in traffic_df.columns and 'location' in traffic_df.columns:
    traffic_df = traffic_df.withColumn("vehicle_count", traffic_df["vehicle_count"].cast(FloatType()))
    traffic_df = traffic_df.filter(traffic_df["vehicle_count"].isNotNull())
    avg_traffic_density_df = traffic_df.groupBy("location").agg(avg("vehicle_count").alias("AvgTrafficDensity"))
else:
    raise ValueError("Required columns not found in traffic_df")

metrics_df = avg_air_quality_df.join(avg_traffic_density_df, "location", "outer") \
                               .join(avg_energy_consumption_df, "location", "outer")

metrics_df.show()


retries = 3
for attempt in range(retries):
    try:
        metrics_df.write.format("delta").mode("overwrite").save("/mnt/datalake/metrics_summary")
        print("Data successfully written to Delta table.")
        break
    except Exception as e:
        print(f"Attempt {attempt + 1} failed with error: {e}")
        if attempt < retries - 1:
            time.sleep(5)  # Wait for 5 seconds before retrying
        else:
            raise


metrics_df.write.csv("/mnt/datalake/metrics_summary_csv", header=True, mode="overwrite")


display(metrics_df)


blob_sas_url = "https://datastorageuxklz.blob.core.windows.net/iot-sensordata.csv?<sas_token>"




