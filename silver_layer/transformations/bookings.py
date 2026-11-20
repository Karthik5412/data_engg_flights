import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import * 

@dlt.view(name="staging_bookings")
def staging_bookings() :
    df = spark.readStream.format("delta")\
        .load("/Volumes/flights/bronze/bronze_volumes/bookings/data/")

    return df.withColumn('modified_date',current_timestamp())\
        .drop("_rescued_data")

dlt.create_streaming_table(name="slv_bookings")


dlt.create_auto_cdc_flow(
    target="slv_bookings",
    source="staging_bookings",
    keys= ["booking_id"],
    sequence_by= "modified_date",
    stored_as_scd_type= 1
)


