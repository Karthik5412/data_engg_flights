import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view(name="staging_passengers")
def staging_passengers() :
    df = spark.readStream.format("delta")\
        .load("/Volumes/flights/bronze/bronze_volumes/passengers/data/") 

    df = df.withColumn('modified_date',current_timestamp())\
        .drop("_rescued_data")

    return df 

dlt.create_streaming_table("slv_passengers")

dlt.create_auto_cdc_flow(
    target = "slv_passengers",
    source = "staging_passengers",
    keys= ["passenger_id"],
    sequence_by= "modified_date",
    stored_as_scd_type= 1

)