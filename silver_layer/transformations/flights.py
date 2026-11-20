import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import * 

@dlt.view(name="staging_flights")
def src_airports():
    df = spark.readStream.format('delta')\
            .load("/Volumes/flights/bronze/bronze_volumes/flights/data/")

    df = df.withColumn("modified_date",current_timestamp())\
        .drop("_rescued_data")
    
    return df 



dlt.create_streaming_table(name='slv_flights')


dlt.create_auto_cdc_flow(
    target='slv_flights',
    source='staging_flights',
    keys=['flight_id'],
    sequence_by='modified_date',
    stored_as_scd_type=1
)