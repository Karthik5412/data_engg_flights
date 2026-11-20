import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import * 

@dlt.view(name="src_airports")
def src_airports():
    df = spark.readStream.format('delta')\
            .load("/Volumes/flights/bronze/bronze_volumes/airports/data/")
    return df 

@dlt.table(name="trans_airports")
def trans_airports() :
    df = spark.readStream.table("src_airports")
    df = df.withColumn("modified_date",current_timestamp())\
            .drop('_rescued_data')

    return df


dlt.create_streaming_table(name='slv_airports')


dlt.create_auto_cdc_flow(
    target='slv_airports',
    source='trans_airports',
    keys=['airport_id'],
    sequence_by='modified_date',
    stored_as_scd_type=1
)