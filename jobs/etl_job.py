"""
etl_job.py
~~~~~~~~~~

This Python module contains an example Apache Spark ETL job definition
that implements best practices for production ETL jobs. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py

where packages.zip contains Python modules required by ETL 
which need to be made available to each executor process on every node
in the cluster; etl_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, etl_job.py contains the Spark application
to be executed by a driver process on the Spark master node.

"""

from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import count, avg
from dependencies.spark import start_spark
import logging
import argparse
def main():
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='ihs_etl_job',
        files=['configs/etl_config.json'])

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')
    # execute ETL pipeline
    countyschema=StructType([StructField('Country',StringType(),True),StructField('Country Name',StringType(),True)])
    partschema=StructType([StructField('Part Number',StringType(),True),StructField('Tolerance',StringType(),True),
               StructField('MOQ',StringType(),True),StructField('Box Qty',StringType(),True),StructField('Level',StringType(),True),
               StructField('Dry Pack',StringType(),True),StructField('COUNTRY OF ORIGIN',StringType(),True),StructField('Status',StringType(),True)])
    statusschema=StructType([StructField('status',StringType(),True),StructField('Full Status',StringType(),True)])
    country_df = extract_data(spark,r'hdfs://192.168.29.221:9000/app/country.csv','csv',',',countyschema)
    status_df = extract_data(spark,r'hdfs://192.168.29.221:9000/app/status.csv','csv','~',statusschema)
    part_two_df = extract_data(spark,r'hdfs://192.168.29.221:9000/app/File2.csv','csv',',',partschema)
    part_one_df = extract_data(spark,r'hdfs://192.168.29.221:9000/app/File1.xlsx','excel',',',partschema)
    distinct_country_df=remove_duplcate(country_df,['Country'])
    distinct_status_df=remove_duplcate(status_df,['Status'])
    distinct_part_one_df=remove_duplcate(part_one_df,['Part Number'])
    distinct_part_two_df=remove_duplcate(part_two_df,['Part Number'])
    cleaned_country_df=remove_nulls(distinct_country_df,['Country'])
    cleaned_status_df=remove_nulls(distinct_status_df,['Status'])
    cleaned_part_one_df=remove_nulls(distinct_part_one_df,['Part Number'])
    cleaned_part_two_df=remove_nulls(distinct_part_two_df,['Part Number'])
    merged_df=cleaned_part_one_df.union(cleaned_part_two_df)
    merged_df_withcountry=merged_df.join(cleaned_country_df,merged_df["COUNTRY OF ORIGIN"] == cleaned_country_df["Country"],"inner")
    merged_df_withcountry_withstatus=merged_df_withcountry.join(cleaned_status_df,merged_df_withcountry["Status"] == cleaned_status_df["Status"],"inner")
    columns_to_drop=["Country","status"]
    full_df=merged_df_withcountry_withstatus.drop(*columns_to_drop).withColumnRenamed("Part Number", "Part_Number").withColumnRenamed("Box Qty", "Box_Qty") \
             .withColumnRenamed("Dry Pack", "Dry_Pack").withColumnRenamed("COUNTRY OF ORIGIN", "COUNTRY_OF_ORIGIN").withColumnRenamed("Country Name", "Country_Name") \
             .withColumnRenamed("Full Status", "Full_Status")
    final_df=full_df.groupBy("Country_Name", "Full_Status").agg(count("Part_Number").alias("count"))
    write_data(full_df,'hdfs://192.168.29.221:9000/output/ihsdata_full/fulldf.parquet')
    write_data(final_df,'hdfs://192.168.29.221:9000/output/ihsdata_final/finaldf.parquet')
    #load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('etl_job is finished')
    spark.stop()
    return None


def extract_data(spark,filepath,typedata,separator,schemaname):
    """Load data
    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    if typedata == 'csv':
        df=(spark.read.option("encoding", "ISO-8859-1").csv(filepath,sep=separator,header=True,schema=schemaname))
        
    if typedata == 'excel':
        df=spark.read.format("com.crealytics.spark.excel").schema(schemaname).option("header", "true").option("treatEmptyValuesAsNulls","true") \
            .option("usePlainNumberFormat","true").option("addColorColumns","false").option("maxRowsInMey", 2000).option("sheetName", "Sheet1") \
            .option("location",filepath) \
            .load(filepath)    
    return df


def remove_duplcate(df,duplicate_cols=[]):
    df_transformed = (df.dropDuplicates(duplicate_cols))
    return df_transformed

def remove_nulls(df,nulls_cols=[]):
    df_transformed = (df.na.drop(subset=nulls_cols))
    return df_transformed

def write_data(df,filepath):
    (df.coalesce(1).write.option("header", "false").mode("overwrite").parquet(filepath))
    return None


def load_data(df):
    """Collect data locally and write to CSV.

    :param df: DataFrame to print.
    :return: None
    """
    (df
     .coalesce(1)
     .write
     .csv('loaded_data', mode='overwrite', header=True))
    return None




# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
