#!/usr/bin/env python
"""
Spark script to parse and aggregate DBS and PhEDEx records on HDFS.
"""

# System modules
import os
import time
import datetime

# Pyspark modules
from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql.functions import struct, array, udf, countDistinct
from pyspark.sql.types import LongType, IntegerType, DoubleType, StringType, StructType, StructField

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, phedex_tables, print_rows
from CMSSpark.spark_utils import spark_context, split_dataset
from CMSSpark.utils import info_save, bytes_to_readable
from CMSSpark.conf import OptionParser

AVERAGES_TIME_DATA_FILE = 'spark_exec_time_averages.txt'

def get_options():
    opts = OptionParser('averages')

    return opts.parser.parse_args()

def get_script_dir():
    return os.path.dirname(os.path.abspath(__file__))

def get_destination_dir():
    return '%s/../../../bash/report_averages' % get_script_dir()

def quiet_logs(sc):
    """
    Sets logger's level to ERROR so INFO logs would not show up.
    """
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

def run(fout, yarn=None, verbose=None):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)

    quiet_logs(ctx)

    sqlContext = HiveContext(ctx)
    
    sqlContext.setConf("spark.sql.files.ignoreCorruptFiles","true")
    sqlContext.sql("set spark.sql.files.ignoreCorruptFiles=true")

    # date, site, dataset, size, replica_date, groupid
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("site", StringType(), True),
        StructField("dataset", StringType(), True),
        StructField("size", DoubleType(), True),
        StructField("replica_date", StringType(), True),
        StructField("groupid", StringType(), True)
    ])

    df = sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load('hdfs:///cms/phedex/*/*/*/part-*', schema=schema)
                        # .load('hdfs:///cms/phedex/2017/03/*/part-00000', schema=schema)

    # Remove all tape sites
    is_tape = lambda site: site.endswith('_MSS') | site.endswith('_Buffer') | site.endswith('_Export')
    df = df.where(is_tape(df.site) == False)

    extract_campaign_udf = udf(lambda dataset: dataset.split('/')[2].split('-')[0])
    extract_tier_udf = udf(lambda dataset: dataset.split('/')[3])
    date_to_timestamp_udf = udf(lambda date: time.mktime(datetime.datetime.strptime(date, "%Y%m%d").timetuple()))
    timestamp_to_date_udf = udf(lambda timestamp: datetime.datetime.fromtimestamp(float(timestamp)).strftime('%Y%m%d'))
    days_delta_udf = udf(lambda t1, t2: (datetime.datetime.fromtimestamp(float(t1)) - datetime.datetime.fromtimestamp(float(t2))).days + 1)
    count_udf = udf(lambda list: len(list))

    df = df.withColumn('campaign', extract_campaign_udf(df.dataset))
    df = df.withColumn('tier', extract_tier_udf(df.dataset))
    df = df.withColumn('date_min', date_to_timestamp_udf(df.date))
    df = df.withColumn('date_max', date_to_timestamp_udf(df.date))
    df = df.withColumn('size_average', df.size)

    df = df.groupBy(['campaign', 'tier'])\
           .agg({'date_min': 'min', 'date_max': 'max', 'date': 'collect_set', 'size_average': 'avg', 'size': 'max'})\
           .withColumnRenamed('min(date_min)', 'date_min')\
           .withColumnRenamed('max(date_max)', 'date_max')\
           .withColumnRenamed('collect_set(date)', 'days_count')\
           .withColumnRenamed('avg(size_average)', 'size_average')\
           .withColumnRenamed('max(size)', 'size_max')\

    df = df.withColumn('period_days', days_delta_udf(df.date_max, df.date_min))\
           .withColumn('days_count', count_udf(df.days_count))\
           .withColumn('date_min', timestamp_to_date_udf(df.date_min))\
           .withColumn('date_max', timestamp_to_date_udf(df.date_max))
        
    df = df.withColumn('existence_in_period', df.days_count / df.period_days)
    df = df.withColumn('average_size_in_period', df.size_average * df.existence_in_period)

    # campaign, tier, date_max, date_min, days_count, size_max, size_average, period_days, existence_in_period, average_size_in_period

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if fout:
        df.write.format("com.databricks.spark.csv")\
                          .option("header", "true").save(fout)
    
    ctx.stop()

@info_save('%s/%s' % (get_destination_dir(), AVERAGES_TIME_DATA_FILE))
def main():
    "Main function"
    opts = get_options()
    print("Input arguments: %s" % opts)
    
    fout = opts.fout
    verbose = opts.verbose
    yarn = opts.yarn
    
    run(fout, yarn, verbose)

if __name__ == '__main__':
    main()
