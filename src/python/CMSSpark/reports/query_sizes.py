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

SIZES_QUERY_TIME_DATA_FILE = 'spark_exec_time_sizes_query.txt'

def get_options():
    opts = OptionParser('averages')

    opts.parser.add_argument("--campaign", action="store",
            dest="campaign", default=None, help="Pass --campaign and --tier to get how much data for a given campaign is there in a given data tier")

    opts.parser.add_argument("--tier", action="store",
            dest="tier", default=None, help="Pass --campaign and --tier to get how much data for a given campaign is there in a given data tier")

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

def get_date_to_timestamp_udf():
    return udf(lambda date: time.mktime(datetime.datetime.strptime(date, "%Y%m%d").timetuple()))

def get_timestamp_to_date_udf():
    return udf(lambda timestamp: datetime.datetime.fromtimestamp(float(timestamp)).strftime('%Y%m%d'))

def run(yarn=None, verbose=None, campaign=None, tier=None):
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

    df = sqlContext.read.format('com.databricks.spark.csv')\
                        .options(header='true', treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load('hdfs:///cms/aggregation/sizes/part-*')
    
    if campaign != None and tier != None:
        campaign_tier_df = df.where(df.campaign == campaign)\
                             .where(df.tier == tier)

        campaign_tier = map(lambda row: row.asDict(), campaign_tier_df.collect())

        print 'Average size: %s' % bytes_to_readable(float(campaign_tier[0]['size_average']))
        print 'Average in period of existence: %s' % bytes_to_readable(float(campaign_tier[0]['average_size_in_period']))
        print 'Max size: %s' % bytes_to_readable(float(campaign_tier[0]['size_max']))
        print 'T1 size: %s' % bytes_to_readable(float(campaign_tier[0]['t1_size']))
        print 'T2 size: %s' % bytes_to_readable(float(campaign_tier[0]['t2_size']))
        print 'T3 size: %s' % bytes_to_readable(float(campaign_tier[0]['t3_size']))

    date_to_timestamp_udf = udf(lambda date: time.mktime(datetime.datetime.strptime(date, "%Y%m%d").timetuple()))
    
    months = [1, 2, 3, 4, 5, 6, 9, 12]
    
    for month in months:
        now = (datetime.datetime.now() - datetime.datetime(1970,1,1)).total_seconds()
        seconds = month * 30 * 24 * 60 * 60
        not_accessed_df = df.withColumn('date_timestamp', date_to_timestamp_udf(df.last_access_date))
        not_accessed_df = not_accessed_df.where(now - not_accessed_df.date_timestamp > seconds)
        not_accessed_df = not_accessed_df.withColumn("size_average", not_accessed_df["size_average"].cast(DoubleType()))

        total_size = not_accessed_df.groupBy().sum('size_average').rdd.map(lambda x: x[0]).collect()[0] or 0

        print 'Size of data not accessed for last %d month(s): %s' % (month, bytes_to_readable(total_size))

    ctx.stop()

@info_save('%s/%s' % (get_destination_dir(), SIZES_QUERY_TIME_DATA_FILE))
def main():
    "Main function"
    opts = get_options()
    print("Input arguments: %s" % opts)
    
    verbose = opts.verbose
    yarn = opts.yarn
    campaign = opts.campaign
    tier = opts.tier
    
    run(yarn, verbose, campaign, tier)

if __name__ == '__main__':
    main()
