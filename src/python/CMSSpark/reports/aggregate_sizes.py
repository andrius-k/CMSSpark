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

SIZES_TIME_DATA_FILE = 'spark_exec_time_sizes.txt'

PHEDEX_HDFS_URL = 'hdfs:///cms/phedex/*/*/*/part-*'
# PHEDEX_HDFS_URL = 'hdfs:///cms/phedex/2017/07/*/part-00000'

DBS_CONDOR_HDFS_URL = 'hdfs:///cms/dbs_condor/dataset/*/*/*/part-*'
# DBS_CONDOR_HDFS_URL = 'hdfs:///cms/dbs_condor/dataset/2017/07/*/part-00000'

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

def get_date_to_timestamp_udf():
    return udf(lambda date: time.mktime(datetime.datetime.strptime(date, "%Y%m%d").timetuple()))

def get_timestamp_to_date_udf():
    return udf(lambda timestamp: datetime.datetime.fromtimestamp(float(timestamp)).strftime('%Y%m%d'))

def get_extract_campaign_udf():
    return udf(lambda dataset: dataset.split('/')[2].split('-')[0])

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
                        .load(PHEDEX_HDFS_URL, schema=schema)
                        
    # Remove all tape sites
    is_tape = lambda site: site.endswith('_MSS') | site.endswith('_Buffer') | site.endswith('_Export')
    df = df.where(is_tape(df.site) == False)

    # Remove all non VALID datasets
    remove_invalid_datasets(df, sqlContext, verbose)

    # Get accesses data frame
    accesses_df = get_dataset_access_dates(sqlContext)

    # extract_campaign_udf = udf(lambda dataset: dataset.split('/')[2].split('-')[0])
    extract_tier_udf = udf(lambda dataset: dataset.split('/')[3])
    days_delta_udf = udf(lambda t1, t2: (datetime.datetime.fromtimestamp(float(t1)) - datetime.datetime.fromtimestamp(float(t2))).days + 1)
    count_udf = udf(lambda list: len(list))
    get_t1_size = udf(lambda size, site: size if site.startswith('T1') else 0)
    get_t2_size = udf(lambda size, site: size if site.startswith('T2') else 0)
    get_t3_size = udf(lambda size, site: size if site.startswith('T3') else 0)

    df = df.withColumn('campaign', get_extract_campaign_udf()(df.dataset))\
           .withColumn('tier', extract_tier_udf(df.dataset))\
           .withColumn('date_min', get_date_to_timestamp_udf()(df.date))\
           .withColumn('date_max', get_date_to_timestamp_udf()(df.date))\
           .withColumn('size_average', df.size)\
           .withColumn('t1_size', get_t1_size(df.size, df.site))\
           .withColumn('t2_size', get_t2_size(df.size, df.site))\
           .withColumn('t3_size', get_t3_size(df.size, df.site))

    df = df.groupBy(['campaign', 'tier'])\
           .agg({'date_min': 'min', 'date_max': 'max', 'date': 'collect_set', 'size_average': 'avg', 'size': 'max', 't1_size': 'avg', 't2_size': 'avg', 't3_size': 'avg'})\
           .withColumnRenamed('min(date_min)', 'date_min')\
           .withColumnRenamed('max(date_max)', 'date_max')\
           .withColumnRenamed('collect_set(date)', 'days_count')\
           .withColumnRenamed('avg(size_average)', 'size_average')\
           .withColumnRenamed('max(size)', 'size_max')\
           .withColumnRenamed('avg(t1_size)', 't1_size')\
           .withColumnRenamed('avg(t2_size)', 't2_size')\
           .withColumnRenamed('avg(t3_size)', 't3_size')\

    df = df.withColumn('period_days', days_delta_udf(df.date_max, df.date_min))\
           .withColumn('days_count', count_udf(df.days_count))\
           .withColumn('date_min', get_timestamp_to_date_udf()(df.date_min))\
           .withColumn('date_max', get_timestamp_to_date_udf()(df.date_max))
        
    df = df.withColumn('existence_in_period', df.days_count / df.period_days)
    df = df.withColumn('average_size_in_period', df.size_average * df.existence_in_period)

    df.show(100, truncate=False)

    # campaign, tier, date_max, date_min, days_count, size_max, size_average, period_days, existence_in_period, average_size_in_period, t1_size, t2_size, t3_size, last_access_date
    df = df.join(accesses_df, 'campaign')
    
    df.show(100, truncate=False)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if fout:
        df.write.format("com.databricks.spark.csv")\
                          .option("header", "true").save(fout)
    
    ctx.stop()

def remove_invalid_datasets(df, sqlContext, verbose):
    tables = {}

    instances = ['GLOBAL', 'PHYS01', 'PHYS02', 'PHYS03']
    for instance in instances:
        dbs_dict = dbs_tables(sqlContext, inst=instance, verbose=verbose)
        for key, val in dbs_dict.items():
            new_key = '%s_%s' % (key, instance)
            tables[new_key] = val
    
    daf = reduce(lambda a,b: a.unionAll(b), [tables['daf_%s' % x] for x in instances])
    ddf = reduce(lambda a,b: a.unionAll(b), [tables['ddf_%s' % x] for x in instances])

    dbs_ddf_cols = ['d_dataset', 'd_dataset_access_type_id']
    dbs_daf_cols = ['dataset_access_type_id', 'dataset_access_type']

    ddf_df = ddf.select(dbs_ddf_cols)
    daf_df = daf.select(dbs_daf_cols)

    # d_dataset, dataset_access_type
    dbs_df = ddf_df.join(daf_df, ddf_df.d_dataset_access_type_id == daf_df.dataset_access_type_id)\
                   .drop(ddf_df.d_dataset_access_type_id)\
                   .drop(daf_df.dataset_access_type_id)

    # ..., dataset_access_type
    df = df.join(dbs_df, df.dataset == dbs_df.d_dataset)

    df = df.where(df.dataset_access_type == 'VALID')\
           .drop(df.dataset_access_type)

    return df

def get_dataset_access_dates(sqlContext):
    # dataset, date, ExitCode
    schema = StructType([
        StructField("dataset", StringType(), True),
        StructField("user", StringType(), True),
        StructField("ExitCode", StringType(), True),
        StructField("Type", StringType(), True),
        StructField("TaskType", StringType(), True),
        StructField("rec_time", StringType(), True),
        StructField("sum_evts", StringType(), True),
        StructField("sum_chr", StringType(), True),
        StructField("date", StringType(), True),
        StructField("rate", StringType(), True),
        StructField("tier", StringType(), True),
    ])
    
    df = sqlContext.read.format('com.databricks.spark.csv')\
                        .options(header='true', treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(DBS_CONDOR_HDFS_URL, schema=schema)

    # campaign, date_max
    df = df.where(df.ExitCode == '0')\
           .where(df.dataset != 'null')\
           .withColumn('campaign', get_extract_campaign_udf()(df.dataset))\
           .withColumn('date_max', get_date_to_timestamp_udf()(df.date))\
           .drop(df.ExitCode)\
           .drop(df.dataset)

    # campaign, last_access_date
    df = df.groupBy('campaign')\
           .agg({'date_max':'max'})\
           .withColumnRenamed('max(date_max)', 'date_max')

    df = df.withColumn('last_access_date', get_timestamp_to_date_udf()(df.date_max))\
           .drop(df.date_max)
    
    return df

@info_save('%s/%s' % (get_destination_dir(), SIZES_TIME_DATA_FILE))
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
