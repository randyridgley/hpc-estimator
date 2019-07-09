import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import types as T
from pyspark.sql.functions import col, rank, row_number, udf, explode, expr, to_timestamp, to_date, year, month, dayofmonth, hour, split
from pyspark.sql.window import Window

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATABASE_NAME', 'TABLE_NAME', 'PRICING_TABLE_NAME', 'S3_OUTPUT_PATH'])    
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

gpu_predicate = 'resource_list_gpu > 0'
cpu_predicate = 'resource_list_gpu == 0'

hpc_gpu = glueContext.create_dynamic_frame.from_catalog(database=args['DATABASE_NAME'], table_name=args['TABLE_NAME'], push_down_predicate=gpu_predicate)
print "Count: ", hpc_gpu.count()
hpc_gpu.printSchema()

hpc_cpu = glueContext.create_dynamic_frame.from_catalog(database=args['DATABASE_NAME'], table_name=args['TABLE_NAME'], push_down_predicate=cpu_predicate)
print "Count: ", hpc_cpu.count()
hpc_cpu.printSchema()

pricing = glueContext.create_dynamic_frame.from_catalog(database=args['DATABASE_NAME'], table_name=args['PRICING_TABLE_NAME'])
print "Count: ", pricing.count()
pricing.printSchema()

gpu_pricing_df = pricing.toDF().filter("instancetype like 'p%'")
print "Count: ", gpu_pricing_df.count()
gpu_pricing_df.printSchema()

cpu_df = hpc_cpu.toDF()
pricing_df = pricing.toDF()

cpu_cross_df = cpu_df.crossJoin(pricing_df)
print "Count: ", cpu_cross_df.count()

cpu_window = Window.partitionBy(cpu_cross_df['id']).orderBy(cpu_cross_df['spotprice'].asc())
cpu_cross_df = cpu_cross_df.filter(col('vcpu') >= col('resource_list_cpu')).filter(col('memory') >= col('resources_used_mem_gb'))
cpu_df = cpu_cross_df.select('*', row_number().over(cpu_window).alias('rank')).filter(col('rank') == 1) 

gpu_df = hpc_gpu.toDF()
gpu_cross_df = gpu_df.crossJoin(gpu_pricing_df)
print "Count: ", gpu_cross_df.count()

gpu_window = Window.partitionBy(gpu_cross_df['id']).orderBy(gpu_cross_df['spotprice'].asc())
gpu_cross_df = gpu_cross_df.filter(col('vcpu') >= col('resource_list_cpu')).filter(col('memory') >= col('resources_used_mem_gb'))
gpu_df = gpu_cross_df.select('*', row_number().over(gpu_window).alias('rank')).filter(col('rank') == 1) 

df_estimate = cpu_df.union(gpu_df)
print "Count: ", df_estimate.count()

df_estimate = df_estimate.withColumn("job_cost", expr("cast(round((spotprice * resource_list_nodect) * resources_used_walltime_hrs, 2) as float)"))

df = DynamicFrame.fromDF(df_estimate, glueContext, "joined")

datasink5 = glueContext.write_dynamic_frame.from_options(frame = df, connection_type = "s3", connection_options = {"path": args['S3_OUTPUT_PATH']}, format="parquet", transformation_ctx="datasink5")

job.commit()