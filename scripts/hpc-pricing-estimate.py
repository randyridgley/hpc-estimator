import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import types as T
from pyspark.sql.functions import col, udf, explode, expr, to_timestamp, to_date, year, month, dayofmonth, hour, split

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATABASE_NAME', 'TABLE_NAME', 'PRICING_TABLE_NAME', 'S3_OUTPUT_PATH'])    
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

hpc = glueContext.create_dynamic_frame.from_catalog(database=args['DATABASE_NAME'], table_name=args['TABLE_NAME'])
print "Count: ", hpc.count()
hpc.printSchema()

pricing = glueContext.create_dynamic_frame.from_catalog(database=args['DATABASE_NAME'], table_name=args['PRICING_TABLE_NAME'])
print "Count: ", pricing.count()
pricing.printSchema()

estimate = Join.apply(frame1 = hpc, frame2 = pricing, keys1 = ["resources_used_mem_gb", "resource_list_cores"], keys2 = ["memory", "vcpu"])
print "Count: ", estimate.count()
estimate.printSchema()

resolvechoice2 = ResolveChoice.apply(frame = estimate, choice = "make_struct", transformation_ctx = "resolvechoice2")

dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")

datasink5 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields3, connection_type = "s3", connection_options = {"path": args['S3_OUTPUT_PATH']}, format="parquet", transformation_ctx="datasink5")

job.commit()