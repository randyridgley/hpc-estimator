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
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATABASE_NAME', 'TABLE_NAME', 'PRICING_TABLE', 'S3_OUTPUT_PATH'])    
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

hpc = glueContext.create_dynamic_frame.from_catalog(database=args['DATABASE_NAME'], table_name=args['TABLE_NAME'])
print "Count: ", hpc.count()
hpc.printSchema()

pricing = glueContext.create_dynamic_frame.from_catalog(database=args['DATABASE_NAME'], table_name=args['PRICING_NAME'])
print "Count: ", pricing.count()
pricing.printSchema()

datasink5 = glueContext.write_dynamic_frame.from_options(frame = estimate, connection_type = "s3", connection_options = {"path": 's3://'+args['S3_OUTPUT_PATH'], "partitionKeys": ["year", "month", "day"]}, format="parquet", transformation_ctx="datasink5")

job.commit()