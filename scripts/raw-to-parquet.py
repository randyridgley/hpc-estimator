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
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATABASE_NAME', 'TABLE_NAME'])

@udf("map<string,string>")
def map_keys(s):
    keys = {}
    if s:    
        try:
            for x in s.split(" "):
                if x.lower().startswith("resource_list.nodes"):
                    kvs = [int(y.split("=")[1]) for y in x.split(":")]
                    keys["resource_list_nodes"] = sum(kvs[1:])
                else:
                    kv = x.split("=")
                    keys[kv[0].lower().replace('.', '_')] = kv[1]
        except:
            print(s)
    return keys

@udf("long")
def get_sec(time_str):
    if not time_str: return 0
    return long(sum(float(n) * m for n, m in zip(reversed(time_str.split(':')), (1, 60, 3600))))
    
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

torq = glueContext.create_dynamic_frame.from_catalog(database=args['DATABASE_NAME'], table_name=args['TABLE_NAME'])
print "Count: ", torq.count()
torq.printSchema()

torqDF = torq.toDF()

dt = split(torqDF.col0, ' ')

torqDF = torqDF.select(col('col1').alias('job_status'), col('col3').alias('detail'),
          to_timestamp(torqDF.col0, 'MM/dd/yyyy HH:mm:ss').alias('o_dt'), torqDF.col0).where(torqDF.col1 == 'E') \
          .select('job_status', 'detail', 'o_dt', 
          to_date(col("o_dt"),"yyyy-MM-dd").alias('date'),
          year('o_dt').alias('year'),
          month('o_dt').alias('month'),
          dayofmonth('o_dt').alias('day'),
          hour('o_dt').alias('hour'))

with_map = torqDF.withColumn("kvs", map_keys("detail"))

keys = (with_map
  .select(explode("kvs"))
  .select("key")
  .distinct()
  .rdd.flatMap(lambda x: x)
  .collect())

with_map = with_map.select(*["*"] + [col("kvs").getItem(k).alias(k) for k in keys])

# change the data types and column names to be easier to query later
with_map = with_map \
  .withColumn("resources_used_walltime_secs", get_sec("resources_used_walltime")) \
  .withColumn("resource_list_walltime_secs", get_sec("resource_list_walltime")) \
  .withColumn("resources_used_mem_gb", expr("round(CAST(substring(resources_used_mem, 1, length(resources_used_mem)-2) AS LONG) * 0.0001, 0)")) \
  .withColumn("resource_list_nodect", expr("CAST(resource_list_nodect AS INTEGER)")) \
  .withColumn("qtime", expr("CAST(qtime AS LONG)")) \
  .withColumn("start", expr("CAST(start AS LONG)")) \
  .withColumn("ctime", expr("CAST(qtime AS LONG)")) \
  .withColumn("etime", expr("CAST(qtime AS LONG)")) \
  .withColumn("end", expr("CAST(qtime AS LONG)")) \
  .withColumn("exit_status", expr("CAST(exit_status AS INTEGER)")) \
  .withColumnRenamed("group", "group_name") \
  .withColumnRenamed("resource_list_nodes", "resource_list_cores") \
  .drop('resources_used_vmem', 'session', 'detail', 'kvs', 'exec_host', 'resource_list_neednodes')

torq = DynamicFrame.fromDF(with_map, glueContext, "joined")

datasink5 = glueContext.write_dynamic_frame.from_options(frame = torq, connection_type = "s3", connection_options = {"path": 's3://'+args['S3_OUTPUT_PATH'], "partitionKeys": ["year", "month", "day"]}, format="parquet", transformation_ctx="datasink5")

job.commit()