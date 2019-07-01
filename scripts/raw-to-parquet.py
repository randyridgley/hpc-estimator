import sys
import math
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import types as T
from pyspark.sql.functions import col, udf, explode, expr, to_timestamp, to_date, year, month, dayofmonth, hour, split, monotonically_increasing_id

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATABASE_NAME', 'TABLE_NAME', 'S3_OUTPUT_PATH'])

# need to cleanup parsing resource_list.nodes it has a number of combinations not sure I've found all
@udf("map<string,string>")
def map_keys(s):
    keys = {}

    if s:          
      for x in s.split(" "):        
        if x.lower().startswith("resource_list.nodes"):
            kvs = x[len("resource_List."):].split(":")
            
            try:
              gpu = kvs[2]
              keys["resource_list_gpu"] = 1
              keys["resource_list_gpu_type"] = gpu
            except IndexError:
              keys["resource_list_gpu"] = 0
                
            nodes = kvs[0].split('=')[1]

            try:          
              cpus = kvs[1].split('=')[1]
            except IndexError:
              cpus = 4 * int(nodes) # this is a guess on how many cpus would be needed if not passed in

            keys["resource_list_nodes"] = nodes
            keys["resource_list_cpu"] = cpus
        else:
            kv = x.split("=")
            keys[kv[0].lower().replace('.', '_')] = kv[1]
    return keys

@udf("long")
def get_sec(time_str):
    if not time_str: return 0
    return long(sum(float(n) * m for n, m in zip(reversed(time_str.split(':')), (1, 60, 3600))))
    
@udf("long")
def convert_to_gb(kb):
  if not kb: return 1
  return long(math.ceil(float(kb[:len(kb)-2]) * 0.000001))


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
  .withColumn("id", monotonically_increasing_id()) \
  .withColumn("resources_used_walltime_secs", get_sec("resources_used_walltime")) \
  .withColumn("resource_list_walltime_secs", get_sec("resource_list_walltime")) \
  .withColumn("resources_used_mem_gb", convert_to_gb("resources_used_mem")) \
  .withColumn("resource_list_nodect", expr("CAST(resource_list_nodect AS INTEGER)")) \
  .withColumn("resource_list_cpu", expr("CAST(resource_list_cpu AS INTEGER)")) \
  .withColumn("resource_list_gpu", expr("CAST(resource_list_gpu AS INTEGER)")) \
  .withColumn("qtime", expr("CAST(qtime AS LONG)")) \
  .withColumn("start", expr("CAST(start AS LONG)")) \
  .withColumn("ctime", expr("CAST(qtime AS LONG)")) \
  .withColumn("etime", expr("CAST(qtime AS LONG)")) \
  .withColumn("end", expr("CAST(qtime AS LONG)")) \
  .withColumn("exit_status", expr("CAST(exit_status AS INTEGER)")) \
  .withColumnRenamed("group", "group_name") \
  .withColumn("resource_list_cores", expr("CAST(resource_list_nodes as LONG) * CAST(resource_list_cpu as INTEGER)")) \
  .drop('resources_used_vmem', 'kvs', 'session', 'exec_host', 'resource_list_neednodes')
# eventually drop detail and the asked resources to only use actually used

torq = DynamicFrame.fromDF(with_map, glueContext, "joined")

datasink5 = glueContext.write_dynamic_frame.from_options(frame = torq, connection_type = "s3", connection_options = {"path": args['S3_OUTPUT_PATH'], "partitionKeys": ["year", "month", "day"]}, format="parquet", transformation_ctx="datasink5")

job.commit()