import sys
import boto3
import pprint
import pandas as pd
import re
import json
from awsglue.utils import getResolvedOptions

glue_client = boto3.client("glue")
args = getResolvedOptions(sys.argv, [
                          'WORKFLOW_NAME', 'WORKFLOW_RUN_ID', 'JOB_NAME', 'CUSTOMER', 'SCHEDULER_TYPE' 'REGION'])
workflow_name = args['WORKFLOW_NAME']
workflow_run_id = args['WORKFLOW_RUN_ID']

# if workflow_name:
#     workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name,
#                                         RunId=workflow_run_id)["RunProperties"]
#     # target_database = workflow_params['target_database']
#     # target_s3_location = workflow_params['target_s3_location']

region = args['REGION']
