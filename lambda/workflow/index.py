import os
import os.path
import sys

# envLambdaTaskRoot = os.environ["LAMBDA_TASK_ROOT"]
# sys.path.insert(0, envLambdaTaskRoot)

import tempfile
import json
import logging
import boto3
import botocore
from botocore.client import ClientError
from botocore.vendored import requests
from uuid import uuid4
print(boto3.__version__)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

start_trigger_name = 'start_hpc_estimate'
pricing_trigger_name = 'trigger_pricing_crawler'
raw_trigger_name = 'trigger_parq_etl'
parq_trigger_name = 'trigger_crawl_parq'
wait_trigger_name = 'trigger_wait_pricing_parq'
estimate_trigger_name = 'trigger_estimate_crawler'

client = boto3.client("glue")


def perform_s3_copy_recursive(s3_client, from_path, to_path):
    from_bucket, from_key_path = extract_bucket_key(from_path)
    to_bucket, to_key_path = extract_bucket_key(to_path)
    s3_paginator = s3_client.get_paginator('list_objects')
    for page in s3_paginator.paginate(Bucket=from_bucket, Prefix=from_key_path):
        if page.get('Contents'):
            for key in page.get('Contents'):
                # Extract filename relative to the from_path
                base_filename = key.get('Key').replace(from_key_path, '')
                if base_filename:
                    _logger.info('Copying key %s to destination %s' % (
                        from_key_path + base_filename, to_key_path + base_filename))
                    copy_s3_object(s3_client, from_bucket, from_key_path + base_filename, to_bucket,
                                   to_key_path + base_filename)


def create_workflow(body):
    logger.info(body)

    try:
        parq_crawler = os.environ['PARQ_CRAWLER']
        pricing_crawler = os.environ['PRICING_CRAWLER']
        estimate_crawler = os.environ['ESTIMATE_CRAWLER']
        pricing_job = os.environ['PRICING_JOB']
        slurm_parq_job = os.environ['SLURM_PARQ_JOB']
        sge_parq_job = os.environ['SGE_PARQ_JOB']
        torque_parq_job = os.environ['TORQUE_PARQ_JOB']
        estimate_job = os.environ['ESTIMATE_JOB']
        raw_crawler = os.environ['RAW_CRAWLER']
        workflow_bucket = os.environ['WORKFLOW_BUCKET']
        customer_name = body["customerName"]
        schedule_type = body["schedulerType"]
        logger.info('scheduler type {0}'.format(schedule_type))
        raw_data_location = body["rawDataS3Path"]
        workflow_name = customer_name + '-hpc-workflow'
    except KeyError as e:
        print('Failed with e {}'.format(e))
        return respond("Error loading paramaters {}".format(e))

    # sync_command = f"aws s3 sync {0} s3://{1}/raw/hpc/".format(
    #     raw_data_location, workflow_bucket
    # )
    # os.system(sync_command)

    logger.info('Creating workflow')
    workflow_name = "{0}-{1}".format(schedule_type, workflow_name)
    client.create_workflow(
        Name=workflow_name,
        Description='HPC Estimation workflow for {0}'.format(customer_name),
        DefaultRunProperties={
            'hpc_logs': 'somepath'
        },
    )

    logger.info('Creating Start Trigger')
    try:
        start_trigger = dict(
            Name="{0}_{1}".format(schedule_type, start_trigger_name),
            Description='Trigger to estimate customers HPC cost',
            Type='ON_DEMAND',
            WorkflowName=workflow_name,
            Actions=[
                dict(
                    JobName=pricing_job
                ),
                dict(
                    CrawlerName=raw_crawler
                )
            ]
        )
        print(start_trigger)
        client.create_trigger(**start_trigger)
    except Exception as e:
        print('Failed with e {}'.format(e))
        return respond("Error creating  {}".format(e))

    logger.info('Creating Pricing Trigger')
    pricing_trigger = dict(
        Name="{0}_{1}".format(schedule_type, pricing_trigger_name),
        Description='Trigger to crawl the AWS EC2 pricing data set',
        Type='CONDITIONAL',
        WorkflowName=workflow_name,
        Actions=[dict(CrawlerName=pricing_crawler)],
        Predicate=dict(
            Logical='ANY',
            Conditions=[
                dict(
                    JobName=pricing_job,
                    LogicalOperator='EQUALS',
                    State='SUCCEEDED'
                )
            ]
        ),
        StartOnCreation=True
    )

    client.create_trigger(**pricing_trigger)

    logger.info('Creating Raw Job')
    parq_job = torque_parq_job

    if schedule_type.lower() == "slurm":
        parq_job = slurm_parq_job
    elif schedule_type.lower() == "sge":
        parq_job == sge_parq_job

    raw_to_parq_trigger = dict(
        Name="{0}_{1}".format(schedule_type, raw_trigger_name),
        Description='Trigger to convert raw HPC logs to Parquet',
        Type='CONDITIONAL',
        WorkflowName=workflow_name,
        Actions=[dict(JobName=parq_job)],
        Predicate=dict(
            Logical='ANY',
            Conditions=[
                dict(
                    CrawlerName=raw_crawler,
                    LogicalOperator='EQUALS',
                    CrawlState='SUCCEEDED'
                )
            ]
        ),
        StartOnCreation=True
    )

    client.create_trigger(**raw_to_parq_trigger)

    logger.info('Creating Parq Job')
    crawl_parq_trigger = dict(
        Name="{0}_{1}".format(schedule_type, parq_trigger_name),
        Description='Trigger to crawl Parquet converted HPC logs',
        Type='CONDITIONAL',
        WorkflowName=workflow_name,
        Actions=[dict(CrawlerName=parq_crawler)],
        Predicate=dict(
            Logical='ANY',
            Conditions=[
                dict(
                    JobName=parq_job,
                    LogicalOperator='EQUALS',
                    State='SUCCEEDED'
                )
            ]
        ),
        StartOnCreation=True
    )

    client.create_trigger(**crawl_parq_trigger)

    logger.info('Creating Wait Trigger')
    wait_trigger = dict(
        Name="{0}_{1}".format(schedule_type, wait_trigger_name),
        Description='Trigger to wait to run estimate',
        Type='CONDITIONAL',
        WorkflowName=workflow_name,
        Actions=[dict(JobName=estimate_job)],
        Predicate=dict(
            Logical='AND',
            Conditions=[
                dict(
                    CrawlerName=parq_crawler,
                    LogicalOperator='EQUALS',
                    CrawlState='SUCCEEDED'
                ),
                dict(
                    CrawlerName=pricing_crawler,
                    LogicalOperator='EQUALS',
                    CrawlState='SUCCEEDED'
                )
            ]
        ),
        StartOnCreation=True
    )

    client.create_trigger(**wait_trigger)

    logger.info('Creating Estimate Trigger')
    estimate_crawl_trigger = dict(
        Name="{0}_{1}".format(schedule_type, estimate_trigger_name),
        Description='Trigger to crawl estimate data set',
        Type='CONDITIONAL',
        WorkflowName=workflow_name,
        Actions=[dict(CrawlerName=estimate_crawler)],
        Predicate=dict(
            Logical='ANY',
            Conditions=[
                dict(
                    JobName=estimate_job,
                    LogicalOperator='EQUALS',
                    State='SUCCEEDED'
                )
            ]
        ),
        StartOnCreation=True
    )

    client.create_trigger(**estimate_crawl_trigger)
    return '{status: "OK"}'


def run_workflow(body):
    print(body)
    return ""


def get_workflow(body):
    print(body)
    return ""


def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': err if err else json.dumps(res),
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/json',
            'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
            'X-Requested-With': '*',
        },
    }


def handler(event, context):
    try:
        print(f"httpMethod: {event['httpMethod']}")
        print(f"path: {event['path']}")
        http_method = event['httpMethod']

        if http_method == 'OPTIONS':
            return respond(None, {})

        if http_method == 'POST':
            update = create_workflow(json.loads(event["body"]))
            return respond(None, update)

        if http_method == 'PUT':
            update = run_workflow(json.loads(event["body"]))
            return respond(None, update)

        if http_method == 'GET':
            update = get_workflow(json.loads(event["body"]))
            return respond(None, update)

        return respond(f"Invalid path: {event['path']}", {})
    except Exception as e:
        print('Failed with e {0}'.format(e))
        return respond("Error fetching updates {}".format(e))
