import os
import os.path
import sys
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

client = boto3.client('glue')
account_id = boto3.client('sts').get_caller_identity().get('Account')


def create_raw_table(raw_data_location, database_name, scheduler_type):
    if scheduler_type == 'torque':
        client.create_table(
            CatalogId=account_id,
            DatabaseName=database_name,
            TableInput={
                'Name': 'o_raw',
                'Description': 'Raw HPC Logs for Torque Scheduler',
                'StorageDescriptor': {
                    'Columns': [
                        {
                            'Name': 'col0',
                            'Type': 'string'
                        },
                        {
                            'Name': 'col1',
                            'Type': 'string'
                        },
                        {
                            'Name': 'col2',
                            'Type': 'string'
                        },
                        {
                            'Name': 'col3',
                            'Type': 'string'
                        }
                    ],
                    'Location': raw_data_location,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        'Parameters': {
                            'field.delim': ';'
                        }
                    }
                },
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': {
                    'classification': 'csv',
                    'delimiter': ';',
                }
            }
        )
    elif scheduler_type == 'slurm':
        client.create_table(
            CatalogId=account_id,
            DatabaseName=database_name,
            TableInput={
                'Name': 'o_raw',
                'Description': 'Raw HPC Logs for Slurm Scheduler',
                'StorageDescriptor': {
                    'Columns': [
                        {
                            'Name': 'jobid',
                            'Type': 'string'
                        },
                        {
                            'Name': 'elapsed',
                            'Type': 'string'
                        },
                        {
                            'Name': 'ncpus',
                            'Type': 'bigint'
                        },
                        {
                            'Name': 'state',
                            'Type': 'string'
                        },
                        {
                            'Name': 'user',
                            'Type': 'string'
                        },
                        {
                            'Name': 'partition',
                            'Type': 'string'
                        },
                        {
                            'Name': 'elapsedraw',
                            'Type': 'bigint'
                        }
                    ],
                    'Location': raw_data_location,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        'Parameters': {
                            'field.delim': '|'
                        }
                    }
                },
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': {
                    'classification': 'csv',
                    'delimiter': '|',
                    'skip.header.line.count': '1',
                }
            }
        )
    else:
        print('create sge table')


def create_pricing_table(database_name, pricing_location):
    client.create_table(
        CatalogId=account_id,
        DatabaseName=database_name,
        TableInput={
            'Name': 'o_pricing',
            'Description': 'EC2 Pricing table',
            'StorageDescriptor': {
                'Columns': [
                    {
                        'Name': 'discount',
                        'Type': 'bigint'
                    },
                    {
                        'Name': 'gpu',
                        'Type': 'bigint'
                    },
                    {
                        'Name': 'instancetype',
                        'Type': 'string'
                    },
                    {
                        'Name': 'memory',
                        'Type': 'bigint'
                    },
                    {
                        'Name': 'ondemandprice',
                        'Type': 'double'
                    },
                    {
                        'Name': 'spotprice',
                        'Type': 'double'
                    },
                    {
                        'Name': 'vcpu',
                        'Type': 'bigint'
                    }
                ],
                'Location': pricing_location,
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                    'Parameters': {
                        'field.delim': ','
                    }
                }
            },
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'classification': 'csv',
                'delimiter': ',',
                'skip.header.line.count': '1',
            }
        }
    )


def create_estimate_table(database_name, estimate_location):
    client.create_table(
        CatalogId=account_id,
        DatabaseName=database_name,
        TableInput={
            'Name': 'p_estimate',
            'Description': 'HPC Logs Pricing Estimate Table',
            'StorageDescriptor': {
                'Columns': [
                    {
                        'Name': 'job_status',
                        'Type': 'string'
                    },
                    {
                        'Name': 'hour',
                        'Type': 'int'
                    },
                    {
                        'Name': 'gpu_type',
                        'Type': 'string'
                    },
                    {
                        'Name': 'queue',
                        'Type': 'string'
                    },
                    {
                        'Name': 'group_name',
                        'Type': 'string'
                    },
                    {
                        'Name': 'exit_status',
                        'Type': 'int'
                    },
                    {
                        'Name': 'user',
                        'Type': 'string'
                    },
                    {
                        'Name': 'job_name',
                        'Type': 'string'
                    },
                    {
                        'Name': 'etime',
                        'Type': 'bigint'
                    },
                    {
                        'Name': 'owner',
                        'Type': 'string'
                    },
                    {
                        'Name': 'id',
                        'Type': 'bigint'
                    },
                    {
                        'Name': 'walltime_secs',
                        'Type': 'bigint'
                    },
                    {
                        'Name': 'cpu_time',
                        'Type': 'bigint'
                    },
                    {
                        'Name': 'mem_gb',
                        'Type': 'bigint'
                    },
                    {
                        'Name': 'node_ct',
                        'Type': 'int'
                    },
                    {
                        'Name': 'num_cpus',
                        'Type': 'int'
                    },
                    {
                        'Name': 'num_gpus',
                        'Type': 'int'
                    },
                    {
                        'Name': 'queued_time',
                        'Type': 'bigint'
                    },
                    {
                        'Name': 'start_time',
                        'Type': 'bigint'
                    },
                    {
                        'Name': 'created_time',
                        'Type': 'bigint'
                    },
                    {
                        'Name': 'end_time',
                        'Type': 'bigint'
                    },
                    {
                        'Name': 'num_cores',
                        'Type': 'bigint'
                    },
                    {
                        'Name': 'walltime_hrs',
                        'Type': 'float'
                    },
                    {
                        'Name': 'cpu_time_hrs',
                        'Type': 'float'
                    },
                    {
                        'Name': 'year',
                        'Type': 'int'
                    },
                    {
                        'Name': 'month',
                        'Type': 'int'
                    },
                    {
                        'Name': 'day',
                        'Type': 'int'
                    },
                    {
                        'Name': 'discount',
                        'Type': 'bigint'
                    },
                    {
                        'Name': 'gpu',
                        'Type': 'bigint'
                    },
                    {
                        'Name': 'instancetype',
                        'Type': 'string'
                    },
                    {
                        'Name': 'memory',
                        'Type': 'bigint'
                    },
                    {
                        'Name': 'ondemandprice',
                        'Type': 'double'
                    },
                    {
                        'Name': 'spotprice',
                        'Type': 'double'
                    },
                    {
                        'Name': 'vcpu',
                        'Type': 'bigint'
                    },
                    {
                        'Name': 'rank',
                        'Type': 'int'
                    },
                    {
                        'Name': 'job_cost',
                        'Type': 'float'
                    }
                ],
                'Location': estimate_location,
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                    'Parameters': {
                        'serialization.format': '1'
                    }
                },
                'Parameters': {
                    'classification': 'parquet'
                }
            },
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'classification': 'parquet'
            }
        }
    )


def create_workflow(body):
    logger.info(body)

    try:
        parq_crawler = os.environ['PARQ_CRAWLER']
        pricing_job = os.environ['PRICING_JOB']
        slurm_parq_job = os.environ['SLURM_PARQ_JOB']
        sge_parq_job = os.environ['SGE_PARQ_JOB']
        torque_parq_job = os.environ['TORQUE_PARQ_JOB']
        estimate_job = os.environ['ESTIMATE_JOB']
        workflow_bucket = os.environ['WORKFLOW_BUCKET']
        database_name = os.environ['GLUE_DATABASE_NAME']
        customer_name = body['customerName']
        schedule_type = body['schedulerType']
        logger.info('scheduler type {0}'.format(schedule_type))
        raw_data_location = body['rawDataS3Path']
        workflow_name = customer_name + '-hpc-workflow'
    except KeyError as e:
        print('Failed with e {}'.format(e))
        return respond('Error loading paramaters {}'.format(e))

    logger.info('Creating workflow')
    workflow_name = '{0}-{1}'.format(schedule_type, workflow_name)
    resp = client.create_workflow(
        Name=workflow_name,
        Description='HPC Estimation workflow for {0}'.format(customer_name),
        DefaultRunProperties={
            'hpc_logs': 'somepath'
        },
    )

    logger.info('Creating Raw Job')
    parq_job = torque_parq_job

    if schedule_type.lower() == 'slurm':
        parq_job = slurm_parq_job
    elif schedule_type.lower() == 'sge':
        parq_job == sge_parq_job

    create_raw_table(raw_data_location, database_name, schedule_type)
    create_pricing_table(
        database_name,
        's3://{0}/raw/pricing/'.format(workflow_bucket)
    )
    create_estimate_table(
        database_name,
        's3://{0}/processed/estimate/'.format(workflow_bucket)
    )

    logger.info('Creating Start Trigger')
    try:
        start_trigger = dict(
            Name='{0}_{1}'.format(schedule_type, start_trigger_name),
            Description='Trigger to estimate customers HPC cost',
            Type='ON_DEMAND',
            WorkflowName=workflow_name,
            Actions=[
                dict(
                    JobName=pricing_job
                ),
                dict(
                    JobName=parq_job
                )
            ]
        )
        print(start_trigger)
        client.create_trigger(**start_trigger)
    except Exception as e:
        print('Failed with e {}'.format(e))
        return respond('Error creating  {}'.format(e))

    logger.info('Creating Parq Job')
    crawl_parq_trigger = dict(
        Name='{0}_{1}'.format(schedule_type, parq_trigger_name),
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
        Name='{0}_{1}'.format(schedule_type, wait_trigger_name),
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
                    JobName=pricing_job,
                    LogicalOperator='EQUALS',
                    State='SUCCEEDED'
                )
            ]
        ),
        StartOnCreation=True
    )

    client.create_trigger(**wait_trigger)

    # logger.info('Creating Estimate Trigger')
    # estimate_crawl_trigger = dict(
    #     Name='{0}_{1}'.format(schedule_type, estimate_trigger_name),
    #     Description='Trigger to crawl estimate data set',
    #     Type='CONDITIONAL',
    #     WorkflowName=workflow_name,
    #     Actions=[dict(CrawlerName=estimate_crawler)],
    #     Predicate=dict(
    #         Logical='ANY',
    #         Conditions=[
    #             dict(
    #                 JobName=estimate_job,
    #                 LogicalOperator='EQUALS',
    #                 State='SUCCEEDED'
    #             )
    #         ]
    #     ),
    #     StartOnCreation=True
    # )

    # client.create_trigger(**estimate_crawl_trigger)
    return resp


def run_workflow(body):
    print(body)
    return ''


def get_workflow(body):
    print(body)
    return ''


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
