import os
import os.path
import sys

envLambdaTaskRoot = os.environ["LAMBDA_TASK_ROOT"]
sys.path.insert(0,envLambdaTaskRoot)

import tempfile
import json
import logging
import boto3
import botocore
from botocore.client import ClientError
from botocore.vendored import requests
from uuid import uuid4

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CFN_SUCCESS = "SUCCESS"
CFN_FAILED = "FAILED"

start_trigger_name = 'start_hpc_estimate'
pricing_trigger_name = 'trigger_pricing_crawler'
raw_trigger_name = 'trigger_parq_etl'
parq_trigger_name = 'trigger_crawl_parq'
wait_trigger_name = 'trigger_wait_pricing_parq'
estimate_trigger_name = 'trigger_estimate_crawler'

client = boto3.client("glue")

def main(event, context):

    def cfn_error(message=None):
        logger.error("| cfn_error: %s" % message)
        cfn_send(event, context, CFN_FAILED, reason=message)

    try:
        logger.info(event)

        # cloudformation request type (create/update/delete)
        request_type = event['RequestType']

        # extract resource properties
        props = event['ResourceProperties']
        old_props = event.get('OldResourceProperties', {})
        physical_id = event.get('PhysicalResourceId', None)

        try:
            rawCrawler = props['RawCrawler']
            parqCrawler = props['ParqCrawler']
            pricingCrawler = props['PricingCrawler']
            estimateCrawler = props['EstimateCrawler']
            pricingJob = props['PricingJob']
            parqJob = props['ParqJob']
            estimateJob = props['EstimateJob'] 
            customerName = props['CustomerName']
            workflow_name = customerName + '-hpc-workflow'
        except KeyError as e:
            cfn_error("missing request resource property %s. props: %s" % (str(e), props))
            return

        # if we are creating a new resource, allocate a physical id for it
        # otherwise, we expect physical id to be relayed by cloudformation
        if request_type == "Create":
            physical_id = "aws.glue.workflow.%s" % str(uuid4())

            response = client.create_workflow(
                Name=workflow_name,
                Description='HPC Estimation workflow for ' + customerName,
                DefaultRunProperties={
                    'hpc_logs': 'somepath'
                },

            )

            # create the 6 triggers and the workflow
            start_trigger = dict(
                Name=start_trigger_name,
                Description='Trigger to start the estimate of for customers HPC cluster',
                Type='ON_DEMAND',
                WorkflowName=workflow_name,
                Actions=[
                    dict(
                        JobName=pricingJob
                    ), 
                    dict(
                        CrawlerName=rawCrawler
                    )
                ]
            )  

            start_response = client.create_trigger(**start_trigger)

            pricing_trigger = dict(
                Name=pricing_trigger_name,
                Description='Trigger to crawl the AWS EC2 pricing data set',
                Type='CONDITIONAL',
                WorkflowName=workflow_name,
                Actions=[dict(CrawlerName=pricingCrawler)],
                Predicate=dict(
                    Logical='AND',
                    Conditions=[
                        dict(
                            JobName=pricingJob,
                            LogicalOperator='EQUALS',
                            State='SUCCEEDED'
                        )
                    ]
                )
            )  

            pricing_response = client.create_trigger(**pricing_trigger)

            raw_to_parq_trigger = dict(
                Name=raw_trigger_name,
                Description='Trigger to run Glue ETL job converting raw HPC logs to Parquet',
                Type='CONDITIONAL',
                WorkflowName=workflow_name,
                Actions=[dict(JobName=parqJob)],
                Predicate=dict(
                    Logical='AND',
                    Conditions=[
                        dict(
                            CrawlerName=rawCrawler,
                            LogicalOperator='EQUALS',
                            CrawlState='SUCCEEDED'
                        )
                    ]
                )
            )  

            raw_response = client.create_trigger(**raw_to_parq_trigger)

            crawl_parq_trigger = dict(
                Name=parq_trigger_name,
                Description='Trigger to crawl Parquet converted HPC logs',
                Type='CONDITIONAL',
                WorkflowName=workflow_name,
                Actions=[dict(CrawlerName=parqCrawler)],
                Predicate=dict(
                    Logical='AND',
                    Conditions=[
                        dict(
                            JobName=parqJob,
                            LogicalOperator='EQUALS',
                            State='SUCCEEDED'
                        )
                    ]
                )
            )  

            parq_response = client.create_trigger(**crawl_parq_trigger)

            wait_trigger = dict(
                Name=wait_trigger_name,
                Description='Trigger to wait for pricing and parquet to run estimate Glue Job',
                Type='CONDITIONAL',
                WorkflowName=workflow_name,
                Actions=[dict(JobName=estimateJob)],
                Predicate=dict(
                    Logical='AND',
                    Conditions=[
                        dict(
                            CrawlerName=parqCrawler,
                            LogicalOperator='EQUALS',
                            CrawlState='SUCCEEDED'
                        ),
                        dict(
                            CrawlerName=pricingCrawler,
                            LogicalOperator='EQUALS',
                            CrawlState='SUCCEEDED'
                        )
                    ]
                )
            )  

            wait_response = client.create_trigger(**wait_trigger)

            estimate_crawl_trigger = dict(
                Name=estimate_trigger_name,
                Description='Trigger to crawl estimate data set',
                Type='CONDITIONAL',
                WorkflowName=workflow_name,
                Actions=[dict(CrawlerName=estimateCrawler)],
                Predicate=dict(
                    Logical='AND',
                    Conditions=[
                        dict(
                            JobName=estimateJob,
                            LogicalOperator='EQUALS',
                            State='SUCCEEDED'
                        )
                    ]
                )
            )  

            estimate_crawl_response = client.create_trigger(**estimate_crawl_trigger)            
        else:
            if not physical_id:
                cfn_error("invalid request: request type is '%s' but 'PhysicalResourceId' is not defined" % request_type)
                return

        # delete
        if request_type == "Delete":
            response = client.delete_workflow(
                Name=workflow_name
            )

            triggers = [start_trigger_name, pricing_trigger_name, raw_trigger_name, parq_trigger_name, wait_trigger_name, estimate_trigger_name]
            
            for trigger in triggers: 
                try:
                    print('deleting {0}'.format(trigger))
                    response = client.delete_trigger(
                        Name=trigger
                    )                    
                except Exception as e:
                    cfn_error("Failed to delete triggers and workflow %s." % (str(e)))
                    return

        cfn_send(event, context, CFN_SUCCESS, physicalResourceId=physical_id)
    except KeyError as e:
        cfn_error("invalid request. Missing key %s" % str(e))
    except Exception as e:
        logger.exception(e)
        cfn_error(str(e))

#---------------------------------------------------------------------------------------------------
# sends a response to cloudformation
def cfn_send(event, context, responseStatus, responseData={}, physicalResourceId=None, noEcho=False, reason=None):

    responseUrl = event['ResponseURL']
    logger.info(responseUrl)

    responseBody = {}
    responseBody['Status'] = responseStatus
    responseBody['Reason'] = reason or ('See the details in CloudWatch Log Stream: ' + context.log_stream_name)
    responseBody['PhysicalResourceId'] = physicalResourceId or context.log_stream_name
    responseBody['StackId'] = event['StackId']
    responseBody['RequestId'] = event['RequestId']
    responseBody['LogicalResourceId'] = event['LogicalResourceId']
    responseBody['NoEcho'] = noEcho
    responseBody['Data'] = responseData

    body = json.dumps(responseBody)
    logger.info("| response body:\n" + body)

    headers = {
        'content-type' : '',
        'content-length' : str(len(body))
    }

    try:
        response = requests.put(responseUrl, data=body, headers=headers)
        logger.info("| status code: " + response.reason)
    except Exception as e:
        logger.error("| unable to send response to CloudFormation")
        logger.exception(e)