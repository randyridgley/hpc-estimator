import cdk = require('@aws-cdk/core');
import s3 = require('@aws-cdk/aws-s3');
import glue = require('@aws-cdk/aws-glue');
import iam = require('@aws-cdk/aws-iam');
import { GlueWorkflowResource } from './glue-workflow-resource';
import path = require('path')

interface TorqueStackProps extends cdk.StackProps {
    customerBucket: s3.Bucket;
    glueDatabase: string;
    pricingCrawler: string;
    pricingJob: string;
}

export class TorqueStack extends cdk.Stack {
    public readonly customerBucket: s3.Bucket

    constructor(scope: cdk.Construct, id: string, props: TorqueStackProps) {
        super(scope, id, props);

        const customerName = this.node.tryGetContext("customerName");
        const customerLogBucket = this.node.tryGetContext("customerLogBucket");
        const customerLogKey = this.node.tryGetContext("customerLogKey");

        const glueETLJobRole = new iam.Role(this, 'TorqueETLJobRole', {
            roleName: 'TorqueETLJobServiceRole',
            assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
                iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess')
            ]
        });
    
        props.customerBucket.grantPut(glueETLJobRole);
        props.customerBucket.grantRead(glueETLJobRole);

        const glueCrawlerRole = new iam.Role(this, 'TorqueCrawlerRole', {
            roleName: 'TorqueCrawlerServiceRole',
            assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
                iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess')
            ]
        });

        props.customerBucket.grantRead(glueCrawlerRole);

        const rawCrawlerName = customerName + '-torque-raw-crawler'
        new glue.CfnCrawler(this, 'TorqueRawCrawler', {
            databaseName: props.glueDatabase,
            name: rawCrawlerName,
            targets: {
                s3Targets: [
                    {
                        path: path.join(customerLogBucket, customerLogKey)
                    }
                ]
            },
            role: glueCrawlerRole.roleName,
            tablePrefix: 'o_torque_'
        })

        const parqCrawlerName = customerName + '-torque-parq-crawler'
        new glue.CfnCrawler(this, 'TorqueProcessedCrawler', {
            databaseName: props.glueDatabase,
            name: parqCrawlerName,
            targets: {
                s3Targets: [
                    {
                        path: props.customerBucket.bucketName + '/processed/torque/hpc/'
                    }
                ]
            },
            role: glueCrawlerRole.roleName,
            tablePrefix: 'p_torque_'
        })

        const estimateCrawlerName = customerName + '-estimate-crawler'
        new glue.CfnCrawler(this, 'CuratedHPCLogCrawler', {
            databaseName: props.glueDatabase,
            name: estimateCrawlerName,
            targets: {
                s3Targets: [
                    {
                        path: props.customerBucket.bucketName + '/processed/torque/estimate/'
                    }
                ]
            },
            role: glueCrawlerRole.roleName,
            tablePrefix: 'p_torque_'
        })

        const parqJobName = customerName + '-raw-to-parquet-etl'
        new glue.CfnJob(this, 'HPCRawLogsToParquet', {
            role: glueETLJobRole.roleName,
            command: {
                name: "glueetl",
                scriptLocation: 's3://' + props.customerBucket.bucketName + '/scripts/torque-raw-to-parquet.py'
            },
            name: parqJobName,
            description: 'Convert raw HPC logs to parquet and remove unneeded fields',
            defaultArguments: {
                "--DATABASE_NAME": props.glueDatabase,
                "--TABLE_NAME": 'o_torque_raw', // can I not hard code this value?
                "--S3_OUTPUT_PATH": 's3://' + props.customerBucket.bucketName + '/processed/torque/hpc/',
                "--REGION": this.region,
                "--job-bookmark-option": "job-bookmark-enable"
            },
            allocatedCapacity: 10,
            maxRetries: 0,
            executionProperty: {
                maxConcurrentRuns: 1
            }
        })

        const estimateJobName = customerName + '-torque-pricing-estimate-etl'
        new glue.CfnJob(this, 'HPCCalculatePricingEstimate', {
            role: glueETLJobRole.roleName,
            command: {
                name: "glueetl",
                scriptLocation: 's3://' + props.customerBucket.bucketName + '/scripts/pricing-estimate.py'
            },
            name: estimateJobName,
            description: 'Calculate job costs based on merging EC2 pricing with HPC Logs based on CPU and Memory',
            defaultArguments: {
                "--DATABASE_NAME": props.glueDatabase,
                "--TABLE_NAME": 'p_torque_hpc', // can I not hard code this value?
                "--PRICING_TABLE_NAME": 'o_pricing',
                "--S3_OUTPUT_PATH": 's3://' + props.customerBucket.bucketName + '/processed/torque/estimate/',
                "--REGION": this.region,
                "--job-bookmark-option": "job-bookmark-enable"
            },
            allocatedCapacity: 10,
            maxRetries: 0,
            executionProperty: {
                maxConcurrentRuns: 1
            }
        })

        new GlueWorkflowResource(this, 'TorqueWorkflowResource', {
            rawCrawler: rawCrawlerName,
            parqCrawler: parqCrawlerName,
            pricingCrawler: props.pricingCrawler,
            estimateCrawler: estimateCrawlerName,
            pricingJob: props.pricingJob,
            parqJob: parqJobName,
            estimateJob: estimateJobName,
            customerName: customerName,
            hpcName: 'torque'
        });
    }
}