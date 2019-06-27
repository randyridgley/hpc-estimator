import cdk = require('@aws-cdk/core');
import s3 = require('@aws-cdk/aws-s3');
import glue = require('@aws-cdk/aws-glue');
import iam = require('@aws-cdk/aws-iam');
import { GlueWorkflowResource } from './glue-workflow-resource';
import path = require('path')

interface GlueStackProps extends cdk.StackProps {
    database: string;
    customerBucket: s3.Bucket;
}

export class GlueJobStack extends cdk.Stack {
    public readonly customerBucket: s3.Bucket

    constructor(scope: cdk.Construct, id: string, props: GlueStackProps) {
        super(scope, id, props);

        const customerName = this.node.tryGetContext("customerName");
        const customerLogBucket = this.node.tryGetContext("customerLogBucket");
        const customerLogKey = this.node.tryGetContext("customerLogKey");
    
        const gluePricingJobRole = new iam.Role(this, 'GluePricingJobRole', {
            roleName: 'GluePricingJobServiceRole',
            assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
                iam.ManagedPolicy.fromAwsManagedPolicyName('AWSPriceListServiceFullAccess'),
            ]
        });

        props.customerBucket.grantPut(gluePricingJobRole);
        props.customerBucket.grantRead(gluePricingJobRole);

        const glueETLJobRole = new iam.Role(this, 'GlueETLJobRole', {
            roleName: 'GlueETLJobServiceRole',
            assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole')
            ]
        });

        props.customerBucket.grantPut(glueETLJobRole);
        props.customerBucket.grantRead(glueETLJobRole);

        const glueCrawlerRole = new iam.Role(this, 'GlueCrawlerRole', {
            roleName: 'GlueCrawlerServiceRole',
            assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole')
            ]
        });

        props.customerBucket.grantRead(glueCrawlerRole);

        const rawCrawlerName = customerName + '-raw-crawler'
        new glue.CfnCrawler(this, 'RawHPCLogCrawler', {
            databaseName: props.database,
            name: rawCrawlerName,
            targets: {
                s3Targets: [
                    {
                        path: path.join(customerLogBucket, customerLogKey)
                    }
                ]
            },
            role: glueCrawlerRole.roleName,
            tablePrefix: 'o_'
        })

        const parqCrawlerName = customerName + '-parq-crawler'
        new glue.CfnCrawler(this, 'ProcessedHPCLogCrawler', {
            databaseName: props.database,
            name: parqCrawlerName,
            targets: {
                s3Targets: [
                    {
                        path: props.customerBucket.bucketName + '/processed/hpc/'
                    }
                ]
            },
            role: glueCrawlerRole.roleName,
            tablePrefix: 'p_'
        })

        const estimateCrawlerName = customerName + '-estimate-crawler'
        new glue.CfnCrawler(this, 'CuratedHPCLogCrawler', {
            databaseName: props.database,
            name: estimateCrawlerName,
            targets: {
                s3Targets: [
                    {
                        path: props.customerBucket.bucketName + '/processed/estimate/'
                    }
                ]
            },
            role: glueCrawlerRole.roleName,
            tablePrefix: 'p_'
        })

        const pricingCrawlerName = customerName + '-pricing-crawler'
        new glue.CfnCrawler(this, 'AWSPricingCrawler', {
            databaseName: props.database,
            name: pricingCrawlerName,
            targets: {
                s3Targets: [
                    {
                        path: props.customerBucket.bucketName + '/raw/pricing/'
                    }
                ]
            },
            role: glueCrawlerRole.roleName,
            tablePrefix: 'o_'
        })

        const pricingJobName = customerName + '-pricing-builder'
        new glue.CfnJob(this, 'AWSEC2PricingGenerator', {
            role: gluePricingJobRole.roleName,
            command: {
                name: "pythonshell",
                scriptLocation: 's3://' + props.customerBucket.bucketName + '/scripts/pricing-builder.py',
            },
            name: pricingJobName,
            description: 'AWS Pricing for OnDemand and Spot instances for HPC jobs',
            defaultArguments: {
                "--DATABASE_NAME": props.database,
                "--S3_OUTPUT_BUCKET": props.customerBucket.bucketName,
                "--S3_OUTPUT_KEY": '/raw/pricing/',
                "--REGION": this.region
            },
            maxRetries: 0,
            executionProperty: {
                maxConcurrentRuns: 1
            }
        })

        const parqJobName = customerName + '-raw-to-parquet-etl'
        new glue.CfnJob(this, 'HPCRawLogsToParquet', {
            role: glueETLJobRole.roleName,
            command: {
                name: "glueetl",
                scriptLocation: 's3://' + props.customerBucket.bucketName + '/scripts/raw-to-parquet.py'
            },
            name: parqJobName,
            description: 'Convert raw HPC logs to parquet and remove unneeded fields',
            defaultArguments: {
                "--DATABASE_NAME": props.database,
                "--TABLE_NAME": 'o_hpc', // can I not hard code this value?
                "--S3_OUTPUT_PATH": 's3://' + props.customerBucket.bucketName + '/raw/pricing/',
                "--REGION": this.region,
                "--job-bookmark-option": "job-bookmark-enable"
            },
            allocatedCapacity: 10,
            maxRetries: 0,
            executionProperty: {
                maxConcurrentRuns: 1
            }
        })

        const estimateJobName = customerName + '-hpc-pricing-estimate-etl'
        new glue.CfnJob(this, 'HPCCalculatePricingEstimate', {
            role: glueETLJobRole.roleName,
            command: {
                name: "glueetl",
                scriptLocation: 's3://' + props.customerBucket.bucketName + '/scripts/hpc-pricing-estimate.py'
            },
            name: estimateJobName,
            description: 'Calculate job costs based on merging EC2 pricing with HPC Logs based on CPU and Memory',
            defaultArguments: {
                "--DATABASE_NAME": props.database,
                "--TABLE_NAME": 'p_hpc', // can I not hard code this value?
                "--PRICING_NAME": 'o_pricing',
                "--S3_OUTPUT_PATH": 's3://' + props.customerBucket.bucketName + '/raw/estimate/',
                "--REGION": this.region,
                "--job-bookmark-option": "job-bookmark-enable"
            },
            allocatedCapacity: 10,
            maxRetries: 0,
            executionProperty: {
                maxConcurrentRuns: 1
            }
        })

        const resource = new GlueWorkflowResource(this, 'GlueWorkflowResource', {
            rawCrawler: rawCrawlerName,
            parqCrawler: parqCrawlerName,
            pricingCrawler: pricingCrawlerName,
            estimateCrawler: estimateCrawlerName,
            pricingJob: pricingJobName,
            parqJob: parqJobName,
            estimateJob: estimateJobName,
            customerName: customerName,
        });
    }
}