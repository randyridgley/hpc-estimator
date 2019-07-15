import cdk = require('@aws-cdk/core');
import s3 = require('@aws-cdk/aws-s3');
import glue = require('@aws-cdk/aws-glue');
import iam = require('@aws-cdk/aws-iam');

interface EstimateStackProps extends cdk.StackProps {
    customerBucket: s3.Bucket;
    glueDatabase: string;
}

export class EstimateStack extends cdk.Stack {
    public readonly estimateJobName: string
    public readonly parqCrawlerName: string
    public readonly slurmParqJobName: string
    public readonly sgeParqJobName: string
    public readonly torqueParqJobName: string

    constructor(scope: cdk.Construct, id: string, props: EstimateStackProps) {
        super(scope, id, props);

        const glueETLJobRole = new iam.Role(this, 'ETLJobRole', {
            roleName: 'ETLJobServiceRole',
            assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
                iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess')
            ]
        });
    
        props.customerBucket.grantPut(glueETLJobRole);
        props.customerBucket.grantRead(glueETLJobRole);

        const glueCrawlerRole = new iam.Role(this, 'CrawlerRole', {
            roleName: 'HPCCrawlerServiceRole',
            assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
                iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess')
            ]
        });

        props.customerBucket.grantRead(glueCrawlerRole);

        this.parqCrawlerName = 'hpc-parq-crawler'
        new glue.CfnCrawler(this, 'HPCParquetCrawler', {
            databaseName: props.glueDatabase,
            name: this.parqCrawlerName,
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

        this.sgeParqJobName = 'sge-raw-to-parquet-etl'
        new glue.CfnJob(this, 'SGERawToParquet', {
            role: glueETLJobRole.roleName,
            command: {
                name: "glueetl",
                scriptLocation: 's3://' + props.customerBucket.bucketName + '/scripts/sge-raw-to-parquet.py'
            },
            name: this.sgeParqJobName,
            description: 'Convert raw HPC logs to parquet and remove unneeded fields',
            defaultArguments: {
                "--DATABASE_NAME": props.glueDatabase,
                "--TABLE_NAME": 'o_raw', // can I not hard code this value?
                "--S3_OUTPUT_PATH": 's3://' + props.customerBucket.bucketName + '/processed/hpc/',
                "--REGION": this.region,
                "--job-bookmark-option": "job-bookmark-enable"
            },
            allocatedCapacity: 10,
            maxRetries: 0,
            executionProperty: {
                maxConcurrentRuns: 1
            }
        })

        this.slurmParqJobName = 'slurm-raw-to-parquet-etl'
        new glue.CfnJob(this, 'SlurmRawToParquet', {
            role: glueETLJobRole.roleName,
            command: {
                name: "glueetl",
                scriptLocation: 's3://' + props.customerBucket.bucketName + '/scripts/slurm-raw-to-parquet.py'
            },
            name: this.slurmParqJobName,
            description: 'Convert raw HPC logs to parquet and remove unneeded fields',
            defaultArguments: {
                "--DATABASE_NAME": props.glueDatabase,
                "--TABLE_NAME": 'o_raw', // can I not hard code this value?
                "--S3_OUTPUT_PATH": 's3://' + props.customerBucket.bucketName + '/processed/hpc/',
                "--REGION": this.region,
                "--job-bookmark-option": "job-bookmark-enable"
            },
            allocatedCapacity: 10,
            maxRetries: 0,
            executionProperty: {
                maxConcurrentRuns: 1
            }
        })

        this.torqueParqJobName = 'torque-raw-to-parquet-etl'
        new glue.CfnJob(this, 'TorqueRawToParquet', {
            role: glueETLJobRole.roleName,
            command: {
                name: "glueetl",
                scriptLocation: 's3://' + props.customerBucket.bucketName + '/scripts/torque-raw-to-parquet.py'
            },
            name: this.torqueParqJobName,
            description: 'Convert raw HPC logs to parquet and remove unneeded fields',
            defaultArguments: {
                "--DATABASE_NAME": props.glueDatabase,
                "--TABLE_NAME": 'o_raw', // can I not hard code this value?
                "--S3_OUTPUT_PATH": 's3://' + props.customerBucket.bucketName + '/processed/hpc/',
                "--REGION": this.region,
                "--job-bookmark-option": "job-bookmark-enable"
            },
            allocatedCapacity: 10,
            maxRetries: 0,
            executionProperty: {
                maxConcurrentRuns: 1
            }
        })

        this.estimateJobName = 'hpc-pricing-estimate-etl'
        new glue.CfnJob(this, 'CalculatePricingEstimate', {
            role: glueETLJobRole.roleName,
            command: {
                name: "glueetl",
                scriptLocation: 's3://' + props.customerBucket.bucketName + '/scripts/pricing-estimate.py'
            },
            name: this.estimateJobName,
            description: 'Calculate job costs based on merging EC2 pricing with HPC Logs based on CPU and Memory',
            defaultArguments: {
                "--DATABASE_NAME": props.glueDatabase,
                "--TABLE_NAME": 'p_hpc', // can I not hard code this value?
                "--PRICING_TABLE_NAME": 'o_pricing',
                "--S3_OUTPUT_PATH": 's3://' + props.customerBucket.bucketName + '/processed/estimate/',
                "--REGION": this.region,
                "--job-bookmark-option": "job-bookmark-enable"
            },
            allocatedCapacity: 10,
            maxRetries: 0,
            executionProperty: {
                maxConcurrentRuns: 1
            }
        })
    }
}