import cdk = require('@aws-cdk/core');
import s3 = require('@aws-cdk/aws-s3');
import glue = require('@aws-cdk/aws-glue');
import iam = require('@aws-cdk/aws-iam');

interface PricingStackProps extends cdk.StackProps {
    customerBucket: s3.Bucket;
    glueDatabase: string;
}

export class PricingStack extends cdk.Stack {
    public readonly pricingCrawlerName: string
    public readonly pricingJobName: string 

    constructor(scope: cdk.Construct, id: string, props: PricingStackProps) {
        super(scope, id, props);

        const customerName = this.node.tryGetContext("customerName");

        const gluePricingJobRole = new iam.Role(this, 'GluePricingJobRole', {
            roleName: 'GluePricingJobServiceRole',
            assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
                iam.ManagedPolicy.fromAwsManagedPolicyName('AWSPriceListServiceFullAccess')                
            ]
        });

        const azStatement = new iam.PolicyStatement({
            actions: ['ec2:DescribeAvailabilityZones', 'ec2:DescribeSpotPriceHistory', 's3:PutObject'],
            resources: ['*']
        });

        const azPolicy = new iam.Policy(this, "DescribeAZsPolicy", {
            statements: [azStatement]
        });

        gluePricingJobRole.attachInlinePolicy(azPolicy)

        props.customerBucket.grantPut(gluePricingJobRole);
        props.customerBucket.grantRead(gluePricingJobRole);

        const glueETLJobRole = new iam.Role(this, 'GlueETLJobRole', {
            roleName: 'GlueETLJobServiceRole',
            assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
                iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess')
            ]
        });
    
        props.customerBucket.grantPut(glueETLJobRole);
        props.customerBucket.grantRead(glueETLJobRole);

        const glueCrawlerRole = new iam.Role(this, 'GlueCrawlerRole', {
            roleName: 'GlueCrawlerServiceRole',
            assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
                iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess')
            ]
        });

        props.customerBucket.grantRead(glueCrawlerRole);

        this.pricingCrawlerName = customerName + '-pricing-crawler'
        new glue.CfnCrawler(this, 'AWSPricingCrawler', {
            databaseName: props.glueDatabase,
            name: this.pricingCrawlerName,
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

        this.pricingJobName = customerName + '-pricing-builder'
        new glue.CfnJob(this, 'AWSEC2PricingGenerator', {
            role: gluePricingJobRole.roleName,
            command: {
                name: "pythonshell",
                scriptLocation: 's3://' + props.customerBucket.bucketName + '/scripts/pricing-builder.py',
            },
            name: this.pricingJobName,
            description: 'AWS Pricing for OnDemand and Spot instances for HPC jobs',
            defaultArguments: {
                "--S3_OUTPUT_BUCKET": props.customerBucket.bucketName,
                "--S3_OUTPUT_KEY": 'raw/pricing/' + this.region + '-pricing.csv',
                "--REGION": this.region
            },
            maxRetries: 0,
            executionProperty: {
                maxConcurrentRuns: 1
            }
        })
    }
}