import cdk = require('@aws-cdk/core');
import s3 = require('@aws-cdk/aws-s3');
import lambda = require('@aws-cdk/aws-lambda');
import apigateway = require('@aws-cdk/aws-apigateway');
import iam = require('@aws-cdk/aws-iam');
import { Duration } from '@aws-cdk/core';

interface HPCServiceStackProps extends cdk.StackProps {
    customerBucket: s3.Bucket;
    glueDatabase: string;
    parqCrawler: string;
    pricingJob: string;
    estimateJob: string;
    slurmParqJob: string;
    sgeParqJob: string;
    torqueParqJob: string;
}

export class HPCServiceStack extends cdk.Stack {
    public readonly pricingCrawlerName: string
    public readonly pricingJobName: string

    constructor(scope: cdk.Construct, id: string, props: HPCServiceStackProps) {
        super(scope, id, props);

        const createWorkflowLambda = new lambda.Function(this, 'workflowFunction', {
            code: new lambda.AssetCode('lambda/workflow'),
            handler: 'index.handler',
            runtime: lambda.Runtime.PYTHON_3_7,
            timeout: Duration.minutes(1),
            environment: {
                PARQ_CRAWLER: props.parqCrawler,
                PRICING_JOB: props.pricingJob,
                SLURM_PARQ_JOB: props.slurmParqJob,
                SGE_PARQ_JOB: props.sgeParqJob,
                TORQUE_PARQ_JOB: props.torqueParqJob,
                ESTIMATE_JOB: props.estimateJob,
                WORKFLOW_BUCKET: props.customerBucket.bucketName,
                GLUE_DATABASE_NAME: props.glueDatabase
            }
        });

        const getGlueCredentialsPolicy = new iam.PolicyStatement();
        getGlueCredentialsPolicy.addResources("*");
        getGlueCredentialsPolicy.addActions('glue:*');

        createWorkflowLambda.addToRolePolicy(getGlueCredentialsPolicy);

        const api = new apigateway.RestApi(this, 'hpcEstimateApi', {
            restApiName: 'HPC Estimation Service'
        });

        const items = api.root.addResource('estimate');
        const createWorkflowIntegration = new apigateway.LambdaIntegration(createWorkflowLambda);
        items.addMethod('POST', createWorkflowIntegration);
        items.addMethod('PUT', createWorkflowIntegration);
        items.addMethod('GET', createWorkflowIntegration);
    }
}

export function addCorsOptions(apiResource: apigateway.IResource) {
    apiResource.addMethod('OPTIONS', new apigateway.MockIntegration({
        integrationResponses: [{
            statusCode: '200',
            responseParameters: {
                'method.response.header.Access-Control-Allow-Headers': "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent'",
                'method.response.header.Access-Control-Allow-Origin': "'*'",
                'method.response.header.Access-Control-Allow-Credentials': "'false'",
                'method.response.header.Access-Control-Allow-Methods': "'OPTIONS,GET,PUT,POST,DELETE'",
            },
        }],
        passthroughBehavior: apigateway.PassthroughBehavior.NEVER,
        requestTemplates: {
            "application/json": "{\"statusCode\": 200}"
        },
    }), {
        methodResponses: [{
            statusCode: '200',
            responseParameters: {
                'method.response.header.Access-Control-Allow-Headers': true,
                'method.response.header.Access-Control-Allow-Methods': true,
                'method.response.header.Access-Control-Allow-Credentials': true,
                'method.response.header.Access-Control-Allow-Origin': true,
            },
        }]
    })
}