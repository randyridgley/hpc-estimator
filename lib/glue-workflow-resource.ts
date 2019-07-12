import cfn = require('@aws-cdk/aws-cloudformation');
import lambda = require('@aws-cdk/aws-lambda');
import cdk = require('@aws-cdk/core');
import iam = require('@aws-cdk/aws-iam');

import path = require('path')

export interface GlueWorkflowResourceProps {
    rawCrawler: string;
    parqCrawler: string;
    pricingCrawler: string;
    estimateCrawler: string;
    pricingJob: string;
    parqJob: string;
    estimateJob: string;
    customerName:string;
    hpcName:string;
}

export class GlueWorkflowResource extends cdk.Construct {
    public readonly response: string;

    constructor(scope: cdk.Construct, id: string, props: GlueWorkflowResourceProps) {
        super(scope, id);

        const lambdaFunction = new lambda.SingletonFunction(this, 'Singleton', {
            uuid: 'f7d4f730-4ee1-11e8-9c2d-fa7ae01bbebc',
            code: lambda.Code.asset('lambda/workflow-lambda'),                
            handler: 'index.main',
            runtime: lambda.Runtime.PYTHON_3_6,
        })
        
        lambdaFunction.addToRolePolicy(new iam.PolicyStatement({
            actions: ['glue:*'],
            resources: ['*'],
        }));

        const resource = new cfn.CustomResource(this, 'Resource', {
            provider: cfn.CustomResourceProvider.lambda(lambdaFunction),
            properties: props
        });

        this.response = resource.getAtt('Response').toString();
    }
}
