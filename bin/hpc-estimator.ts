#!/usr/bin/env node
import 'source-map-support/register';
import cdk = require('@aws-cdk/core');
import { SetupStack } from '../lib/setup-stack';
import { PricingStack } from '../lib/pricing-stack';
import { HPCServiceStack } from '../lib/hpc-service-stack';
import { EstimateStack } from '../lib/estimate-stack';

const app = new cdk.App();
const setupStack = new SetupStack(app, 'SetupStack');

const pricingStack = new PricingStack(app, "PricingStack", {
    customerBucket: setupStack.customerBucket,
    glueDatabase: setupStack.glueDatabase.databaseName
})
pricingStack.addDependency(setupStack)

const estimateStack = new EstimateStack(app, "EstimateStack", {
    customerBucket: setupStack.customerBucket,
    glueDatabase: setupStack.glueDatabase.databaseName
})

estimateStack.addDependency(pricingStack)

new HPCServiceStack(app, "HPCServiceStack", {
    customerBucket: setupStack.customerBucket,
    glueDatabase: setupStack.glueDatabase.databaseName,
    parqCrawler: estimateStack.parqCrawlerName,
    estimateCrawler: estimateStack.estimateCrawlerName,
    pricingJob: pricingStack.pricingJobName,
    slurmParqJob: estimateStack.slurmParqJobName,
    sgeParqJob: estimateStack.sgeParqJobName,
    torqueParqJob: estimateStack.torqueParqJobName,
    estimateJob: estimateStack.estimateJobName,
}).addDependency(estimateStack)