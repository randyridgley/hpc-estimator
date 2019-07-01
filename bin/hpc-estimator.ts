#!/usr/bin/env node
import 'source-map-support/register';
import cdk = require('@aws-cdk/core');
import { HpcEstimatorStack } from '../lib/hpc-estimator-stack';
import { GlueJobStack } from '../lib/glue-job-stack';

const app = new cdk.App();
const hpcStack = new HpcEstimatorStack(app, 'SetupStack');

new GlueJobStack(app, "GlueJobStack", {
    customerBucket: hpcStack.customerBucket
}).addDependency(hpcStack)
