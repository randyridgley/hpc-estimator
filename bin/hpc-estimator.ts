#!/usr/bin/env node
import 'source-map-support/register';
import cdk = require('@aws-cdk/core');
import { HpcEstimatorStack } from '../lib/hpc-estimator-stack';

const app = new cdk.App();
new HpcEstimatorStack(app, 'HpcEstimatorStack');
