import cdk = require('@aws-cdk/core');
import s3 = require('@aws-cdk/aws-s3');
import s3deploy = require('@aws-cdk/aws-s3-deployment');
import glue = require('@aws-cdk/aws-glue');

export class HpcEstimatorStack extends cdk.Stack {
  public readonly customerBucket: s3.Bucket
  public readonly glueDatabase: glue.Database

  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const customerName = this.node.tryGetContext("customerName");

    this.customerBucket = new s3.Bucket(this, 'CustomerHPCLogBucket', {

    });

    new s3deploy.BucketDeployment(this, 'DeployGlueData', {
      source: s3deploy.Source.asset('./scripts'),
      destinationBucket: this.customerBucket,
      destinationKeyPrefix: 'scripts',
      retainOnDelete: false
    });

    const glueDatabaseName = customerName + '-db'
    this.glueDatabase = new glue.Database(this, 'GlueDatabase', {
      databaseName: glueDatabaseName
    });
  }
}