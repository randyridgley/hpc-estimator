# Useful commands

 * `npm run build`   compile typescript to js
 * `npm run watch`   watch for changes and compile
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk synth`       emits the synthesized CloudFormation template


to deploy it requires a few context variables for now but will be modified shortly. Currently only supports Torque logs but working on including SGE and Slurm.

* `cdk deploy -c customerName={{test}} -c customerLogBucket={{bucket_name}} -c customerLogKey={{key to raw logs}}  GlueJobStack`


``` bash
Slurm account command to get history
sacct -S2019-01-01-00:01 -E2019-03-31-23:59 –parsable2 --format=jobid,elapsed,ncpus,state,user,partition,elapsedRAW > slurm-acct.txt
```
