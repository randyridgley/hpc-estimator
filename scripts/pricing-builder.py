import sys
import boto3
import pprint
import pandas as pd
import re
import json
from awsglue.utils import getResolvedOptions

glue_client = boto3.client("glue")
args = getResolvedOptions(sys.argv, [
                          'WORKFLOW_NAME', 'WORKFLOW_RUN_ID',
                          'S3_OUTPUT_BUCKET', 'S3_OUTPUT_KEY', 'REGION'])
workflow_name = args['WORKFLOW_NAME']
workflow_run_id = args['WORKFLOW_RUN_ID']

# if workflow_name:
#     workflow_params = glue_client.get_workflow_run_properties(
#                                         Name=workflow_name,
#                                         RunId=workflow_run_id
#     )["RunProperties"]
#     # target_database = workflow_params['target_database']
#     # target_s3_location = workflow_params['target_s3_location']

region = args['REGION']
availability_zones = []
product_descriptions = ['Linux/UNIX (Amazon VPC)']
numbers = re.compile('\d+(?:\.\d+)?')

pricing = boto3.client('pricing', region_name='us-east-1')
ec2 = boto3.client('ec2', region_name=region)

region_response = ec2.describe_availability_zones()
az_list = sorted([x['ZoneName'] for x in region_response['AvailabilityZones']])

filters = []

if availability_zones is not None:
    filters.append({'Name': 'availability-zone',
                    'Values': az_list})

region_map = {
    'us-east-1': 'US East (N. Virginia)',
    'ap-south-1': 'Asia Pacific (Mumbai)',
    'us-east-2': 'US East (Ohio)',
    'us-west-2': 'US West (Oregon)',
    'us-gov-east-1': 'AWS GovCloud (US-East)',
    'ap-east-1': 'Asia Pacific (Hong Kong)',
    'ap-northeast-1': 'Asia Pacific (Tokyo)',
    'eu-north-1': 'EU (Stockholm)',
    'ap-southeast-1': 'Asia Pacific (Singapore)',
    'ap-northeast-3': 'Asia Pacific (Osaka-Local)',
    'eu-west-2': 'EU (London)',
    'sa-east-1': 'South America (Sao Paulo)',
    'ap-southeast-2': 'Asia Pacific (Sydney)',
    'eu-west-1': 'EU (Ireland)',
    'eu-central-1': 'EU (Frankfurt)',
    'eu-west-3': 'EU (Paris)',
    'ca-central-1': 'Canada (Central)',
    'ap-northeast-2': 'Asia Pacific (Seoul)',
    'us-gov-west-1': 'AWS GovCloud (US)',
    'us-west-1': 'US West (N. California)',
}


# if the instance type is an a or n type or metal skip for now
def is_exotic_type(inst_type):
    instance_types = ['c', 'm', 'r', 'p']
    inst = inst_type.split('.')
    return inst[0].endswith('a') \
        or inst[0].endswith('n') \
        or inst[1].endswith('metal') \
        or inst_type[0] not in instance_types

d = []
next_token = ""

while next_token is not None:
    response = pricing.get_products(
        ServiceCode='AmazonEC2',
        Filters=[
            {'Type': 'TERM_MATCH', 'Field': 'operatingSystem',
                'Value': 'Linux'},
            {'Type': 'TERM_MATCH', 'Field': 'location',
                'Value': region_map[region]},
            {'Type': 'TERM_MATCH', 'Field': 'preInstalledSw',
                'Value': 'NA'},
            {'Type': 'TERM_MATCH', 'Field': 'tenancy',
                'Value': 'Shared'},
            {'Type': 'TERM_MATCH', 'Field': 'capacityStatus',
                'Value': 'Used'},
            {'Type': 'TERM_MATCH', 'Field': 'currentGeneration',
                'Value': 'yes'},
        ],
        MaxResults=100,
        NextToken=next_token
    )

    for product in response['PriceList']:
        pp = json.loads(product)
        qq = json.loads(json.dumps(pp['product']['attributes']))
        rr = json.loads(json.dumps(pp['terms']['OnDemand']))
        ss = rr[list(rr.keys())[0]]['priceDimensions']
        tt = ss[list(ss.keys())[0]]['pricePerUnit']['USD']

        spot_prices = ec2.describe_spot_price_history(
            Filters=filters,
            InstanceTypes=[qq['instanceType']],
            ProductDescriptions=product_descriptions,
            MaxResults=len(region_response['AvailabilityZones'])
        )

        lowest = 99
        discount = 0

        for price in spot_prices['SpotPriceHistory']:
            if float(price['SpotPrice']) < lowest:
                lowest = float(price['SpotPrice'])

        if lowest != 99:
            discount = int(((float(tt) - float(lowest)) / float(tt)) * 100.00)
        else:
            lowest = 0

        if not is_exotic_type(qq['instanceType'].lower()):
            gpu = 0

            if 'gpu' in qq:
                gpu = int(qq['gpu'])

            d.append({
                'instanceType': qq['instanceType'],
                'vCPU': qq['vcpu'],
                'memory': '{0}'.format(int(round(float(numbers.findall(qq['memory'].replace(",", ""))[0])))),
                'onDemandPrice': '{:02.5f}'.format(float(tt)),
                'spotPrice': '{:02.5f}'.format(float(lowest)),
                'discount': discount,
                'gpu': gpu
            })

    if "NextToken" not in response:
        break
    next_token = response['NextToken']

df = pd.DataFrame(d)
csv_file = '{0}-pricing.csv'.format(region)
df.to_csv(csv_file, mode='w', index=False)

s3 = boto3.resource('s3', region_name=region)
s3.meta.client.upload_file(
    csv_file, args['S3_OUTPUT_BUCKET'], args['S3_OUTPUT_KEY'])
