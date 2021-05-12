import boto3
import json
import gblevntdata_rshift_config as db_config

iam = boto3.client('iam',
                     region_name='us-east-1',
                     aws_access_key_id=db_config.KEY,
                     aws_secret_access_key=db_config.SECRET
                    )


# Create the IAM role
try:
    print("1.1 Creating a new IAM Role") 
    dwhRole = iam.create_role(
        Path='/',
        RoleName=db_config.DWH_IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps({"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":["redshift.amazonaws.com"]},"Action":["sts:AssumeRole"]}]})
    )
   
except Exception as e:
    print(e)

# Attach Policy
print("1.2 Attaching Policy")

iam.attach_role_policy(RoleName=db_config.DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']


# Get and print the IAM role ARN
print("1.3 Get the IAM role ARN")
roleArn = iam.get_role(RoleName=db_config.DWH_IAM_ROLE_NAME)['Role']['Arn']

print(roleArn)



    