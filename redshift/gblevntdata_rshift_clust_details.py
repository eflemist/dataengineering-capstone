import boto3
import configparser
import json
import pandas as pd
import gblevntdata_rshift_config as db_config


redshift = boto3.client('redshift',
                       region_name="us-east-1",
                       aws_access_key_id=db_config.KEY,
                       aws_secret_access_key=db_config.SECRET
                       )

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


print(db_config.DWH_CLUSTER_IDENTIFIER)
myClusterProps = redshift.describe_clusters(ClusterIdentifier=db_config.DWH_CLUSTER_IDENTIFIER)
print(myClusterProps)
print(prettyRedshiftProps(myClusterProps))

#ASSMP1_ENDPOINT = myClusterProps['Endpoint']['Address']
#ASSMP1_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
#print("ASSMP1_ENDPOINT :: ", ASSMP1_ENDPOINT)
#print("DASSMP1_ROLE_ARN :: ", ASSMP1_ROLE_ARN)



