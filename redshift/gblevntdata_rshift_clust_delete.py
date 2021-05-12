import boto3
import gblevntdata_rshift_config as db_config

iam_resource = boto3.client('iam',
                             region_name='us-east-1',
                             aws_access_key_id=db_config.KEY,
                             aws_secret_access_key=db_config.SECRET
                            )

redshift_resource = boto3.client('redshift',
                                   region_name="us-east-1",
                                   aws_access_key_id=db_config.KEY,
                                   aws_secret_access_key=db_config.SECRET
                       )



#-- Uncomment & run to delete the created resources
redshift_resource.delete_cluster( ClusterIdentifier=db_config.DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)


#-- Uncomment & run to delete the created resources
iam_resource.detach_role_policy(RoleName=db_config.DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
#iam.delete_role(RoleName=ASSMP1_IAM_ROLE_NAME)
#### CAREFUL!!