import time
import boto3
import gblevntdata_rshift_config as db_config

ec2_resource = boto3.resource('ec2',
                               region_name="us-east-1",
                               aws_access_key_id=db_config.KEY,
                               aws_secret_access_key=db_config.SECRET
                            )

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

#roleArn = db_config.DWH_ARN
roleArn = iam_resource.get_role(RoleName=db_config.DWH_IAM_ROLE_NAME)['Role']['Arn']

try:
    response = redshift_resource.create_cluster(        
        #HW
        ClusterType=db_config.DWH_CLUSTER_TYPE,
        NodeType=db_config.DWH_NODE_TYPE,
        NumberOfNodes=int(db_config.DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=db_config.DWH_DB,
        ClusterIdentifier=db_config.DWH_CLUSTER_IDENTIFIER,
        MasterUsername=db_config.DWH_DB_USER,
        MasterUserPassword=db_config.DWH_DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[roleArn]  
    )
except Exception as e:
    print(e)

cluster_status = response['Cluster']['ClusterStatus']
if cluster_status == 'creating':
    creating_cluster = True
    while creating_cluster:
        response = redshift_resource.describe_clusters(ClusterIdentifier=db_config.DWH_CLUSTER_IDENTIFIER)
        cluster_status = response['Clusters'][0]['ClusterStatus']
        if cluster_status == 'available':
            creating_cluster = False
            print(cluster_status)
        else:
            print("Waiting while cluster is... ",cluster_status)
            time.sleep(60)
    
#Get cluster properties
myClusterProps = redshift_resource.describe_clusters(ClusterIdentifier=db_config.DWH_CLUSTER_IDENTIFIER)

#Open an incoming TCP port to access the cluster endpoint

try:
    secgrp_id = myClusterProps['Clusters'][0]['VpcSecurityGroups'][0]['VpcSecurityGroupId']
    #print('secgrp_id is: ', secgrp_id)
    secgrp_value = 'ec2.SecurityGroup(id=' + "'" + secgrp_id + "')"
    #print('secgrp_value is: ', secgrp_value)
    vpc_id = myClusterProps['Clusters'][0]['VpcId']
    vpc = ec2_resource.Vpc(id=vpc_id)
    #print('vpc is: ', vpc)
    secgrp_list = list(vpc.security_groups.all())
    #print(secgrp_list)
    for secgrp_item in secgrp_list:
        if str(secgrp_item) == secgrp_value:
            defaultSg_resource = secgrp_item
    
    #vpc = ec2_resource.Vpc(id=myClusterProps['Clusters'][0]['VpcId'])
    #defaultSg_id = myClusterProps['Clusters'][0]['VpcSecurityGroups'][0]['VpcSecurityGroupId']
    #defaultSg_resource = vpc.security_groups.filter(GroupIds=[defaultSg_id])
    #defaultSg = list(vpc.security_groups.all())[0]
    #list(vpc.security_groups.all())[0]
    print(defaultSg_resource)
    defaultSg_resource.authorize_ingress(
        GroupName=defaultSg_resource.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(db_config.DWH_PORT),
        ToPort=int(db_config.DWH_PORT)
    )
except Exception as e:
    print(e)



    