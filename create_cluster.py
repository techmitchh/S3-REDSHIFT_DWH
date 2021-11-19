import numpy as np
import pandas as pd
import boto3
import configparser

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# REDSHIFT PARAMETERS
KEY                    = config.get('AWS', 'KEY')
SECRET                 = config.get('AWS', 'SECRET')
DB_ROLE_NAME           = config.get('IAM_ROLE','DB_IAM_ROLE_NAME')
HOST                   = config.get('CLUSTER', 'HOST')
DB_PORT                = config.get('CLUSTER', 'DB_PORT')
DB_CLUSTER_TYPE        = config.get('CLUSTER', 'DB_CLUSTER_TYPE')
DB_NODE_TYPE           = config.get('CLUSTER', 'DB_NODE_TYPE')
DB_NAME                = config.get('CLUSTER', 'DB_NAME')
DB_CLUSTER_IDENTIFIER  = config.get('CLUSTER', 'DB_CLUSTER_IDENTIFIER')
DB_USER                = config.get('CLUSTER', 'DB_USER')
DB_PASSWORD            = config.get('CLUSTER', 'DB_PASSWORD')

pd.DataFrame({"Param": 
                ["DB_ROLE_NAME", "DB_CLUSTER_TYPE", "DB_NODE_TYPE", "DB_NAME", "DB_CLUSTER_IDENTIFIER", "DB_USER", "DB_PASSWORD"],
              "Value":
                [DB_ROLE_NAME, DB_CLUSTER_TYPE, DB_NODE_TYPE, DB_NAME, DB_CLUSTER_IDENTIFIER, DB_USER, DB_PASSWORD]
            })


# ESTABLISH RESOURCES TO CREATE AND CONNECT TO REDSHIFT CLUSTER
s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )
iam = boto3.client('iam',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )    
redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                           )

# GET IAM ROLE 
roleArn = iam.get_role(RoleName=DB_ROLE_NAME)['Role']['Arn']

# CREATE CLUSTER
response = redshift.create_cluster(
    # ADD PARAMETER FOR HARDWARE
    ClusterType=DB_CLUSTER_TYPE,
    NodeType=DB_NODE_TYPE,

    # ADD PARAMETERS FOR IDENTIFIERS & CREDENTIALS
    DBName=DB_NAME, 
    ClusterIdentifier=DB_CLUSTER_IDENTIFIER,
    MasterUsername=DB_USER,
    MasterUserPassword=DB_PASSWORD, 

    # ADD PARAMETER FOR ROLE (TO ALLOW S3 ACCESS)
    IamRoles=[roleArn]
)

# CHECK REDSHIFT CLUSTER STATUS
def clusterProperties(props):
    pd.set_option('display.max_colwidth', 1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "VpcId"]
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

myClusterprops = redshift.describe_clusters(ClusterIdentifier=DB_CLUSTER_IDENTIFIER)['Clusters'][0]
print(clusterProperties(myClusterprops))
