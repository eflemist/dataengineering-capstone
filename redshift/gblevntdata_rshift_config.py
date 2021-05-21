import configparser

config = configparser.ConfigParser()
config.read_file(open('rshift_dwh.cfg'))

KEY                    = config.get("AWS","KEY")
SECRET                 = config.get("AWS","SECRET")

DWH_CLUSTER_TYPE       = config.get("CLUSTER","CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("CLUSTER","NUM_NODES")
DWH_NODE_TYPE          = config.get("CLUSTER","NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","CLUSTER_IDENTIFIER")
DWH_HOST               = config.get("CLUSTER","HOST")
DWH_DB                 = config.get("CLUSTER","DB_NAME")
DWH_DB_USER            = config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DWH_PORT               = config.get("CLUSTER","DB_PORT")

DWH_IAM_ROLE_NAME      = config.get("IAM_ROLE", "IAM_ROLE_NAME")
DWH_ARN                = config.get("IAM_ROLE", "ARN")

