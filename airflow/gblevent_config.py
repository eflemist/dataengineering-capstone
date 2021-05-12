import configparser

from datetime import datetime

import sys
import os
import os.path

absFilePath = os.path.abspath(__file__)
fileDir = os.path.dirname(os.path.abspath(__file__))

fileLoc = fileDir + "/gblevent.cfg"

config = configparser.ConfigParser()
config.read_file(open(fileLoc))

KEY                    = config.get("AWS","KEY")
SECRET                 = config.get("AWS","SECRET")

STAGING_FILE           = config.get("FILE_LOCATION","STAGING_FILE")
GBLFACT_FILE           = config.get("FILE_LOCATION","GBLFACT_FILE")

GBLEVENT_STAGING_PREP  = config.get("SPARK_SUBMIT","GBLEVENT_STAGING_PREP")
GBLEVENT_FACT_PREP     = config.get("SPARK_SUBMIT","GBLEVENT_FACT_PREP")

STARTDATE              = config.get("EXECUTION_DATES","STARTDATE")
ENDDATE                = config.get("EXECUTION_DATES","ENDDATE")

JDBC_REDSHIFT          = config.get("REDSHIFT_CONN","JDBC_REDSHIFT")
REDSHIFT_USER          = config.get("REDSHIFT_CONN","REDSHIFT_USER")
REDSHIFT_PWD           = config.get("REDSHIFT_CONN","REDSHIFT_PWD")