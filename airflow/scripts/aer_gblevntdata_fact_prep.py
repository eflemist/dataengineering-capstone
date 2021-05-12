#----------------
# Standard library imports
#----------------

import datetime
import logging
import os
import os.path
import sys

absFilePath = os.path.abspath(__file__)
fileDir = os.path.dirname(os.path.abspath(__file__))
parentDir = os.path.dirname(fileDir)

sys.path.append(parentDir)

#----------------
# Third Party imports
#----------------

#import findspark
#findspark.init()

import pyspark
import pyspark.sql.functions as sqlf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DateType, DecimalType

#---------------
# Local app imports
#--------------

import gblevent_config as gblevent_config

#--------------
# variable setup
#--------------

#url = "jdbc:postgresql://localhost/gbleventdb"

#url = "jdbc:redshift://dwhcluster.comxjkm8stwd.us-east-1.redshift.amazonaws.com:5439/gbleventdb"

url = gblevent_config.JDBC_REDSHIFT

properties = {
    "user": gblevent_config.REDSHIFT_USER,
    "password": gblevent_config.REDSHIFT_PWD
}

evntclschema = StructType() \
    .add("evntcls_key",IntegerType(), True) \
    .add("envtclscd",StringType(),True) \
    .add("envtclsdesc",StringType(),True) \
    .add("evntypcd",StringType(),True) \
    .add("evnttypdesc",StringType(),True) \
    .add("evntsubtypcd",StringType(),True) \
    .add("evntsubtypdesc",StringType(),True) \
    .add("evntcd",StringType(),True) \
    .add("evntdesc",StringType(),True) 

actclschema = StructType() \
    .add("actor_key",IntegerType(), True) \
    .add("actcd",StringType(),True) \
    .add("actdesc",StringType(),True) \
    .add("acttypcd",StringType(),True) \
    .add("acttypdesc",StringType(),True) 

evntlocschema = StructType() \
    .add("evntloc_key",IntegerType(), True) \
    .add("cntrycd",StringType(),True) \
    .add("continent",StringType(),True) \
    .add("region",StringType(),True) \
    .add("cntry",StringType(),True) \

evntcatschema = StructType() \
    .add("evntcat_key",IntegerType(),True) \
    .add("relcd",StringType(),True) \
    .add("reldesc",StringType(),True) \
    .add("relcat",StringType(),True) 
    

dateschema = StructType() \
    .add("datekey",IntegerType(),True) \
    .add("date",DateType(),True) \
    .add("dayofweekname",StringType(),True) \
    .add("dayofweek",IntegerType(),True) \
    .add("dayofmonth",IntegerType(),True) \
    .add("dayofyear",IntegerType(),True) \
    .add("calendarweek",IntegerType(),True) \
    .add("calendarmonthname",StringType(),True) \
    .add("calendarmonth",IntegerType(),True) \
    .add("calendaryear",IntegerType(),True) \
    .add("lastdayinmonth",StringType(),True) 
    
    
gbldatafactschema = StructType() \
    .add("globaleventid",IntegerType(),True) \
    .add("event_date",DateType(),True) \
    .add("actcd",StringType(),True) \
    .add("evntcd",StringType(),True) \
    .add("cntrycd",StringType(),True) \
    .add("relcd",StringType(),True) \
    .add("evntname",StringType(),True) \
    .add("actorname",StringType(),True) \
    .add("goldstein_scale",DecimalType(3,1),True) \
    .add("num_mentions",IntegerType(),True) \
    .add("evnt_cnt",IntegerType(),True) 
    

evntclscols_to_drop = ("envtclscd","envtclsdesc","evntypcd","evnttypdesc","evntsubtypcd","evntsubtypdesc","evntdesc") 
actclscols_to_drop = ("actdesc","acttypcd","acttypdesc") 
evntloccols_to_drop = ("continent","region","cntry") 
evntcatcols_to_drop = ("reldesc","relcat") 
datecols_to_drop =  ("dayofweekname","dayofweek","dayofmonth","dayofyear","calendarweek","calendarmonthname","calendarmonth","calendaryear","lastdayinmonth")   

def main():
    
    spark = SparkSession \
        .builder \
        .appName("Gblevntdata Prod Prep") \
        .config("spark.jars", "/usr/lib/redshift-jdbc42-2.0.0.4.jar")\
        .getOrCreate()
    
    access=gblevent_config.KEY
    secret=gblevent_config.SECRET
    
    sc=spark.sparkContext
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", access)
    hadoop_conf.set("fs.s3a.secret.key", secret)
    hadoop_conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    #hadoop_conf.set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
    hadoop_conf.set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    hadoop_conf.set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
        
        ##.set("spark.driver.extraClassPath", ":".join(jars))
	#.set("spark.hadoop.fs.s3a.access.key", config.get('aws_access_key'))
	#.set("spark.hadoop.fs.s3a.secret.key", config.get('aws_secret_key'))
	#.set("spark.hadoop.fs.s3a.path.style.access", True)
	#.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
	#.set("com.amazonaws.services.s3.enableV4", True)
	#.set("spark.hadoop.fs.s3a.endpoint", f"s3-{config.get('region')}.amazonaws.com")
	##.set("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
        ##.set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
        
    
#    sprk_df = spark.read.format("jdbc")\
#        .option("delimiter", "\t")\
#        .options(url=url, dbtable="evntcat_dim", **properties)\
#        .option("driver", "com.amazon.redshift.jdbc42.Driver")\
#        .load()

    #load df's for dimension tables (event,actor,location,category)
    
    sprk_eventclassdf = spark.read.format("jdbc")\
                        .schema(evntclschema)\
                        .options(url=url, dbtable="gblevent.evntclass_dim", **properties)\
			.option("driver", "com.amazon.redshift.jdbc42.Driver")\
                        .load()
    
    sprk_actorclassdf = spark.read.format("jdbc")\
                        .schema(actclschema)\
                        .options(url=url, dbtable="gblevent.actorclass_dim", **properties)\
			.option("driver", "com.amazon.redshift.jdbc42.Driver")\
                        .load()
    
    sprk_eventlocdf = spark.read.format("jdbc")\
                      .schema(evntlocschema)\
                      .options(url=url, dbtable="gblevent.evntloc_dim", **properties)\
		      .option("driver", "com.amazon.redshift.jdbc42.Driver")\
                      .load()
    
    sprk_eventcatdf = spark.read.format("jdbc")\
                      .schema(evntcatschema)\
                      .options(url=url, dbtable="gblevent.evntcat_dim", **properties)\
 		      .option("driver", "com.amazon.redshift.jdbc42.Driver")\
                      .load()

    sprk_datedf = spark.read.format("jdbc")\
                      .schema(dateschema)\
                      .options(url=url, dbtable="gblevent.date_dim", **properties)\
 		      .option("driver", "com.amazon.redshift.jdbc42.Driver")\
                      .load()
 
    #create df from staging fact table
    sprk_gbldatafctdf = spark.read.format("jdbc")\
                      .schema(gbldatafactschema)\
                      .options(url=url, dbtable="gbleventstg.gbldata_stgfact", **properties)\
  		      .option("driver", "com.amazon.redshift.jdbc42.Driver")\
                      .load()

#    sprk_eventdf.printSchema()    
#    sprk_eventclassdf.printSchema()
#    sprk_actorclassdf.printSchema()
#    sprk_eventlocdf.printSchema()
#    sprk_eventcatdf.printSchema()

    #before doing joins, remove extra columns for df's
    sprk_eventclassdf = sprk_eventclassdf.drop(*evntclscols_to_drop)
    sprk_actorclassdf = sprk_actorclassdf.drop(*actclscols_to_drop)
    sprk_eventlocdf = sprk_eventlocdf.drop(*evntloccols_to_drop)
    sprk_eventcatdf = sprk_eventcatdf.drop(*evntcatcols_to_drop)
    sprk_datedf = sprk_datedf.drop(*datecols_to_drop)

    #join lookup df's to staging df
    sprk_gbldatafctdf = sprk_gbldatafctdf.join(sprk_eventclassdf,["evntcd"]) \
      .join(sprk_actorclassdf,["actcd"]) \
      .join(sprk_eventlocdf,["cntrycd"]) \
      .join(sprk_eventcatdf,["relcd"]) \
      .join(sprk_datedf,sprk_gbldatafctdf.event_date ==  sprk_datedf.date,"inner")     
      
    #reorder columns of fact df to match table 
    sprk_gbldatafctdf = sprk_gbldatafctdf.select("globaleventid", "datekey", "actor_key", 
                                                 "evntcls_key", "evntloc_key", "evntcat_key", 
                                                 "evntname", "actorname", "goldstein_scale", 
                                                 "num_mentions", "evnt_cnt")

#    sprk_stagedf.printSchema()
    #write staging df to file
    #output_data = "/home/emfdellpc/spark/output/capstone/"
    #writing parquet file
    #stagevnt_tbl_file = output_data + "stagevnt_table2.parquet"
    #sprk_stagedf.write.mode('overwrite').partitionBy("year","month").parquet(stagevnt_tbl_file)
    
    #writing to s3 csv file
    sprk_gbldatafctdf.coalesce(1).write\
        .mode('overwrite')\
        .format("csv")\
        .option("header","false")\
        .option("sep","\t")\
        .save("s3://dataeng-capstone-gbldata/gbldata_fact.csv")
    
if __name__ == '__main__':
    main()