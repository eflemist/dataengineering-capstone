#----------------
# Standard library imports
#----------------

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

import pyspark
import pyspark.sql.functions as sqlf

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType

#---------------
# Local app imports
#--------------

import gblevent_config as gblevent_config

#--------------
# variable setup
#--------------
evntschema = StructType() \
    .add("globaleventid",StringType(),True) \
    .add("day",StringType(),True) \
    .add("month",StringType(),True) \
    .add("year",StringType(),True) \
    .add("fractiondate",StringType(),True) \
    .add("actor1code",StringType(),True) \
    .add("actorname",StringType(),True) \
    .add("cntrycd",StringType(),True) \
    .add("actor1knowngroupcode",StringType(),True) \
    .add("actor1ethniccode",StringType(),True) \
    .add("relcd",StringType(),True) \
    .add("actor1religion2code",StringType(),True) \
    .add("actcd",StringType(),True) \
    .add("actor1type2code",StringType(),True) \
    .add("actor1type3code",StringType(),True) \
    .add("actor2code",StringType(),True) \
    .add("actor2name",StringType(),True) \
    .add("actor2countrycode",StringType(),True) \
    .add("actor2knowngroupcode",StringType(),True) \
    .add("actor2ethniccode",StringType(),True) \
    .add("actor2religion1code",StringType(),True) \
    .add("actor2religion2code",StringType(),True) \
    .add("actor2type1code",StringType(),True) \
    .add("actor2type2code",StringType(),True) \
    .add("actor2type3code",StringType(),True) \
    .add("isrootevent",StringType(),True) \
    .add("evntcd",StringType(),True) \
    .add("eventbasecode",StringType(),True) \
    .add("eventrootcode",StringType(),True) \
    .add("quadclass",StringType(),True) \
    .add("goldsteinscale",StringType(),True) \
    .add("nummentions",StringType(),True) \
    .add("numsources",StringType(),True) \
    .add("numarticles",StringType(),True)

newsschema = StructType() \
    .add("globaleventid",StringType(),True) \
    .add("tbrmved1",StringType(),True) \
    .add("tbrmved2",StringType(),True) \
    .add("tbrmved3",StringType(),True) \
    .add("newsource",StringType(),True) \
    .add("evntname",StringType(),True) 

newscols_to_drop = ("tbrmved1","tbrmved1","tbrmved1")

evntclschema = StructType() \
    .add("envtclscd",StringType(),True) \
    .add("envtclsdesc",StringType(),True) \
    .add("evntypcd",StringType(),True) \
    .add("evnttypdesc",StringType(),True) \
    .add("evntsubtypcd",StringType(),True) \
    .add("evntsubtypdesc",StringType(),True) \
    .add("evntcd",StringType(),True) \
    .add("evntdesc",StringType(),True)

actclschema = StructType() \
    .add("actcd",StringType(),True) \
    .add("actdesc",StringType(),True) \
    .add("acttypcd",StringType(),True) \
    .add("acttypdesc",StringType(),True) 

evntlocschema = StructType() \
    .add("cntrycd",StringType(),True) \
    .add("continent",StringType(),True) \
    .add("region",StringType(),True) \
    .add("cntry",StringType(),True) 

evntcatschema = StructType() \
    .add("relcd",StringType(),True) \
    .add("reldesc",StringType(),True) \
    .add("relcat",StringType(),True) 


staging_columns = ["globaleventid","day","month","year","evntname","evntcd","actcd",
                  "actorname","newsource","cntrycd","relcd",
                  "goldsteinscale","nummentions"]


def rename_df_columns(colname1, colname2, dataset):
    for x, y in zip(colname1, colname2):
        dataset = dataset.withColumnRenamed(x,y)
        
    return dataset
    
def get_s3_key_value(folder):
    
    yr_val  = str(sys.argv[1])
    mth_val = str(sys.argv[2])
    dy_val  = str(sys.argv[3])
    
    if len(mth_val) == 1:
        mth_val = '0' + mth_val
        
    if len(dy_val) == 1:
        dy_val = '0' + dy_val
        
    s3_key_value =  's3://dataeng-capstone/' + folder + '/' + yr_val + mth_val + dy_val    

    return (s3_key_value)
    
def main():
    
    spark = SparkSession \
        .builder \
        .appName("Gblevntdata Staging Prep") \
        .getOrCreate()

    #allow spark to interact with s3
    
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
    
    #load df's for events and newsarticle listing from s3

    s3_key_value_part = get_s3_key_value('events')
    
    event_extension = '??????.export.csv'
        
    s3_event_key_value = s3_key_value_part + event_extension
        
    sprk_eventdf = spark.read.format("csv")\
                   .option("delimiter", "\t")\
                   .schema(evntschema)\
                   .load(s3_event_key_value)
                   #.load("s3a://dataeng-capstone/events/20150219044500.export.csv") 
    
    s3_key_value_part = get_s3_key_value('articles')
    
    article_extension = '??????.mentions.csv'
    
    s3_article_key_value = s3_key_value_part + article_extension    
    
    sprk_newsdf = spark.read.format("csv")\
                  .option("delimiter", "\t")\
                  .schema(newsschema)\
                  .load(s3_article_key_value)
                  #.load("s3a://dataeng-capstone/articles/20150219044500.mentions.csv")

    #load df's for lookup details (event,actor,location,category) from s3
    
    sprk_eventclassdf = spark.read.format("json")\
                        .schema(evntclschema)\
                        .load("s3://dataeng-capstone/dimensions/events.json") 
    
    sprk_actorclassdf = spark.read.format("json")\
                        .schema(actclschema)\
                        .load("s3://dataeng-capstone/dimensions/actorclass.json") 
    
    sprk_eventlocdf = spark.read.format("json")\
                      .schema(evntlocschema)\
                      .load("s3://dataeng-capstone/dimensions/eventloc.json") 
    
    sprk_eventcatdf = spark.read.format("json")\
                      .schema(evntcatschema)\
                      .load("s3://dataeng-capstone/dimensions/eventcat.json") 

    #rename columns in events df
#    sprk_eventdf.printSchema()    
#    sprk_eventclassdf.printSchema()
#    sprk_actorclassdf.printSchema()
#    sprk_eventlocdf.printSchema()
#    sprk_eventcatdf.printSchema()

    #set value in month col to mm only on events df
    sprk_eventdf = sprk_eventdf.withColumn('month', sqlf.substring('month', 5,2))

    #clean event df to relfect details for only one actorcountrycode 
    sprk_eventdf = sprk_eventdf.withColumn("cntrycd", sqlf.when(sprk_eventdf.cntrycd.isNull(), sprk_eventdf.actor2countrycode) \
                          .otherwise(sprk_eventdf.cntrycd))
    
    #remove any rows where cntrycd is null
    sprk_eventdf = sprk_eventdf.na.drop(subset=["cntrycd"])

    #clean event df to relfect details for only one actorname
    sprk_eventdf = sprk_eventdf.withColumn("actorname", sqlf.when(sprk_eventdf.actorname.isNull(), sprk_eventdf.actor2name) \
                          .otherwise(sprk_eventdf.actorname))

    #clean event df to relfect details for only one actortypecode
    sprk_eventdf = sprk_eventdf.withColumn("actcd", sqlf.when(sprk_eventdf.actcd.isNull(), sprk_eventdf.actor2type1code) \
                          .otherwise(sprk_eventdf.actcd))

    #clean event df to relfect details for only one actorcategory/religion code
    sprk_eventdf = sprk_eventdf.withColumn("relcd", sqlf.when(sprk_eventdf.relcd.isNull(), sprk_eventdf.actor2religion1code) \
                          .otherwise(sprk_eventdf.relcd))


    #remove cols not needed in news article listing df
    sprk_newsdf = sprk_newsdf.drop(*newscols_to_drop)

    #join news article listing df to events df 
    sprk_eventdf = sprk_eventdf.join(sprk_newsdf,["globaleventid"])
    
#    sprk_eventdf.printSchema()
    
    #create staging df 
    sprk_stagedf = sprk_eventdf.select(staging_columns)

    #update all null values in staging df to UNKNWN
    sprk_stagedf = sprk_stagedf.fillna("UNKWN")

    #join lookup df's to staging df
    sprk_stagedf = sprk_stagedf.join(sprk_eventclassdf,["evntcd"]) \
      .join(sprk_actorclassdf,["actcd"]) \
      .join(sprk_eventlocdf,["cntrycd"]) \
      .join(sprk_eventcatdf,["relcd"])

#    sprk_stagedf.printSchema()
    #write staging df to file
    #output_data = "/home/emfdellpc/spark/output/capstone/"
    #writing parquet file
    #stagevnt_tbl_file = output_data + "stagevnt_table2.parquet"
    #sprk_stagedf.write.mode('overwrite').partitionBy("year","month").parquet(stagevnt_tbl_file)
    
    #writing csv file to s3
    stagevnt_tbl_file = gblevent_config.STAGING_FILE
    sprk_stagedf.coalesce(1).write\
        .mode('append')\
        .format("csv")\
        .option("header","false")\
        .option("sep","\t")\
        .save(stagevnt_tbl_file)
    
if __name__ == '__main__':
    main()
 

