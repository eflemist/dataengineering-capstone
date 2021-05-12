import gblevntdata_rshift_config as db_config
import boto3

iam_resource = boto3.client('iam',
                             region_name='us-east-1',                            
                             aws_access_key_id=db_config.KEY,
                             aws_secret_access_key=db_config.SECRET
                            )

roleArn = iam_resource.get_role(RoleName=db_config.DWH_IAM_ROLE_NAME)['Role']['Arn']

# DROP STAGING TABLES

gbldata_staging_table_drop = "DROP TABLE IF EXISTS gbleventstg.gbldata_staging"
gbldata_stgfact_table_drop = "DROP TABLE IF EXISTS gbleventstg.gbldata_stgfact"
actorclass_stgdim_table_drop = "DROP TABLE IF EXISTS gbleventstg.actorclass_stgdim"
evntclass_stgdim_table_drop = "DROP TABLE IF EXISTS gbleventstg.evntclass_stgdim"
evntloc_stgdim_table_drop = "DROP TABLE IF EXISTS gbleventstg.eventloc_stgdim"
evntcat_stgdim_table_drop = "DROP TABLE IF EXISTS gbleventstg.eventcat_stgdim"


# DROP FINAL TABLES
gbldata_fact_table_drop = "DROP TABLE IF EXISTS gblevent.gbldata_fact"
actorclass_dim_table_drop = "DROP TABLE IF EXISTS gblevent.actorclass_dim"
evntclass_dim_table_drop = "DROP TABLE IF EXISTS gblevent.evntclass_dim"
evntloc_dim_table_drop = "DROP TABLE IF EXISTS gblevent.eventloc_dim"
evntcat_dim_table_drop = "DROP TABLE IF EXISTS gblevent.eventcat_dim"
date_dim_table_drop = "DROP TABLE IF EXISTS gblevent.date_dim"


# CREATE SCHEMAS TABLES

gbleventstg_schema_create = ("""CREATE SCHEMA IF NOT EXISTS gbleventstg""")
gblevent_schema_create = ("""CREATE SCHEMA IF NOT EXISTS gblevent""")


# CREATE STAGING TABLES

gbldata_staging_table_create = ("""CREATE TABLE IF NOT EXISTS gbleventstg.gbldata_staging
    (relcd varchar,
    cntrycd varchar,
    actcd varchar,
    evntcd varchar,
    globaleventid  varchar,
    day varchar,
    month varchar,
    year varchar,
    evntname varchar,
    actorname varchar,
    newsource varchar,
    goldsteinscale varchar,
    nummentions varchar,
    envtclscd varchar,
    envtclsdesc varchar,
    evntypcd varchar,
    evnttypdesc varchar,
    evntsubtypcd varchar,
    evntsubtypdesc varchar,
    evntdesc varchar,
    actdesc varchar,
    acttypcd varchar,
    acttypdesc varchar,
    continent varchar,
    region varchar,
    cntry varchar,
    reldesc varchar,
    relcat varchar);
    """)

gbldata_stgfact_table_create = ("""CREATE TABLE IF NOT EXISTS gbleventstg.gbldata_stgfact
                                   (globaleventid int,
                                    event_date date,
                                    actcd varchar,
                                    evntcd varchar,
                                    cntrycd varchar,
                                    relcd varchar,
                                    evntname varchar,
                                    actorname varchar,
                                    goldstein_scale numeric (3,1),
                                    num_mentions int,
                                    evnt_cnt int)
                                    DISTSTYLE AUTO
                                    SORTKEY AUTO;
                                  """)
                                  
                                  
actorclass_stgdim_table_create = ("""CREATE TABLE IF NOT EXISTS gbleventstg.actorclass_stgdim
                                     (actcd varchar,
                                      actdesc varchar,
                                      acttypcd varchar,
                                      acttypdesc varchar)
                                      DISTSTYLE AUTO
                                      SORTKEY AUTO;
                                  """)


evntclass_stgdim_table_create = ("""CREATE TABLE IF NOT EXISTS gbleventstg.evntclass_stgdim
                                    (envtclscd varchar,
                                     envtclsdesc varchar,
                                     evntypcd varchar,
                                     evnttypdesc varchar,
                                     evntsubtypcd varchar,
                                     evntsubtypdesc varchar,
                                     evntcd varchar,
                                     evntdesc varchar)
                                     DISTSTYLE AUTO
                                     SORTKEY AUTO;
                                   """)

evntloc_stgdim_table_create = ("""CREATE TABLE IF NOT EXISTS gbleventstg.evntloc_stgdim
                                  (cntrycd varchar,
                                   continent varchar,
                                   region varchar,
                                   cntry varchar)
                                   DISTSTYLE AUTO
                                   SORTKEY AUTO;
                                """)

evntcat_stgdim_table_create = ("""CREATE TABLE IF NOT EXISTS gbleventstg.evntcat_stgdim
                                 (relcd varchar,
                                  reldesc varchar,
                                  relcat varchar)
                                  DISTSTYLE AUTO
                                  SORTKEY AUTO;
                               """)

# CREATE FINAL TABLES

gbldata_fact_table_create = ("""CREATE TABLE IF NOT EXISTS gblevent.gbldata_fact
                                (globaleventid int,
                                 date_key int,
                                 actor_key int,
                                 evntcls_key int,
                                 evntloc_key int,
                                 evntcat_key int,
                                 evntname varchar,
                                 actorname varchar,
                                 goldstein_scale numeric (3,1),
                                 num_mentions int,
                                 evnt_cnt int)
                                 DISTSTYLE AUTO
                                 SORTKEY AUTO;
                               """)

actorclass_dim_table_create = ("""CREATE TABLE IF NOT EXISTS gblevent.actorclass_dim
                                  (actor_key int IDENTITY(0,1),
                                   actcd varchar,
                                   actdesc varchar,
                                   acttypcd varchar,
                                   acttypdesc varchar)
                                   DISTSTYLE AUTO
                                   SORTKEY AUTO;
                               """)

evntclass_dim_table_create = ("""CREATE TABLE IF NOT EXISTS gblevent.evntclass_dim
                                 (evntcls_key int IDENTITY(0,1),
                                  envtclscd varchar,
                                  envtclsdesc varchar,
                                  evntypcd varchar,
                                  evnttypdesc varchar,
                                  evntsubtypcd varchar,
                                  evntsubtypdesc varchar,
                                  evntcd varchar,
                                  evntdesc varchar)
                                  DISTSTYLE AUTO
                                  SORTKEY AUTO;
                               """)

evntloc_dim_table_create = ("""CREATE TABLE IF NOT EXISTS gblevent.evntloc_dim
                               (evntloc_key int IDENTITY(0,1),
                                cntrycd varchar,
                                continent varchar,
                                region varchar,
                                cntry varchar)
                                DISTSTYLE AUTO
                                SORTKEY AUTO;
                               """)
                               
evntcat_dim_table_create = ("""CREATE TABLE IF NOT EXISTS gblevent.evntcat_dim
                               (evntcat_key int IDENTITY(0,1),
                                relcd varchar,
                                reldesc varchar,
                                relcat varchar)
                                DISTSTYLE AUTO
                                SORTKEY AUTO;
                               """)
    
date_dim_table_create = ("""CREATE TABLE IF NOT EXISTS gblevent.date_dim
                            (dateKey integer,
                             date date,
                             dayOfWeekName varchar (10),
                             dayOfWeek integer,
                             dayOfMonth integer,
                             dayOfYear integer,
                             calendarWeek integer,
                             calendarMonthName varchar (10),
                             calendarMonth integer,
                             calendarYear integer,
                             lastDayInMonth varchar (20))
                             DISTSTYLE AUTO
                             SORTKEY AUTO;
                          """)


# QUERY LISTS

create_stg_schema_query = [gbleventstg_schema_create]

create_prd_schema_query = [gblevent_schema_create]

create_stgtable_queries = [gbldata_staging_table_create,
                        gbldata_stgfact_table_create,
                        actorclass_stgdim_table_create,
                        evntclass_stgdim_table_create,
                        evntloc_stgdim_table_create,
                        evntcat_stgdim_table_create]

drop_stgtable_queries = [gbldata_staging_table_drop,
                      gbldata_stgfact_table_drop,
                      actorclass_stgdim_table_drop,
                      evntclass_stgdim_table_drop,
                      evntloc_stgdim_table_drop,
                      evntcat_stgdim_table_drop]
                      
create_table_queries = [gbldata_fact_table_create,
                        actorclass_dim_table_create,
                        evntclass_dim_table_create,
                        evntloc_dim_table_create,
                        evntcat_dim_table_create,
                        date_dim_table_create]

drop_table_queries = [gbldata_fact_table_drop,
                      actorclass_dim_table_drop,
                      evntclass_dim_table_drop,
                      evntloc_dim_table_drop,
                      evntcat_dim_table_drop,
                      date_dim_table_drop]  
                      
                      