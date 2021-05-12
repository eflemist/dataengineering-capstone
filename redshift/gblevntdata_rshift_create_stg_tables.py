#----------------
# Standard library imports
#----------------

#----------------
# Third Party imports
#----------------

import psycopg2

#---------------
# Local app imports
#--------------

from gblevntdata_rshift_sql_create_stmnt import create_stgtable_queries, drop_stgtable_queries, create_stg_schema_query
import gblevntdata_rshift_config as db_config

#--------------

def create_schema(cur, conn):
    """ create prd schema using code in gblevntdata_rshift_sql_create_stmnt module """
    for query in create_stg_schema_query:
        cur.execute(query)
        conn.commit()

def drop_tables(cur, conn):
    """ removes tables from db using the drop table queries from the gblevntdata_rshift_sql_create_stmnt module """
    for query in drop_stgtable_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """ create tables in db using the create table queries from the gblevntdata_rshift_sql_create_stmnt module """
    for query in create_stgtable_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(db_config.DWH_HOST, db_config.DWH_DB, db_config.DWH_DB_USER, db_config.DWH_DB_PASSWORD, db_config.DWH_PORT))
    cur = conn.cursor()

#----create schema gbleventstg

    create_schema(cur, conn)
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()