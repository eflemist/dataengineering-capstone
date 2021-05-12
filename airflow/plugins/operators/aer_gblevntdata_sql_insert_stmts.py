class AERSqlInserts:

# STAGING TABLES
# insert statements for staging fact and dimension tables

    actorclass_stgdim_table_insert = ("""
        SELECT DISTINCT actcd, actdesc, acttypcd, acttypdesc
          FROM gbleventstg.gbldata_staging
          """)

    evntclass_stgdim_table_insert = ("""
        SELECT DISTINCT envtclscd, envtclsdesc, evntypcd, evnttypdesc, evntsubtypcd,
               evntsubtypdesc, evntcd, evntdesc
          FROM gbleventstg.gbldata_staging
        """)

    evntloc_stgdim_table_insert = ("""
        SELECT DISTINCT cntrycd, continent, region, cntry
        FROM gbleventstg.gbldata_staging
    """)

    evntcat_stgdim_table_insert = ("""
        SELECT DISTINCT relcd, reldesc, relcat
        FROM gbleventstg.gbldata_staging
    """)

    gbldata_stgfact_table_insert = ("""
        SELECT globaleventid::INTEGER, day::DATE, actcd, evntcd, cntrycd,
               relcd, evntname, actorname, goldsteinscale::NUMERIC(3,1), nummentions::INTEGER, 1
          FROM gbleventstg.gbldata_staging
    """)
    
 
# FINAL TABLES
# insert statements for final and dimension tables

    actorclass_dim_tbl_ins_into_clause = ("""
        gblevent.actorclass_dim (actcd, actdesc, acttypcd, acttypdesc)
          """)

    actorclass_dim_tbl_ins_val_clause = ("""
        SELECT asd.actcd, asd.actdesc, asd.acttypcd, asd.acttypdesc
	    FROM gbleventstg.actorclass_stgdim asd
	    WHERE NOT EXISTS 
	      (SELECT 1 FROM gblevent.actorclass_dim acd WHERE acd.actcd = asd.actcd );
          """)
    
    evntclass_dim_tbl_ins_into_clause = ("""
        gblevent.evntclass_dim (envtclscd, envtclsdesc, evntypcd, evnttypdesc, evntsubtypcd, evntsubtypdesc, evntcd, evntdesc)
                 """) 
                  
    evntclass_dim_tbl_ins_val_clause = ("""
        SELECT esd.envtclscd, esd.envtclsdesc, esd.evntypcd, esd.evnttypdesc, esd.evntsubtypcd, esd.evntsubtypdesc, esd.evntcd, esd.evntdesc
          FROM gbleventstg.evntclass_stgdim esd
    	  WHERE NOT EXISTS 
    	     (SELECT 1 FROM gblevent.evntclass_dim ecd WHERE ecd.evntcd = esd.evntcd );
    """)    
    
    evntloc_dim_tbl_ins_into_clause = ("""
        gblevent.evntloc_dim (cntrycd, continent, region, cntry)
       """)

    evntloc_dim_tbl_ins_val_clause = ("""
        SELECT esd.cntrycd, esd.continent, esd.region, esd.cntry
	  FROM gbleventstg.evntloc_stgdim esd
	  WHERE NOT EXISTS 
	    (SELECT 1 FROM gblevent.evntloc_dim eld WHERE eld.cntrycd = esd.cntrycd);
       """)
    
    evntcat_dim_tbl_ins_into_clause = ("""
        gblevent.evntcat_dim (relcd, reldesc, relcat)
        """)
    

    evntcat_dim_tbl_ins_val_clause = ("""
        SELECT esd.relcd, esd.reldesc, esd.relcat
	  FROM gbleventstg.evntcat_stgdim esd
	  WHERE NOT EXISTS 
	    (SELECT 1 FROM gblevent.evntcat_dim ecd WHERE ecd.relcd = esd.relcd );
    """)