CREATE OR REPLACE PROCEDURE gblevent.load_datedim(
	start_date date,
	end_date date)
LANGUAGE 'plpgsql'
AS $BODY$
declare
    date_var date := start_date ;
    date_diff int := (end_date - start_date) ;
    
    rec_cnt int := 0;
    
begin
  
 --check if records already in date table
  
  select count(*) into rec_cnt from gblevent.date_dim;
  
  if rec_cnt >= 1 then
    null;
  else  
    for i in 0 .. date_diff loop
        insert into gblevent.date_dim (
             dateKey
            , date
            , dayOfWeekName
            , dayOfWeek
            , dayOfMonth
            , dayOfYear
            , calendarWeek
            , calendarMonthName
            , calendarMonth
            , calendarYear
        )
        values ( to_char(date_var, 'YYYYMMDD')::integer
           , date_var
           , to_char(date_var, 'Day')
           , date_part('dow', date_var)
           , date_part('day', date_var)
           , date_part('doy', date_var)
           , date_part('week', date_var)
           , to_char(date_var, 'Month')
           , date_part('month', date_var)
           , date_part('year', date_var)
           );
        --on conflict (datekey) do nothing;
        date_var := date_var + interval '1 day' ;
    end loop;
  end if;	
end;
$BODY$;

--call load_dateDim(CAST('2014-01-01' as date), CAST('2015-12-31' as date));