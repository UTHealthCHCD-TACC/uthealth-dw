drop function rm_trail_spaces();
create function rm_trail_spaces() returns void as 
   $$

    declare
        selectrow record;
    begin
    for selectrow in
    select 
           'UPDATE '||quote_ident(c.table_schema)||'.'||quote_ident(c.table_name)||' SET '||quote_ident(c.COLUMN_NAME)||'=TRIM('||quote_ident(c.COLUMN_NAME)||')  WHERE '||quote_ident(c.COLUMN_NAME)||' ILIKE ''% '' ' as script
    from (
           select 
              table_schema,table_name,COLUMN_NAME
           from 
              INFORMATION_SCHEMA.COLUMNS 
           where 
             table_schema in ('optum_zip', 'optum_dod', 'medicaid', 'medicare_texas', 'medicare_national', 'truven', 'data_warehouse')  
             and (data_type='text' or data_type='character varying' )
             and is_updatable='YES'
         ) c
    loop
    execute selectrow.script;
    end loop;
    end;
  $$
  language plpgsql  
  
  SELECT rm_trail_spaces();
  