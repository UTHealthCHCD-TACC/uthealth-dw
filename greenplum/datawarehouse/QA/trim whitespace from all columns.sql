



select 'update ' || quote_ident(c.table_schema) || '.' || quote_ident(c.table_name)||' set ' || c.column_name || 
       '=trim('||quote_ident(c.column_name)||') WHERE ' || quote_ident(c.column_name) || ' LIKE ''%'' ;' as script
from ( 
select table_schema, table_name, column_name 
from information_schema.columns a 
where a.table_schema = 'medicaid' and table_name = 'admit'
and data_type in ('text','character varying')
) c
;

vacuum analyze medicaid.enc_dx;


update medicaid.enc_det set rend_prov_id = trim(rend_prov_id);



do $$ 
declare 
	selectrow record;
begin 
	for selectrow in 
	select 'update ' || quote_ident(c.table_schema) || '.' || quote_ident(c.table_name)||' set ' || c.column_name || 
       '=trim('||quote_ident(c.column_name)||') WHERE ' || quote_ident(c.column_name) || ' LIKE ''%'' ;' as script
from ( 
select table_schema, table_name, column_name 
from information_schema.columns a 
where a.table_schema = 'medicaid' and table_name = 'enc_dx'
and data_type in ('text','character varying')
) c
loop 
execute selectrow.script; 
raise notice 'updated: %', selectrow;
end loop;
end;
$$;