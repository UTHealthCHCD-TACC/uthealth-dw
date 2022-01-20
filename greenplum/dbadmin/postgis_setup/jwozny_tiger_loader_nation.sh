TMPDIR="/gisdata/temp/"
UNZIPTOOL=unzip
WGETTOOL="/usr/bin/wget"
PSQL="psql uthealth"
SHP2PGSQL=shp2pgsql
cd /gisdata

cd /gisdata
wget https://www2.census.gov/geo/tiger/TIGER2019/STATE/tl_2019_us_state.zip --mirror --reject=html
cd /gisdata/www2.census.gov/geo/tiger/TIGER2019/STATE
rm -f ${TMPDIR}/*.*
${PSQL} -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
${PSQL} -c "CREATE SCHEMA tiger_staging;"
for z in tl_*state.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
cd $TMPDIR;

${PSQL} -c "CREATE TABLE tiger_data.state_all(LIKE tiger.state INCLUDING ALL) DISTRIBUTED REPLICATED; " #(CONSTRAINT pk_state_all PRIMARY KEY (statefp),CONSTRAINT uidx_state_all_stusps  UNIQUE (stusps), CONSTRAINT uidx_state_all_gid UNIQUE (gid) ) INHERITS(tiger.state); "
${SHP2PGSQL} -D -c -s 4269 -g the_geom   -W "latin1" tl_2019_us_state.dbf tiger_staging.state | ${PSQL}
${PSQL} -c "SELECT loader_load_staged_data(lower('state'), lower('state_all')); "
	${PSQL} -c "CREATE INDEX tiger_data_state_all_the_geom_gist ON tiger_data.state_all USING gist(the_geom);"
	${PSQL} -c "VACUUM ANALYZE tiger_data.state_all"
cd /gisdata
wget https://www2.census.gov/geo/tiger/TIGER2019/COUNTY/tl_2019_us_county.zip --mirror --reject=html
cd /gisdata/www2.census.gov/geo/tiger/TIGER2019/COUNTY
rm -f ${TMPDIR}/*.*
${PSQL} -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
${PSQL} -c "CREATE SCHEMA tiger_staging;"
for z in tl_*county.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
cd $TMPDIR;

${PSQL} -c "CREATE TABLE tiger_data.county_all(LIKE tiger.county INCLUDING ALL) DISTRIBUTED REPLICATED;" #(CONSTRAINT pk_tiger_data_county_all PRIMARY KEY (cntyidfp),CONSTRAINT uidx_tiger_data_county_all_gid UNIQUE (gid)  ) INHERITS(tiger.county); " 
${SHP2PGSQL} -D -c -s 4269 -g the_geom   -W "latin1" tl_2019_us_county.dbf tiger_staging.county | ${PSQL}
${PSQL} -c "ALTER TABLE tiger_staging.county RENAME geoid TO cntyidfp;  SELECT loader_load_staged_data(lower('county'), lower('county_all'));"
	${PSQL} -c "CREATE INDEX tiger_data_county_the_geom_gist ON tiger_data.county_all USING gist(the_geom);"
	${PSQL} -c "CREATE UNIQUE INDEX uidx_tiger_data_county_all_statefp_countyfp ON tiger_data.county_all USING btree(statefp,countyfp);"
	${PSQL} -c "CREATE TABLE tiger_data.county_all_lookup(LIKE tiger.county_lookup INCLUDING ALL) DISTRIBUTED REPLICATED; " # ( CONSTRAINT pk_county_all_lookup PRIMARY KEY (st_code, co_code)) INHERITS (tiger.county_lookup);"
	${PSQL} -c "VACUUM ANALYZE tiger_data.county_all;"
	${PSQL} -c "INSERT INTO tiger_data.county_all_lookup(st_code, state, co_code, name) SELECT CAST(s.statefp as integer), s.abbrev, CAST(c.countyfp as integer), c.name FROM tiger_data.county_all As c INNER JOIN state_lookup As s ON s.statefp = c.statefp;"
	${PSQL} -c "VACUUM ANALYZE tiger_data.county_all_lookup;" 
cd /gisdata
wget https://www2.census.gov/geo/tiger/TIGER2019/ZCTA5/tl_2019_us_zcta510.zip --mirror --reject=html
cd /gisdata/www2.census.gov/geo/tiger/TIGER2019/ZCTA5
rm -f ${TMPDIR}/*.*
${PSQL} -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
${PSQL} -c "CREATE SCHEMA tiger_staging;"
for z in tl_*zcta510.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
cd $TMPDIR;

${PSQL} -c "CREATE TABLE tiger_data.zcta5_raw( zcta5 character varying(5), classfp character varying(2),mtfcc character varying(5), funcstat character varying(1), aland double precision, awater double precision, intptlat character varying(11), intptlon character varying(12), the_geom geometry(MultiPolygon,4269) );"
${SHP2PGSQL} -D -c -s 4269 -g the_geom   -W "latin1" tl_2019_us_zcta510.dbf tiger_staging.zcta510 | ${PSQL}
${PSQL} -c "ALTER TABLE tiger.zcta5 DROP CONSTRAINT IF EXISTS enforce_geotype_the_geom; CREATE TABLE tiger_data.zcta5_all(LIKE tiger.zcta5 INCLUDING ALL) DISTRIBUTED REPLICATED; " #(CONSTRAINT pk_zcta5_all PRIMARY KEY (zcta5ce,statefp), CONSTRAINT uidx_zcta5_raw_all_gid UNIQUE (gid)) INHERITS(tiger.zcta5);"
${PSQL} -c "SELECT loader_load_staged_data(lower('zcta510'), lower('zcta5_raw'));"
${PSQL} -c "INSERT INTO tiger_data.zcta5_all(statefp, zcta5ce, classfp, mtfcc, funcstat, aland, awater, intptlat, intptlon, partflg, the_geom) SELECT  s.statefp, z.zcta5,  z.classfp, z.mtfcc, z.funcstat, z.aland, z.awater, z.intptlat, z.intptlon, CASE WHEN ST_Covers(s.the_geom, z.the_geom) THEN 'N' ELSE 'Y' END, ST_SnapToGrid(ST_Transform(CASE WHEN ST_Covers(s.the_geom, z.the_geom) THEN ST_SimplifyPreserveTopology(ST_Transform(z.the_geom,2163),1000) ELSE ST_SimplifyPreserveTopology(ST_Intersection(ST_Transform(s.the_geom,2163), ST_Transform(z.the_geom,2163)),1000)  END,4269), 0.000001) As geom FROM tiger_data.zcta5_raw AS z INNER JOIN tiger.state AS s ON (ST_Covers(s.the_geom, z.the_geom) or ST_Overlaps(s.the_geom, z.the_geom) );"
	${PSQL} -c "DROP TABLE tiger_data.zcta5_raw; CREATE INDEX idx_tiger_data_zcta5_all_the_geom_gist ON tiger_data.zcta5_all USING gist(the_geom);"

