TMPDIR="/gisdata/temp/"
UNZIPTOOL=unzip
WGETTOOL="/usr/bin/wget"
PSQL="psql uthealth"
SHP2PGSQL=shp2pgsql
cd /gisdata

cd /gisdata
wget https://www2.census.gov/geo/tiger/TIGER2017/PLACE/tl_2017_48_place.zip --mirror --reject=html
cd /gisdata/www2.census.gov/geo/tiger/TIGER2017/PLACE
rm -f ${TMPDIR}/*.*
${PSQL} -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
${PSQL} -c "CREATE SCHEMA tiger_staging;"
for z in tl_2017_48*_place.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
cd $TMPDIR;

${PSQL} -c "CREATE TABLE tiger_data.TX_place(LIKE tiger.place INCLUDING ALL) DISTRIBUTED REPLICATED;" 
${SHP2PGSQL} -D -c -s 4269 -g the_geom   -W "latin1" tl_2017_48_place.dbf tiger_staging.tx_place | ${PSQL}
${PSQL} -c "ALTER TABLE tiger_staging.TX_place RENAME geoid TO plcidfp;SELECT loader_load_staged_data(lower('TX_place'), lower('TX_place')); ALTER TABLE tiger_data.TX_place ADD CONSTRAINT uidx_TX_place_gid UNIQUE (gid);"
${PSQL} -c "INSERT INTO tiger.place SELECT * FROM tiger_data.TX_place;"
${PSQL} -c "CREATE INDEX idx_TX_place_soundex_name ON tiger_data.TX_place USING btree (soundex(name));"
${PSQL} -c "CREATE INDEX tiger_data_TX_place_the_geom_gist ON tiger_data.TX_place USING gist(the_geom);"
${PSQL} -c "ALTER TABLE tiger_data.TX_place ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
cd /gisdata
wget https://www2.census.gov/geo/tiger/TIGER2017/COUSUB/tl_2017_48_cousub.zip --mirror --reject=html
cd /gisdata/www2.census.gov/geo/tiger/TIGER2017/COUSUB
rm -f ${TMPDIR}/*.*
${PSQL} -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
${PSQL} -c "CREATE SCHEMA tiger_staging;"
for z in tl_2017_48*_cousub.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
cd $TMPDIR;

${PSQL} -c "CREATE TABLE tiger_data.TX_cousub(LIKE tiger.cousub INCLUDING ALL) DISTRIBUTED REPLICATED;" 
${SHP2PGSQL} -D -c -s 4269 -g the_geom   -W "latin1" tl_2017_48_cousub.dbf tiger_staging.tx_cousub | ${PSQL}
${PSQL} -c "ALTER TABLE tiger_staging.TX_cousub RENAME geoid TO cosbidfp;SELECT loader_load_staged_data(lower('TX_cousub'), lower('TX_cousub')); ALTER TABLE tiger_data.TX_cousub ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
${PSQL} -c "INSERT INTO tiger.cousub SELECT * FROM tiger_data.TX_cousub;"
${PSQL} -c "CREATE INDEX tiger_data_TX_cousub_the_geom_gist ON tiger_data.TX_cousub USING gist(the_geom);"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_cousub_countyfp ON tiger_data.TX_cousub USING btree(countyfp);"
cd /gisdata
wget https://www2.census.gov/geo/tiger/TIGER2017/TRACT/tl_2017_48_tract.zip --mirror --reject=html
cd /gisdata/www2.census.gov/geo/tiger/TIGER2017/TRACT
rm -f ${TMPDIR}/*.*
${PSQL} -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
${PSQL} -c "CREATE SCHEMA tiger_staging;"
for z in tl_2017_48*_tract.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
cd $TMPDIR;

${PSQL} -c "CREATE TABLE tiger_data.TX_tract(LIKE tiger.tract INCLUDING ALL) DISTRIBUTED REPLICATED; " 
${SHP2PGSQL} -D -c -s 4269 -g the_geom   -W "latin1" tl_2017_48_tract.dbf tiger_staging.tx_tract | ${PSQL}
${PSQL} -c "ALTER TABLE tiger_staging.TX_tract RENAME geoid TO tract_id;  SELECT loader_load_staged_data(lower('TX_tract'), lower('TX_tract'));  INSERT INTO tiger.tract SELECT * FROM tiger_data.TX_tract;"
	${PSQL} -c "CREATE INDEX tiger_data_TX_tract_the_geom_gist ON tiger_data.TX_tract USING gist(the_geom);"
	${PSQL} -c "VACUUM ANALYZE tiger_data.TX_tract;"
	${PSQL} -c "ALTER TABLE tiger_data.TX_tract ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
cd /gisdata
cd /gisdata/www2.census.gov/geo/tiger/TIGER2017/FACES/
rm -f ${TMPDIR}/*.*
${PSQL} -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
${PSQL} -c "CREATE SCHEMA tiger_staging;"
for z in tl_*_48*_faces*.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
cd $TMPDIR;

${PSQL} -c "CREATE TABLE tiger_data.TX_faces(LIKE tiger.faces INCLUDING ALL) DISTRIBUTED REPLICATED;" 
for z in *faces*.dbf; do
${SHP2PGSQL} -D   -D -s 4269 -g the_geom -W "latin1" $z tiger_staging.TX_faces | ${PSQL}
${PSQL} -c "SELECT loader_load_staged_data(lower('TX_faces'), lower('TX_faces'));"
done

${PSQL} -c "INSERT INTO tiger.faces SELECT * FROM tiger_data.TX_faces;"
	${PSQL} -c "CREATE INDEX tiger_data_TX_faces_the_geom_gist ON tiger_data.TX_faces USING gist(the_geom);"
	${PSQL} -c "CREATE INDEX idx_tiger_data_TX_faces_tfid ON tiger_data.TX_faces USING btree (tfid);"
	${PSQL} -c "CREATE INDEX idx_tiger_data_TX_faces_countyfp ON tiger_data.TX_faces USING btree (countyfp);"
	${PSQL} -c "ALTER TABLE tiger_data.TX_faces ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
	${PSQL} -c "vacuum analyze tiger_data.TX_faces;"
cd /gisdata
cd /gisdata/www2.census.gov/geo/tiger/TIGER2017/FEATNAMES/
rm -f ${TMPDIR}/*.*
${PSQL} -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
${PSQL} -c "CREATE SCHEMA tiger_staging;"
for z in tl_*_48*_featnames*.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
cd $TMPDIR;

${PSQL} -c "CREATE TABLE tiger_data.TX_featnames(LIKE tiger.featnames INCLUDING ALL) DISTRIBUTED REPLICATED;ALTER TABLE tiger_data.TX_featnames ALTER COLUMN statefp SET DEFAULT '48';" 
for z in *featnames*.dbf; do
${SHP2PGSQL} -D   -D -s 4269 -g the_geom -W "latin1" $z tiger_staging.TX_featnames | ${PSQL}
${PSQL} -c "SELECT loader_load_staged_data(lower('TX_featnames'), lower('TX_featnames'));"
done

${PSQL} -c "INSERT INTO tiger.featnames SELECT * FROM tiger_data.TX_featnames;"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_featnames_snd_name ON tiger_data.TX_featnames USING btree (soundex(name));"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_featnames_lname ON tiger_data.TX_featnames USING btree (lower(name));"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_featnames_tlid_statefp ON tiger_data.TX_featnames USING btree (tlid,statefp);"
${PSQL} -c "ALTER TABLE tiger_data.TX_featnames ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
${PSQL} -c "vacuum analyze tiger_data.TX_featnames;"
cd /gisdata
cd /gisdata/www2.census.gov/geo/tiger/TIGER2017/EDGES/
rm -f ${TMPDIR}/*.*
${PSQL} -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
${PSQL} -c "CREATE SCHEMA tiger_staging;"
for z in tl_*_48*_edges*.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
cd $TMPDIR;

${PSQL} -c "CREATE TABLE tiger_data.TX_edges(LIKE tiger.edges INCLUDING ALL) DISTRIBUTED REPLICATED;"
for z in *edges*.dbf; do
${SHP2PGSQL} -D   -D -s 4269 -g the_geom -W "latin1" $z tiger_staging.TX_edges | ${PSQL}
${PSQL} -c "SELECT loader_load_staged_data(lower('TX_edges'), lower('TX_edges'));"
done

${PSQL} -c "ALTER TABLE tiger_data.TX_edges ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
${PSQL} -c "INSERT INTO tiger.edges SELECT * FROM tiger_data.TX_edges;"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_edges_tlid ON tiger_data.TX_edges USING btree (tlid);"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_edgestfidr ON tiger_data.TX_edges USING btree (tfidr);"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_edges_tfidl ON tiger_data.TX_edges USING btree (tfidl);"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_edges_countyfp ON tiger_data.TX_edges USING btree (countyfp);"
${PSQL} -c "CREATE INDEX tiger_data_TX_edges_the_geom_gist ON tiger_data.TX_edges USING gist(the_geom);"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_edges_zipl ON tiger_data.TX_edges USING btree (zipl);"
${PSQL} -c "CREATE TABLE tiger_data.TX_zip_state_loc(LIKE tiger.zip_state_loc INCLUDING ALL) DISTRIBUTED REPLICATED;"
${PSQL} -c "INSERT INTO tiger_data.TX_zip_state_loc(zip,stusps,statefp,place) SELECT DISTINCT e.zipl, 'TX', '48', p.name FROM tiger_data.TX_edges AS e INNER JOIN tiger_data.TX_faces AS f ON (e.tfidl = f.tfid OR e.tfidr = f.tfid) INNER JOIN tiger_data.TX_place As p ON(f.statefp = p.statefp AND f.placefp = p.placefp ) WHERE e.zipl IS NOT NULL;"
${PSQL} -c "INSERT INTO tiger.zip_state_loc SELECT * FROM tiger_data.TX_zip_state_loc;"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_zip_state_loc_place ON tiger_data.TX_zip_state_loc USING btree(soundex(place));"
${PSQL} -c "ALTER TABLE tiger_data.TX_zip_state_loc ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
${PSQL} -c "vacuum analyze tiger_data.TX_edges;"
${PSQL} -c "vacuum analyze tiger_data.TX_zip_state_loc;"
${PSQL} -c "CREATE TABLE tiger_data.TX_zip_lookup_base(LIKE tiger.zip_lookup_base, CONSTRAINT pk_TX_zip_state_loc_city PRIMARY KEY(zip,state, county, city, statefp)) DISTRIBUTED REPLICATED;"
${PSQL} -c "INSERT INTO tiger_data.TX_zip_lookup_base(zip,state,county,city, statefp) SELECT DISTINCT e.zipl, 'TX', c.name,p.name,'48'  FROM tiger_data.TX_edges AS e INNER JOIN tiger.county As c  ON (e.countyfp = c.countyfp AND e.statefp = c.statefp AND e.statefp = '48') INNER JOIN tiger_data.TX_faces AS f ON (e.tfidl = f.tfid OR e.tfidr = f.tfid) INNER JOIN tiger_data.TX_place As p ON(f.statefp = p.statefp AND f.placefp = p.placefp ) WHERE e.zipl IS NOT NULL;"
${PSQL} -c "INSERT INTO tiger.zip_lookup_base SELECT * FROM tiger_data.TX_zip_lookup_base;"
${PSQL} -c "ALTER TABLE tiger_data.TX_zip_lookup_base ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_zip_lookup_base_citysnd ON tiger_data.TX_zip_lookup_base USING btree(soundex(city));"
cd /gisdata
cd /gisdata/www2.census.gov/geo/tiger/TIGER2017/ADDR/
rm -f ${TMPDIR}/*.*
${PSQL} -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
${PSQL} -c "CREATE SCHEMA tiger_staging;"
for z in tl_*_48*_addr*.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
cd $TMPDIR;

${PSQL} -c "CREATE TABLE tiger_data.TX_addr(LIKE tiger.addr INCLUDING ALL) DISTRIBUTED REPLICATED;ALTER TABLE tiger_data.TX_addr ALTER COLUMN statefp SET DEFAULT '48';" 
for z in *addr*.dbf; do
${SHP2PGSQL} -D   -D -s 4269 -g the_geom -W "latin1" $z tiger_staging.TX_addr | ${PSQL}
${PSQL} -c "SELECT loader_load_staged_data(lower('TX_addr'), lower('TX_addr'));"
done

${PSQL} -c "ALTER TABLE tiger_data.TX_addr ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
	${PSQL} -c "INSERT INTO tiger.addr SELECT * FROM tiger_data.TX_addr;"
	${PSQL} -c "CREATE INDEX idx_tiger_data_TX_addr_least_address ON tiger_data.TX_addr USING btree (least_hn(fromhn,tohn) );"
	${PSQL} -c "CREATE INDEX idx_tiger_data_TX_addr_tlid_statefp ON tiger_data.TX_addr USING btree (tlid, statefp);"
	${PSQL} -c "CREATE INDEX idx_tiger_data_TX_addr_zip ON tiger_data.TX_addr USING btree (zip);"
	${PSQL} -c "CREATE TABLE tiger_data.TX_zip_state(LIKE tiger.zip_state INCLUDING ALL) DISTRIBUTED REPLICATED; "
	${PSQL} -c "INSERT INTO tiger_data.TX_zip_state(zip,stusps,statefp) SELECT DISTINCT zip, 'TX', '48' FROM tiger_data.TX_addr WHERE zip is not null;"
	${PSQL} -c "INSERT INTO tiger.zip_state SELECT * FROM tiger_data.TX_zip_state;"
	${PSQL} -c "ALTER TABLE tiger_data.TX_zip_state ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
	${PSQL} -c "vacuum analyze tiger_data.TX_addr;"
