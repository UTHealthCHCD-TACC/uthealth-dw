TMPDIR="/gisdata/temp/"
UNZIPTOOL=unzip
WGETTOOL="/usr/bin/wget"
PSQL="psql uthealth"
SHP2PGSQL=shp2pgsql
cd /gisdata

cd /gisdata
wget https://www2.census.gov/geo/tiger/TIGER2019/PLACE/tl_2019_48_place.zip --mirror --reject=html
cd /gisdata/www2.census.gov/geo/tiger/TIGER2019/PLACE
rm -f ${TMPDIR}/*.*
${PSQL} -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
${PSQL} -c "CREATE SCHEMA tiger_staging;"
for z in tl_2019_48*_place.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
cd $TMPDIR;

${PSQL} -c "CREATE TABLE tiger_data.TX_place(LIKE tiger.place INCLUDING ALL) DISTRIBUTED REPLICATED;" #CONSTRAINT pk_TX_place PRIMARY KEY (plcidfp) ) INHERITS(tiger.place);" 
${SHP2PGSQL} -D -c -s 4269 -g the_geom   -W "latin1" tl_2019_48_place.dbf tiger_staging.tx_place | ${PSQL}
${PSQL} -c "ALTER TABLE tiger_staging.TX_place RENAME geoid TO plcidfp;SELECT loader_load_staged_data(lower('TX_place'), lower('TX_place')); ALTER TABLE tiger_data.TX_place ADD CONSTRAINT uidx_TX_place_gid UNIQUE (gid);"
${PSQL} -c "CREATE INDEX idx_TX_place_soundex_name ON tiger_data.TX_place USING btree (soundex(name));"
${PSQL} -c "CREATE INDEX tiger_data_TX_place_the_geom_gist ON tiger_data.TX_place USING gist(the_geom);"
${PSQL} -c "ALTER TABLE tiger_data.TX_place ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
cd /gisdata
wget https://www2.census.gov/geo/tiger/TIGER2019/COUSUB/tl_2019_48_cousub.zip --mirror --reject=html
cd /gisdata/www2.census.gov/geo/tiger/TIGER2019/COUSUB
rm -f ${TMPDIR}/*.*
${PSQL} -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
${PSQL} -c "CREATE SCHEMA tiger_staging;"
for z in tl_2019_48*_cousub.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
cd $TMPDIR;

${PSQL} -c "CREATE TABLE tiger_data.TX_cousub(LIKE tiger.cousub INCLUDING ALL) DISTRIBUTED REPLICATED;" #CONSTRAINT pk_TX_cousub PRIMARY KEY (cosbidfp), CONSTRAINT uidx_TX_cousub_gid UNIQUE (gid)) INHERITS(tiger.cousub);" 
${SHP2PGSQL} -D -c -s 4269 -g the_geom   -W "latin1" tl_2019_48_cousub.dbf tiger_staging.tx_cousub | ${PSQL}
${PSQL} -c "ALTER TABLE tiger_staging.TX_cousub RENAME geoid TO cosbidfp;SELECT loader_load_staged_data(lower('TX_cousub'), lower('TX_cousub')); ALTER TABLE tiger_data.TX_cousub ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
${PSQL} -c "CREATE INDEX tiger_data_TX_cousub_the_geom_gist ON tiger_data.TX_cousub USING gist(the_geom);"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_cousub_countyfp ON tiger_data.TX_cousub USING btree(countyfp);"
cd /gisdata
wget https://www2.census.gov/geo/tiger/TIGER2019/TRACT/tl_2019_48_tract.zip --mirror --reject=html
cd /gisdata/www2.census.gov/geo/tiger/TIGER2019/TRACT
rm -f ${TMPDIR}/*.*
${PSQL} -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
${PSQL} -c "CREATE SCHEMA tiger_staging;"
for z in tl_2019_48*_tract.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
cd $TMPDIR;

${PSQL} -c "CREATE TABLE tiger_data.TX_tract(LIKE tiger.tract INCLUDING ALL) DISTRIBUTED REPLICATED;" #CONSTRAINT pk_TX_tract PRIMARY KEY (tract_id) ) INHERITS(tiger.tract); " 
${SHP2PGSQL} -D -c -s 4269 -g the_geom   -W "latin1" tl_2019_48_tract.dbf tiger_staging.tx_tract | ${PSQL}
${PSQL} -c "ALTER TABLE tiger_staging.TX_tract RENAME geoid TO tract_id;  SELECT loader_load_staged_data(lower('TX_tract'), lower('TX_tract')); "
	${PSQL} -c "CREATE INDEX tiger_data_TX_tract_the_geom_gist ON tiger_data.TX_tract USING gist(the_geom);"
	${PSQL} -c "VACUUM ANALYZE tiger_data.TX_tract;"
	${PSQL} -c "ALTER TABLE tiger_data.TX_tract ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
cd /gisdata
wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48001_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48003_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48005_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48007_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48009_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48011_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48013_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48015_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48017_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48019_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48021_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48023_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48025_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48027_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48029_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48031_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48033_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48035_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48037_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48039_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48041_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48043_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48045_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48047_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48049_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48051_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48053_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48055_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48057_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48059_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48061_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48063_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48065_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48067_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48069_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48071_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48073_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48075_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48077_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48079_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48081_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48083_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48085_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48087_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48089_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48091_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48093_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48095_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48097_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48099_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48101_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48103_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48105_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48107_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48109_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48111_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48113_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48115_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48117_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48119_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48121_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48123_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48125_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48127_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48129_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48131_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48133_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48135_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48137_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48139_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48141_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48143_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48145_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48147_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48149_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48151_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48153_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48155_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48157_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48159_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48161_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48163_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48165_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48167_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48169_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48171_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48173_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48175_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48177_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48179_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48181_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48183_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48185_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48187_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48189_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48191_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48193_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48195_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48197_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48199_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48201_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48203_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48205_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48207_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48209_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48211_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48213_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48215_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48217_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48219_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48221_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48223_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48225_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48227_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48229_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48231_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48233_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48235_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48237_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48239_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48241_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48243_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48245_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48247_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48249_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48251_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48253_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48255_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48257_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48259_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48261_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48263_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48265_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48267_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48269_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48271_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48273_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48275_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48277_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48279_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48281_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48283_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48285_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48287_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48289_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48291_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48293_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48295_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48297_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48299_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48301_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48303_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48305_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48307_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48309_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48311_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48313_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48315_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48317_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48319_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48321_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48323_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48325_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48327_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48329_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48331_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48333_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48335_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48337_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48339_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48341_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48343_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48345_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48347_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48349_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48351_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48353_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48355_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48357_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48359_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48361_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48363_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48365_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48367_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48369_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48371_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48373_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48375_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48377_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48379_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48381_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48383_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48385_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48387_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48389_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48391_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48393_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48395_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48397_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48399_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48401_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48403_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48405_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48407_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48409_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48411_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48413_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48415_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48417_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48419_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48421_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48423_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48425_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48427_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48429_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48431_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48433_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48435_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48437_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48439_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48441_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48443_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48445_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48447_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48449_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48451_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48453_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48455_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48457_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48459_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48461_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48463_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48465_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48467_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48469_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48471_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48473_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48475_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48477_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48479_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48481_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48483_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48485_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48487_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48489_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48491_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48493_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48495_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48497_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48499_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48501_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48503_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48505_faces.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FACES/tl_2019_48507_faces.zip 
cd /gisdata/www2.census.gov/geo/tiger/TIGER2019/FACES/
rm -f ${TMPDIR}/*.*
${PSQL} -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
${PSQL} -c "CREATE SCHEMA tiger_staging;"
for z in tl_*_48*_faces*.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
cd $TMPDIR;

${PSQL} -c "CREATE TABLE tiger_data.TX_faces(LIKE tiger.faces INCLUDING ALL) DISTRIBUTED REPLICATED;" #CONSTRAINT pk_TX_faces PRIMARY KEY (gid)) INHERITS(tiger.faces);" 
for z in *faces*.dbf; do
${SHP2PGSQL} -D   -D -s 4269 -g the_geom -W "latin1" $z tiger_staging.TX_faces | ${PSQL}
${PSQL} -c "SELECT loader_load_staged_data(lower('TX_faces'), lower('TX_faces'));"
done

${PSQL} -c "CREATE INDEX tiger_data_TX_faces_the_geom_gist ON tiger_data.TX_faces USING gist(the_geom);"
	${PSQL} -c "CREATE INDEX idx_tiger_data_TX_faces_tfid ON tiger_data.TX_faces USING btree (tfid);"
	${PSQL} -c "CREATE INDEX idx_tiger_data_TX_faces_countyfp ON tiger_data.TX_faces USING btree (countyfp);"
	${PSQL} -c "ALTER TABLE tiger_data.TX_faces ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
	${PSQL} -c "vacuum analyze tiger_data.TX_faces;"
cd /gisdata
wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48001_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48003_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48005_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48007_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48009_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48011_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48013_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48015_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48017_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48019_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48021_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48023_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48025_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48027_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48029_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48031_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48033_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48035_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48037_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48039_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48041_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48043_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48045_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48047_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48049_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48051_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48053_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48055_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48057_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48059_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48061_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48063_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48065_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48067_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48069_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48071_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48073_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48075_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48077_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48079_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48081_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48083_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48085_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48087_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48089_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48091_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48093_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48095_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48097_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48099_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48101_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48103_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48105_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48107_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48109_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48111_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48113_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48115_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48117_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48119_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48121_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48123_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48125_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48127_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48129_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48131_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48133_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48135_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48137_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48139_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48141_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48143_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48145_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48147_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48149_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48151_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48153_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48155_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48157_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48159_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48161_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48163_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48165_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48167_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48169_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48171_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48173_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48175_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48177_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48179_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48181_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48183_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48185_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48187_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48189_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48191_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48193_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48195_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48197_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48199_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48201_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48203_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48205_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48207_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48209_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48211_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48213_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48215_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48217_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48219_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48221_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48223_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48225_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48227_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48229_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48231_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48233_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48235_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48237_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48239_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48241_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48243_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48245_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48247_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48249_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48251_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48253_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48255_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48257_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48259_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48261_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48263_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48265_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48267_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48269_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48271_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48273_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48275_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48277_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48279_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48281_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48283_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48285_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48287_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48289_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48291_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48293_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48295_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48297_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48299_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48301_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48303_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48305_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48307_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48309_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48311_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48313_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48315_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48317_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48319_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48321_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48323_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48325_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48327_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48329_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48331_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48333_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48335_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48337_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48339_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48341_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48343_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48345_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48347_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48349_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48351_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48353_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48355_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48357_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48359_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48361_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48363_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48365_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48367_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48369_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48371_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48373_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48375_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48377_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48379_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48381_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48383_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48385_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48387_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48389_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48391_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48393_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48395_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48397_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48399_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48401_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48403_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48405_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48407_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48409_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48411_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48413_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48415_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48417_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48419_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48421_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48423_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48425_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48427_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48429_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48431_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48433_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48435_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48437_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48439_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48441_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48443_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48445_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48447_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48449_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48451_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48453_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48455_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48457_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48459_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48461_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48463_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48465_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48467_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48469_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48471_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48473_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48475_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48477_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48479_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48481_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48483_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48485_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48487_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48489_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48491_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48493_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48495_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48497_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48499_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48501_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48503_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48505_featnames.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/tl_2019_48507_featnames.zip 
cd /gisdata/www2.census.gov/geo/tiger/TIGER2019/FEATNAMES/
rm -f ${TMPDIR}/*.*
${PSQL} -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
${PSQL} -c "CREATE SCHEMA tiger_staging;"
for z in tl_*_48*_featnames*.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
cd $TMPDIR;

${PSQL} -c "CREATE TABLE tiger_data.TX_featnames(LIKE tiger.featnames INCLUDING ALL) DISTRIBUTED REPLICATED;" #(CONSTRAINT pk_TX_featnames PRIMARY KEY (gid)) INHERITS(tiger.featnames);ALTER TABLE tiger_data.TX_featnames ALTER COLUMN statefp SET DEFAULT '48';" 
for z in *featnames*.dbf; do
${SHP2PGSQL} -D   -D -s 4269 -g the_geom -W "latin1" $z tiger_staging.TX_featnames | ${PSQL}
${PSQL} -c "SELECT loader_load_staged_data(lower('TX_featnames'), lower('TX_featnames'));"
done

${PSQL} -c "CREATE INDEX idx_tiger_data_TX_featnames_snd_name ON tiger_data.TX_featnames USING btree (soundex(name));"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_featnames_lname ON tiger_data.TX_featnames USING btree (lower(name));"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_featnames_tlid_statefp ON tiger_data.TX_featnames USING btree (tlid,statefp);"
${PSQL} -c "ALTER TABLE tiger_data.TX_featnames ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
${PSQL} -c "vacuum analyze tiger_data.TX_featnames;"
cd /gisdata
wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48001_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48003_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48005_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48007_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48009_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48011_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48013_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48015_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48017_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48019_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48021_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48023_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48025_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48027_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48029_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48031_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48033_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48035_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48037_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48039_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48041_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48043_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48045_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48047_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48049_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48051_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48053_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48055_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48057_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48059_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48061_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48063_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48065_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48067_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48069_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48071_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48073_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48075_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48077_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48079_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48081_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48083_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48085_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48087_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48089_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48091_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48093_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48095_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48097_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48099_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48101_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48103_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48105_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48107_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48109_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48111_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48113_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48115_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48117_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48119_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48121_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48123_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48125_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48127_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48129_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48131_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48133_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48135_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48137_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48139_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48141_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48143_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48145_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48147_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48149_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48151_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48153_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48155_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48157_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48159_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48161_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48163_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48165_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48167_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48169_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48171_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48173_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48175_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48177_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48179_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48181_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48183_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48185_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48187_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48189_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48191_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48193_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48195_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48197_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48199_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48201_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48203_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48205_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48207_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48209_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48211_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48213_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48215_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48217_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48219_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48221_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48223_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48225_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48227_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48229_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48231_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48233_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48235_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48237_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48239_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48241_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48243_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48245_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48247_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48249_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48251_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48253_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48255_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48257_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48259_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48261_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48263_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48265_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48267_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48269_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48271_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48273_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48275_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48277_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48279_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48281_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48283_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48285_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48287_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48289_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48291_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48293_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48295_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48297_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48299_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48301_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48303_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48305_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48307_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48309_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48311_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48313_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48315_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48317_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48319_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48321_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48323_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48325_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48327_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48329_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48331_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48333_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48335_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48337_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48339_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48341_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48343_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48345_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48347_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48349_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48351_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48353_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48355_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48357_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48359_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48361_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48363_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48365_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48367_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48369_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48371_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48373_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48375_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48377_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48379_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48381_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48383_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48385_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48387_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48389_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48391_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48393_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48395_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48397_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48399_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48401_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48403_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48405_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48407_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48409_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48411_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48413_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48415_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48417_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48419_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48421_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48423_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48425_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48427_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48429_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48431_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48433_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48435_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48437_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48439_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48441_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48443_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48445_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48447_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48449_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48451_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48453_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48455_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48457_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48459_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48461_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48463_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48465_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48467_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48469_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48471_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48473_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48475_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48477_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48479_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48481_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48483_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48485_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48487_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48489_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48491_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48493_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48495_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48497_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48499_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48501_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48503_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48505_edges.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/EDGES/tl_2019_48507_edges.zip 
cd /gisdata/www2.census.gov/geo/tiger/TIGER2019/EDGES/
rm -f ${TMPDIR}/*.*
${PSQL} -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
${PSQL} -c "CREATE SCHEMA tiger_staging;"
for z in tl_*_48*_edges*.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
cd $TMPDIR;

${PSQL} -c "CREATE TABLE tiger_data.TX_edges(LIKE tiger.edges INCLUDING ALL) DISTRIBUTED REPLICATED;" #(CONSTRAINT pk_TX_edges PRIMARY KEY (gid)) INHERITS(tiger.edges);"
for z in *edges*.dbf; do
${SHP2PGSQL} -D   -D -s 4269 -g the_geom -W "latin1" $z tiger_staging.TX_edges | ${PSQL}
${PSQL} -c "SELECT loader_load_staged_data(lower('TX_edges'), lower('TX_edges'));"
done

${PSQL} -c "ALTER TABLE tiger_data.TX_edges ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_edges_tlid ON tiger_data.TX_edges USING btree (tlid);"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_edgestfidr ON tiger_data.TX_edges USING btree (tfidr);"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_edges_tfidl ON tiger_data.TX_edges USING btree (tfidl);"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_edges_countyfp ON tiger_data.TX_edges USING btree (countyfp);"
${PSQL} -c "CREATE INDEX tiger_data_TX_edges_the_geom_gist ON tiger_data.TX_edges USING gist(the_geom);"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_edges_zipl ON tiger_data.TX_edges USING btree (zipl);"
${PSQL} -c "CREATE TABLE tiger_data.TX_zip_state_loc(CONSTRAINT pk_TX_zip_state_loc PRIMARY KEY(zip,stusps,place)) INHERITS(tiger.zip_state_loc);"
${PSQL} -c "INSERT INTO tiger_data.TX_zip_state_loc(zip,stusps,statefp,place) SELECT DISTINCT e.zipl, 'TX', '48', p.name FROM tiger_data.TX_edges AS e INNER JOIN tiger_data.TX_faces AS f ON (e.tfidl = f.tfid OR e.tfidr = f.tfid) INNER JOIN tiger_data.TX_place As p ON(f.statefp = p.statefp AND f.placefp = p.placefp ) WHERE e.zipl IS NOT NULL;"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_zip_state_loc_place ON tiger_data.TX_zip_state_loc USING btree(soundex(place));"
${PSQL} -c "ALTER TABLE tiger_data.TX_zip_state_loc ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
${PSQL} -c "vacuum analyze tiger_data.TX_edges;"
${PSQL} -c "vacuum analyze tiger_data.TX_zip_state_loc;"
${PSQL} -c "CREATE TABLE tiger_data.TX_zip_lookup_base(CONSTRAINT pk_TX_zip_state_loc_city PRIMARY KEY(zip,state, county, city, statefp)) INHERITS(tiger.zip_lookup_base);"
${PSQL} -c "INSERT INTO tiger_data.TX_zip_lookup_base(zip,state,county,city, statefp) SELECT DISTINCT e.zipl, 'TX', c.name,p.name,'48'  FROM tiger_data.TX_edges AS e INNER JOIN tiger.county As c  ON (e.countyfp = c.countyfp AND e.statefp = c.statefp AND e.statefp = '48') INNER JOIN tiger_data.TX_faces AS f ON (e.tfidl = f.tfid OR e.tfidr = f.tfid) INNER JOIN tiger_data.TX_place As p ON(f.statefp = p.statefp AND f.placefp = p.placefp ) WHERE e.zipl IS NOT NULL;"
${PSQL} -c "ALTER TABLE tiger_data.TX_zip_lookup_base ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
${PSQL} -c "CREATE INDEX idx_tiger_data_TX_zip_lookup_base_citysnd ON tiger_data.TX_zip_lookup_base USING btree(soundex(city));"
cd /gisdata
wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48001_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48003_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48005_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48007_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48009_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48011_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48013_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48015_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48017_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48019_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48021_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48023_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48025_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48027_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48029_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48031_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48033_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48035_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48037_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48039_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48041_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48043_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48045_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48047_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48049_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48051_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48053_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48055_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48057_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48059_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48061_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48063_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48065_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48067_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48069_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48071_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48073_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48075_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48077_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48079_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48081_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48083_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48085_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48087_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48089_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48091_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48093_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48095_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48097_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48099_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48101_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48103_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48105_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48107_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48109_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48111_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48113_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48115_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48117_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48119_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48121_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48123_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48125_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48127_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48129_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48131_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48133_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48135_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48137_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48139_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48141_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48143_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48145_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48147_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48149_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48151_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48153_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48155_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48157_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48159_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48161_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48163_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48165_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48167_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48169_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48171_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48173_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48175_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48177_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48179_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48181_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48183_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48185_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48187_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48189_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48191_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48193_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48195_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48197_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48199_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48201_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48203_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48205_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48207_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48209_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48211_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48213_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48215_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48217_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48219_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48221_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48223_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48225_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48227_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48229_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48231_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48233_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48235_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48237_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48239_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48241_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48243_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48245_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48247_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48249_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48251_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48253_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48255_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48257_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48259_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48261_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48263_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48265_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48267_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48269_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48271_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48273_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48275_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48277_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48279_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48281_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48283_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48285_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48287_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48289_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48291_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48293_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48295_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48297_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48299_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48301_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48303_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48305_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48307_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48309_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48311_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48313_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48315_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48317_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48319_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48321_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48323_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48325_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48327_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48329_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48331_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48333_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48335_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48337_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48339_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48341_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48343_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48345_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48347_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48349_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48351_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48353_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48355_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48357_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48359_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48361_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48363_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48365_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48367_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48369_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48371_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48373_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48375_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48377_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48379_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48381_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48383_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48385_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48387_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48389_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48391_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48393_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48395_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48397_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48399_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48401_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48403_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48405_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48407_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48409_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48411_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48413_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48415_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48417_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48419_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48421_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48423_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48425_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48427_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48429_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48431_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48433_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48435_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48437_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48439_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48441_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48443_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48445_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48447_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48449_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48451_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48453_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48455_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48457_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48459_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48461_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48463_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48465_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48467_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48469_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48471_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48473_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48475_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48477_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48479_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48481_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48483_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48485_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48487_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48489_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48491_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48493_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48495_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48497_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48499_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48501_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48503_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48505_addr.zip 
 wget --mirror  https://www2.census.gov/geo/tiger/TIGER2019/ADDR/tl_2019_48507_addr.zip 
cd /gisdata/www2.census.gov/geo/tiger/TIGER2019/ADDR/
rm -f ${TMPDIR}/*.*
${PSQL} -c "DROP SCHEMA IF EXISTS tiger_staging CASCADE;"
${PSQL} -c "CREATE SCHEMA tiger_staging;"
for z in tl_*_48*_addr*.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
cd $TMPDIR;

${PSQL} -c "CREATE TABLE tiger_data.TX_addr(LIKE tiger.addr INCLUDING ALL) DISTRIBUTED REPLICATED;" #(CONSTRAINT pk_TX_addr PRIMARY KEY (gid)) INHERITS(tiger.addr);ALTER TABLE tiger_data.TX_addr ALTER COLUMN statefp SET DEFAULT '48';" 
for z in *addr*.dbf; do
${SHP2PGSQL} -D   -D -s 4269 -g the_geom -W "latin1" $z tiger_staging.TX_addr | ${PSQL}
${PSQL} -c "SELECT loader_load_staged_data(lower('TX_addr'), lower('TX_addr'));"
done

${PSQL} -c "ALTER TABLE tiger_data.TX_addr ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
	${PSQL} -c "CREATE INDEX idx_tiger_data_TX_addr_least_address ON tiger_data.TX_addr USING btree (least_hn(fromhn,tohn) );"
	${PSQL} -c "CREATE INDEX idx_tiger_data_TX_addr_tlid_statefp ON tiger_data.TX_addr USING btree (tlid, statefp);"
	${PSQL} -c "CREATE INDEX idx_tiger_data_TX_addr_zip ON tiger_data.TX_addr USING btree (zip);"

	${PSQL} -c "CREATE TABLE tiger_data.TX_zip_state(LIKE tiger.zip_state INCLUDING ALL) DISTRIBUTED REPLICATED;" #(CONSTRAINT pk_TX_zip_state PRIMARY KEY(zip,stusps)) INHERITS(tiger.zip_state); "
	${PSQL} -c "INSERT INTO tiger_data.TX_zip_state(zip,stusps,statefp) SELECT DISTINCT zip, 'TX', '48' FROM tiger_data.TX_addr WHERE zip is not null;"
	${PSQL} -c "ALTER TABLE tiger_data.TX_zip_state ADD CONSTRAINT chk_statefp CHECK (statefp = '48');"
	${PSQL} -c "vacuum analyze tiger_data.TX_addr;"

