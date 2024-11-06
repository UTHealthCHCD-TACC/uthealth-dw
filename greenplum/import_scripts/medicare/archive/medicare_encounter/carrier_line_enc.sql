drop table if exists medicare_national.carrier_line_enc;

create table medicare_national.carrier_line_enc
(
	year text,
	BENE_ID varchar,
	ENC_JOIN_KEY varchar,
	CLM_TYPE_CD varchar,
	CLM_LINE_NUM varchar,
	CLM_THRU_DT varchar,
	PRVDR_NPI varchar,
	PRVDR_SPCLTY varchar,
	LINE_SRVC_CNT varchar,
	LINE_PLACE_OF_SRVC_CD varchar,
	LINE_1ST_EXPNS_DT varchar,
	LINE_LAST_EXPNS_DT varchar,
	HCPCS_CD varchar,
	HCPCS_1ST_MDFR_CD varchar,
	HCPCS_2ND_MDFR_CD varchar,
	HCPCS_3RD_MDFR_CD varchar,
	HCPCS_4TH_MDFR_CD varchar,
	LINE_NDC_CD varchar,
	LINE_RX_NUM varchar,
	LINE_LTST_CLM_IND varchar,
	LINE_NUM_ORIG varchar
)
with(
    appendonly = true,
    orientation = column,
    compresstype = zlib
)
distributed by (BENE_ID);