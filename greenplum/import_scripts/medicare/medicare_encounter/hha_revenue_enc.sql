drop table if exists medicare_national.hha_revenue_enc;

create table medicare_national.hha_revenue_enc
(
	year text,
	BENE_ID varchar,
	ENC_JOIN_KEY varchar,
	CLM_TYPE_CD varchar,
	CLM_LINE_NUM varchar,
	CLM_THRU_DT varchar,
	REV_CNTR varchar,
	REV_CNTR_FROM_DT varchar,
	REV_CNTR_THRU_DT varchar,
	REV_CNTR_UNIT_CNT varchar,
	HCPCS_CD varchar,
	HCPCS_1ST_MDFR_CD varchar,
	HCPCS_2ND_MDFR_CD varchar,
	HCPCS_3RD_MDFR_CD varchar,
	REV_CNTR_IDE_NDC_UPC_NUM varchar,
	REV_CNTR_NDC_QTY varchar,
	REV_CNTR_NDC_QTY_QLFR_CD varchar,
	REV_CNTR_RNDRNG_PHYSN_NPI varchar,
	LINE_LTST_CLM_IND varchar,
	LINE_NUM_ORIG varchar
)
with(
    appendonly = true,
    orientation = column,
    compresstype = zlib
)
distributed by (BENE_ID);