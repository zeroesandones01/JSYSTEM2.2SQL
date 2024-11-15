-- FOREIGN TABLE: acerhomes.view_hlid_no

-- DROP FOREIGN TABLE IF EXISTS acerhomes.view_hlid_no;

CREATE FOREIGN TABLE IF NOT EXISTS acerhomes.view_hlid_no(
	entity_id character varying(10) NOT NULL COLLATE pg_catalog."default",
    proj_id character varying(3) NOT NULL COLLATE pg_catalog."default",
    pbl_id character varying(5) NOT NULL COLLATE pg_catalog."default",
    seq_no integer NOT NULL,
	hlid_no character varying(50) NOT NULL COLLATE pg_catalog."default",
    server_id character varying(25) NULL COLLATE pg_catalog."default",
    proj_server character varying(25) NULL COLLATE pg_catalog."default"
)
    SERVER server1
    OPTIONS (query 'select LTRIM(RTRIM(msu.Entity_id)) as entity_id, 
			 		(case when LTRIM(RTRIM(msu.ProjCode))=''005'' then ''019'' else case when LTRIM(RTRIM(msu.ProjCode))=''016'' then ''021'' else case when LTRIM(RTRIM(msu.ProjCode))=''008'' then ''017'' else LTRIM(RTRIM(msu.ProjCode)) end end end) as proj_id, 
			 		LTRIM(RTRIM(msu.Pbl_ID)) as pbl_id, 
			 		msu.Seq_no as seq_no, LTRIM(RTRIM(me.hlid_no)) as hlid_no,
			 		''old'' as proj_server , 
					''itsreal'' as server_id
					from dbo.mf_sold_unit msu
					LEFT JOIN dbo.mf_entity me on me.entity_id = msu.entity_id 
					where me.hlid_no IS NOT NULL AND me.hlid_no <> ''''
					order by msu.Entity_id');

ALTER FOREIGN TABLE acerhomes.view_hlid_no
    OWNER TO postgres;