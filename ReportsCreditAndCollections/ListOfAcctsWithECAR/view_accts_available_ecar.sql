-- FUNCTION: public.view_accts_available_ecar(character varying, character varying, character varying)

-- DROP FUNCTION IF EXISTS public.view_accts_available_ecar(character varying, character varying, character varying);

CREATE OR REPLACE FUNCTION public.view_accts_available_ecar(
	p_co_id character varying,
	p_proj_id character varying,
	p_phase character varying)
    RETURNS TABLE(c_client_name character varying, c_company_alias character varying, c_proj_alias character varying, c_unit character varying, c_ecar_amt numeric) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE 

v_WithECAR RECORD;
v_TCT_Mother RECORD;

BEGIN
   FOR v_WithECAR in (select d.entity_name, FORMAT('%s-%s-%s-%s', b.proj_alias, c.phase, c.block, c.lot) as unit,
					  a.entity_id, a.projcode, a.pbl_id, a.seq_no, e.company_alias, b.proj_alias
						from rf_sold_unit a
						LEFT JOIN mf_project b on TRIM(b.proj_id) = TRIM(a.projcode)
						LEFT JOIN mf_unit_info c on TRIM(c.proj_id) = TRIM(a.projcode) and TRIM(c.pbl_id) = TRIM(a.pbl_id)
						LEFT JOIN rf_entity d on d.entity_id = a.entity_id
						LEFT JOIN mf_company e on e.co_id = b.co_id
						where a.currentstatus != '02'
						AND a.status_id = 'A'
					    and case when nullif(p_co_id, 'null') IS NULL OR nullif(p_co_id, 'All') IS NULL THEN TRUE ELSE TRIM(b.co_id) = p_co_id end
					    and case when nullif(p_proj_id, 'null') IS NULL OR nullif(p_proj_id, 'All') IS NULL THEN TRUE ELSE TRIM(a.projcode) = p_proj_id end
					    AND CASE WHEN NULLIF(p_phase, 'null') IS NULL OR NULLIF(p_phase, 'All') IS NULL THEN TRUE ELSE trim(c.phase) = p_phase end
						and exists (select g.doc_status
									from rf_tct_taxdec_monitoring_hd f
									LEFT JOIN rf_tct_taxdec_monitoring_dl g on g.doc_no = f.doc_no and g.pbl_id = g.pbl_id
									where trim(f.proj_id) = trim(a.projcode)
									and trim(f.pbl_id) = trim(a.pbl_id)
									and trim(g.doc_status) = '229')
					    ORDER BY b.co_id, b.proj_alias, getinteger(c.phase), getinteger(c.block), getinteger(c.lot)) LOOP
				
				c_client_name   := v_WithECAR.entity_name; 
				c_company_alias := v_WithECAR.company_alias; 
				c_proj_alias 	:= v_WithECAR.proj_alias; 
				c_unit 			:= v_WithECAR.unit; 
				c_ecar_amt 		:= (SELECT sum(c_tcost_detail_amt) FROM view_card_tcost_computation_v2(v_WithECAR.entity_id, v_WithECAR.projcode, v_WithECAR.pbl_id, v_WithECAR.seq_no, true));
				
				
				RETURN NEXT;
			
		END LOOP;

END;
$BODY$;

ALTER FUNCTION public.view_accts_available_ecar(character varying, character varying, character varying)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION public.view_accts_available_ecar(character varying, character varying, character varying) TO PUBLIC;

GRANT EXECUTE ON FUNCTION public.view_accts_available_ecar(character varying, character varying, character varying) TO employee;

GRANT EXECUTE ON FUNCTION public.view_accts_available_ecar(character varying, character varying, character varying) TO postgres;

