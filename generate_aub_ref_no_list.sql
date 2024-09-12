-- FUNCTION: public.generate_aub_ref_no_list(character varying)

-- DROP FUNCTION IF EXISTS public.generate_aub_ref_no_list(character varying);

CREATE OR REPLACE FUNCTION public.generate_aub_ref_no_list()
    RETURNS TABLE(c_unit character varying, c_client_name character varying, c_reference_no character varying) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
  v_RecordLoop RECORD;
  v_limit integer;
BEGIN
	--v_limit	:= (select (((last_no - first_no)+1) - (( (case when last_no_used = 0 then (first_no-1) else last_no_used end) - first_no) + 1)) from cs_receipt_book where emp_code = p_emp_code and status_id = 'A' and last_no != last_no_used and doc_id = '03' and co_id = '02' and branch_id = '01'/*hard coded kasi puro TV lang ang happywell added branch_id na hard coded april 3 2023*/);
	-- this is to get how many receipts employee had
	FOR v_RecordLoop IN 
	(
		select a.entity_id, a.projcode, a.pbl_id, a.seq_no,
		e.entity_name, FORMAT('%s-%s', b.proj_alias, c.description) as unit,
		d.reference_no
		from rf_sold_unit a
		LEFT JOIN mf_project b on TRIM(b.proj_id) = TRIM(a.projcode)
		LEFT JOIN mf_unit_info c on TRIM(c.proj_id) = TRIM(a.projcode) and TRIM(c.pbl_id) = TRIM(a.pbl_id)
		left join rf_aub_pmt_reference_table_v2 d on TRIM(d.entity_id) = TRIM(a.entity_id) and TRIM(d.proj_id) = TRIM(a.projcode) and TRIM(d.pbl_id) = TRIM(a.pbl_id) and d.seq_no = a.seq_no and TRIM(d.status_id) = 'A'
		LEFT JOIN rf_entity e on e.entity_id = a.entity_id
		where  d.status_id = 'A'
		and get_group_id(a.buyertype) IN ('02', '04')
		and case when a.projcode = '019' then TRIM(c.phase) = '1-B' 
				 when a.projcode = '015' then trim(c.phase) IN ('3', '5')
				 when a.projcode = '018' THEN TRUE 
				 WHEN a.projcode = '017' THEN trim(c.phase) = '2'
				 ELSE FALSE END
		ORDER BY b.proj_alias, getinteger(c.phase), getinteger(c.block), getinteger(c.lot)
	) LOOP /********** START OF THE LOOP **********/ 
	
		c_unit 			:= v_RecordLoop.unit;
		c_client_name 	:= v_RecordLoop.entity_name;
		c_reference_no  := v_RecordLoop.reference_no;

    RETURN NEXT;
  END LOOP;

  RETURN;
END;
$BODY$;

ALTER FUNCTION public.generate_aub_ref_no_list()
    OWNER TO jffatallo;
