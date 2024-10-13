-- FUNCTION: public.view_unilateral_gen_monitoring()

-- DROP FUNCTION IF EXISTS public.view_unilateral_gen_monitoring(character varying, character varying, character varying);

CREATE OR REPLACE FUNCTION public.view_unilateral_gen_monitoring(
	p_co_id character varying, p_proj_id character varying, p_phase character varying)
    RETURNS TABLE(c_group character varying, c_client_name character varying, c_proj_alias character varying, c_phase character varying, c_block character varying, c_lot character varying, c_model_house character varying, c_tct_no character varying, c_selling_price numeric, c_date date, c_td_lot_no character varying, c_td_house_no character varying, c_mc_rel date, c_dst_rplf character varying) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE 
	v_rec RECORD;
	v_GroupA RECORD;
	v_GroupB RECORD;
	v_GroupC RECORD;
	v_GroupD RECORD;
	v_GroupE RECORD;
	v_GroupF RECORD;
	v_GroupG RECORD;
	v_GroupH RECORD;
	v_GroupI RECORD;
	v_GroupJ RECORD;
	v_GroupK RECORD;
	
BEGIN

	FOR v_rec IN(select a.entity_id, a.projcode, a.pbl_id, a.seq_no, get_client_name(a.entity_id) as client_name, b.proj_alias, c.phase, c.block, c.lot, d.model_desc, e.net_sprice
				from rf_sold_unit a
				LEFT JOIN mf_project b on b.proj_id= a.projcode
				LEFT JOIN mf_unit_info c on trim(c.proj_id) = trim(a.projcode) and trim(c.pbl_id) = trim(a.pbl_id)
				LEFT JOIN mf_product_model d on trim(d.model_id) = trim(a.model_id) and coalesce(d.server_id, '') = coalesce(a.server_id) and coalesce(d.proj_server, '') = coalesce(a.proj_server, '')
				LEFT JOIN rf_client_price_history e on trim(e.entity_id) = trim(a.entity_id) and trim(e.proj_id) = trim(a.projcode) and trim(e.pbl_id) = trim(a.pbl_id) and e.seq_no = a.seq_no and trim(e.status_id) = 'A'
				where a.currentstatus != '02'
				AND a.status_id = 'A'
				AND get_group_id(trim(a.buyertype)) = '02'
				and case when nullif(p_co_id, 'null') is null then true else b.co_id = p_co_id end
				and case when nullif(p_proj_id, 'null') is null then true else a.projcode = p_proj_id END
				and case when nullif(p_phase, 'null') is null then true else c.phase = p_phase end
				AND EXISTS (SELECT *
				 			FROM rf_buyer_status
							where trim(entity_id) = trim(a.entity_id)
							and trim(proj_id) = trim(a.projcode)
							and trim(pbl_id) = trim(a.pbl_id)
							and seq_no = a.seq_no
							and trim(byrstatus_id) = '27'
							AND trim(status_id) = 'A')
				and not exists (SELECT *
				 				FROM rf_payments
								where trim(entity_id) = a.entity_id
								and trim(proj_id) = trim(a.projcode)
								and trim(pbl_id) = trim(a.pbl_id)
								and seq_no = a.seq_no
								and TRIM(pay_part_id) = '182'
								AND status_id = 'A')
				AND NOT EXISTS (SELECT *
				 			FROM rf_buyer_status
							where trim(entity_id) = trim(a.entity_id)
							and trim(proj_id) = trim(a.projcode)
							and trim(pbl_id) = trim(a.pbl_id)
							and seq_no = a.seq_no
							and trim(byrstatus_id) IN ('1D', '103')
							AND trim(status_id) = 'A')
				ORDER BY b.proj_alias, getinteger(c.phase), getinteger(c.block), getinteger(c.lot)) LOOP

		--CHECKS IF BUYER SUBMITTED THE BIR FORM 1904
		SELECT INTO v_GroupA * FROM rf_buyer_documents where trim(entity_id) = trim(v_rec.entity_id) and trim(projcode) = trim(v_rec.projcode) and trim(pbl_id) = trim(v_rec.pbl_id) and seq_no = v_rec.seq_no and trim(doc_id) = '149' and trim(status_id) = 'A'; 

		--CHECKS IF THE CLIENT HAS A PRINTED UDOAS IN THE SYSTEM
		SELECT INTO v_GroupB * FROM rf_printed_documents where trim(entity_id) = trim(v_rec.entity_id) and trim(projcode) = trim(v_rec.projcode) and trim(pbl_id) = trim(v_rec.pbl_id) and seq_no = v_rec.seq_no and trim(doc_id) = '309' and status_id = 'A';


		IF v_GroupA IS NOT NULL THEN
			c_group := 'A. TIN VERIFIED';
		END IF;

		IF v_GroupB IS NOT NULL THEN
			c_group := 'B. TIN NO RECORDS IN BIR';
		END IF;

		IF v_GroupC IS NOT NULL THEN
			c_group := 'C. UDOAS PRINTED';
		END IF;

		IF v_GroupD IS NOT NULL THEN
			c_group := 'D. UDOAS FOR SIGNING';
		END IF;

		IF v_GroupE IS NOT NULL THEN
			c_group := 'E. UDOAS SIGNED';
		END IF;

		IF v_GroupF IS NOT NULL THEN
			c_group := 'F. SOA FOR CHECKING';
		END IF;

		IF v_GroupG IS NOT NULL THEN
			c_group := 'G. NOTARIZED FORWARDED TO LLD';
		END IF;

		IF v_GroupH IS NOT NULL THEN
			c_group := 'H. DST SUBMITTED TO BIR';
		END IF;
		
		IF v_GroupI IS NOT NULL THEN
			c_group := 'I. ECAR SUBMITTED TO BIR';
		END IF;

		IF v_GroupJ IS NOT NULL THEN
			c_group := 'J. ECAR FORWARDED TO LRMD';
		END IF;

		IF v_GroupK IS NOT NULL THEN
			c_group := 'K. ECAR FORWARDED TO FAD';
		END IF;

		c_client_name 		:= 	v_rec.client_name;  
		c_proj_alias  		:= 	v_rec.proj_alias;
		c_phase 		 	:= 	v_rec.phase;
		c_block 			:= 	v_rec.block;
		c_lot 				:=	v_rec.lot;
		c_model_house 		:=	v_rec.model_desc;
		c_tct_no 		 	:= 	(select doc_no from rf_tct_taxdec_monitoring_hd where trim(proj_id) = trim(v_rec.projcode) and trim(pbl_id) = TRIM(v_rec.pbl_id) AND TRIM(doc_type) = '64' and TRIM(co_id) = TRIM(v_Project.co_id) and TRIM(status_id) = 'A' order by date_created desc limit 1);
		c_selling_price 	:=  v_rec.net_sprice;
		c_date 				:=  null;
		c_td_lot_no 	 	:=  null;
		c_td_house_no 		:= 	null;
		c_mc_rel  			:=  null;
		c_dst_rplf 			:= 	null;

		
		RETURN NEXT;

	END LOOP;

END;
$BODY$;

ALTER FUNCTION public.view_unilateral_gen_monitoring(character varying, character varying, character varying)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION public.view_unilateral_gen_monitoring(character varying, character varying, character varying) TO PUBLIC;

GRANT EXECUTE ON FUNCTION public.view_unilateral_gen_monitoring(character varying, character varying, character varying) TO employee;

GRANT EXECUTE ON FUNCTION public.view_unilateral_gen_monitoring(character varying, character varying, character varying) TO postgres;

