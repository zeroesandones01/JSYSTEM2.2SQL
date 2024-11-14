-- FUNCTION: public.sp_save_lump_payment_itsreal_soa(character varying, timestamp without time zone)

-- DROP FUNCTION IF EXISTS public.sp_save_lump_payment_itsreal_soa(character varying, character varying, character varying, character varying, character varying, numeric, date, character varying, character varying ,character varying);

CREATE OR REPLACE FUNCTION public.sp_save_lump_payment_itsreal_soa(
	p_entity_id character varying, p_proj_id character varying, p_pbl_id character varying, p_seq_no character varying, p_receipt_no character varying, p_lump_amt numeric, p_date_lump date, p_pay_part_id character varying, p_user_id character varying, p_branch_id character varying)
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	v_Mf_Project RECORD;
	v_Sold_Unit RECORD;
	v_clientSeqNo VARCHAR(12);
	v_Unit_Info RECORD;

BEGIN

	SELECT INTO v_Mf_Project * FROM mf_project where trim(proj_id) = p_proj_id;
	SELECT INTO v_Unit_Info * FROM mf_unit_info where trim(proj_id) = p_proj_id and trim(pbl_id) = p_pbl_id;
	SELECT INTO v_Sold_Unit * FROM rf_sold_unit where trim(entity_id) = p_entity_id and trim(projcode) = p_proj_id and trim(pbl_id) = p_pbl_id and seq_no = p_seq_no::INT;
	v_clientSeqNo := get_new_client_seqno(p_branch_id, FALSE);
	
	IF NOT EXISTS (SELECT * FROM rf_payments where trim(entity_id) = p_entity_id and trim(proj_id) = p_proj_id and trim(pbl_id) = p_pbl_id 
				   and seq_no = p_seq_no::INT AND status_id = 'A' AND remarks ~*'Itsreal SOA Lump') THEN
		
		INSERT INTO public.rf_pay_header(
		client_seqno, entity_id, branch_id, trans_date, booking_date, proj_id, pbl_id, seq_no, selling_price, mktgarm, selling_agent, type_id, 
		pmt_scheme_id, type_of_sale, total_amt_paid, status_id, model_id, co_id, created_by, date_created, rs_time_out, cs_time_out, op_status, unit_id)
		VALUES(v_clientSeqNo, p_entity_id, p_branch_id, p_date_lump, p_date_lump, p_proj_id, p_pbl_id, p_seq_no, v_Sold_Unit.sellingprice, v_Sold_Unit.mktgarm,
			  v_Sold_Unit.sellingagent, v_Sold_Unit.pmt_scheme_id, '01', p_lump_amt, 'A', v_Sold_Unit.model_id, v_Mf_Project.co_id, p_user_id, now(), 
			  to_char(now(), 'HH:MI:SS AM'), to_char(now(), 'HH:MI:SS AM'), 'P', v_Unit_Info.unit_id);
			  
		INSERT INTO public.rf_pay_detail(
		client_seqno, entity_id, part_type, amount, receipt_no, receipt_type, status_id, pending_or, due_type, co_id)
		VALUES (v_clientSeqNo, p_entity_id, p_branch_id, p_pay_part_id, p_lump_amt,p_receipt_no, '01', 'A', TRUE, 'R', v_Mf_Project.co_id);	  
		

		INSERT INTO public.rf_payments(
			entity_id, proj_id, pbl_id, seq_no, actual_date, trans_date, pay_part_id, pymnt_type, amount, or_no, or_date, remarks, branch_id, client_seqno, 
			or_doc_id, status_id, pay_rec_id, co_id, unit_id, created_by, date_created, proj_server, server_id)
			VALUES (p_entity_id, p_proj_id, p_pbl_id, p_seq_no, p_date_lump, p_date_lump, p_pay_part_id, 'A', p_lump_amt, p_receipt_no, p_date_lump, 
					'Itsreal SOA Lump', p_branch_id, v_clientSeqNo, '01', 'A', (SELECT nextval('rf_payments_rec_id_seq')), v_Mf_Project.co_id, 
					v_Unit_Info.unit_id, p_user_id, now(), v_Unit_Info.proj_server, v_Unit_Info.server_id);

	
	END IF;
  
  
  
    
  
RETURN TRUE;
END;
$BODY$;

ALTER FUNCTION public.sp_save_lump_payment_itsreal_soa(character varying, character varying, character varying, character varying, character varying, numeric, date, character varying, character varying ,character varying)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION public.sp_save_lump_payment_itsreal_soa(character varying, character varying, character varying, character varying, character varying, numeric, date, character varying, character varying ,character varying) TO PUBLIC;

GRANT EXECUTE ON FUNCTION public.sp_save_lump_payment_itsreal_soa(character varying, character varying, character varying, character varying, character varying, numeric, date, character varying, character varying ,character varying) TO employee;

GRANT EXECUTE ON FUNCTION public.sp_save_lump_payment_itsreal_soa(character varying, character varying, character varying, character varying, character varying, numeric, date, character varying, character varying ,character varying) TO postgres;

COMMENT ON FUNCTION public.sp_save_lump_payment_itsreal_soa(character varying, character varying, character varying, character varying, character varying, numeric, date, character varying, character varying ,character varying) IS 'Function to insert payments for the lumpsum amt';