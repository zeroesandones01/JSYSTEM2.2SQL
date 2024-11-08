-- FUNCTION: public.view_card_itsreal_soa_boi(character varying, character varying, character varying, integer, boolean)

-- DROP FUNCTION IF EXISTS public.view_card_itsreal_soa_boi(character varying, character varying, character varying, integer, boolean);

CREATE OR REPLACE FUNCTION public.view_card_itsreal_soa_boi(
	p_entity_id character varying,
	p_proj_id character varying,
	p_pbl_id character varying,
	p_seq_no integer,
	p_refund boolean)
    RETURNS TABLE(c_actual_date timestamp without time zone, c_trans_date timestamp without time zone, c_sched_date timestamp without time zone, c_amount_paid numeric, c_pico numeric, c_proc_fees numeric, c_rpt_amt numeric, c_res numeric, c_dp numeric, c_mri numeric, c_fire numeric, c_vat numeric, c_soi numeric, c_sop numeric, c_penalty numeric, c_cbp numeric, c_adjustment numeric, c_interest numeric, c_principal numeric, c_balance numeric, c_percent_paid numeric, c_pay_rec_id integer, c_due_type character varying, c_receipt_no character varying) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE

	v_Pay_Part_ID VARCHAR;
	v_intRecID INTEGER;
	v_runningReservation NUMERIC;
	v_runningDownpayment NUMERIC;
	v_runningPrincipal NUMERIC;

	v_totalReservation NUMERIC;
	v_totalDownpayment NUMERIC;
	v_totalPrincipal NUMERIC;
	v_totalProcFee NUMERIC;

	v_numNSP NUMERIC;
	v_numBalance NUMERIC;
	v_tmpSchedDate TIMESTAMP;
	v_intRow INTEGER;

	v_recSchedule RECORD;
	v_recLedger RECORD;
	v_Receipt_ID VARCHAR;

	v_Principal_Sched NUMERIC;
	v_Principal_Sched_Paid NUMERIC;
	v_Proc_Fee_Sched NUMERIC;

	v_check_no VARCHAR;
	v_minORdate TIMESTAMP;
	v_actualDate TIMESTAMP;
	v_Payment RECORD;
	v_sold_unit RECORD;
	v_TR_DATE DATE;
	
	v_Record RECORD;
	
	
	--LAST MODIFIED BY LESTER 2023-02-27

BEGIN
    select into v_sold_unit * from rf_sold_unit  WHERE entity_id = p_entity_id AND projcode = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no;
	v_TR_DATE          := (SELECT actual_date::DATE FROM rf_buyer_status WHERE TRIM(entity_id) = p_entity_id AND TRIM(proj_id) = p_proj_id AND TRIM(pbl_id) = p_pbl_id AND seq_no = p_seq_no AND TRIM(byrstatus_id) = '17' AND status_id = 'A');

	v_numNSP			:= (SELECT net_sprice FROM rf_client_price_history WHERE TRIM(entity_id) = TRIM(p_entity_id) AND TRIM(proj_ID) = trim(p_proj_id) AND trim(pbl_id) = trim(p_pbl_id) AND seq_no = p_seq_no AND status_id = 'A');
	v_totalReservation	:= (SELECT SUM(principal) FROM rf_client_schedule WHERE TRIM(entity_id) = TRIM(p_entity_id) AND TRIM(proj_id) = trim(p_proj_id) AND trim(pbl_id) = trim(p_pbl_id) AND seq_no = p_seq_no AND part_id = '012' AND status_id = 'A');
	RAISE INFO 'NSP: %', v_numNSP;
																									  
-- 	IF p_entity_id = '3436559580' then
-- 		v_totalDownpayment := (SELECT SUM(principal) FROM rf_client_schedule WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no AND part_id = '013' AND status_id = 'A');
-- 	ELSE
		v_totalDownpayment	:= v_numNSP; 
	--END IF;
	
	v_totalPrincipal	:= (SELECT SUM(principal) FROM rf_client_schedule WHERE entity_id = p_entity_id AND proj_id = p_proj_id AND pbl_id = p_pbl_id AND seq_no = p_seq_no AND part_id = '014' AND status_id = 'A');
	v_totalProcFee		:= (SELECT SUM(proc_fee) from rf_client_schedule where entity_id = p_entity_id and proj_id = p_proj_id and pbl_id = p_pbl_id and seq_no = p_seq_no and part_id = '013' and status_id = 'A');
	v_minORdate			:= (SELECT min(or_date) from rf_payments where entity_id = p_entity_id and pbl_id = p_pbl_id and seq_no = p_seq_no and or_date is not null and status_id != 'I');
	
	v_numBalance		:= v_numNSP;
	
	FOR v_Record IN (SELECT *
					 FROM rf_itsreal_bir_soa
					 WHERE trim(entity_id) = trim(p_entity_id)
					 and trim(proj_id) = trim(p_proj_id)
					 and trim(pbl_id) = trim(p_pbl_id)
					 and seq_no = p_seq_no
					 and status_id = 'A'
					 ORDER BY rec_id) LOOP
			
			c_actual_date 	:= v_Record.actual_pmt_date;
			c_trans_date 	:= NULL;
			c_sched_date 	:= v_Record.pmt_due_date;
			c_amount_paid 	:= v_Record.amt_paid; 
			c_pico 			:= NULL;
			c_proc_fees 	:= v_Record.other_fees; 
			c_rpt_amt 		:= null; 
			c_res 			:= v_Record.reservation; 
			c_dp 			:= v_Record.dp; 
			c_mri 			:= v_Record.mri; 
			c_fire 			:= v_Record.fire; 
			c_vat 			:= v_Record.vat; 
			c_soi 			:= v_Record.soi; 
			c_sop 			:= v_Record.sop; 
			c_penalty 		:= v_Record.resdp_penalty; 
			c_cbp 			:= null;
			c_adjustment 	:= null; 
			c_interest 		:= v_Record.interest; 
			c_principal 	:= v_Record.principal; 
			v_numBalance	:= COALESCE(v_numBalance, 0) - COALESCE(c_dp, 0) - COALESCE(c_principal, 0);
			c_balance 		:= v_numBalance; 
			
			IF NULLIF(c_res, 0) IS NOT NULL THEN
				c_percent_paid	:= 0.00;
			ELSE
				raise info 'Balance: %', c_balance;
				c_percent_paid := NULLIF(((v_numNSP-c_balance) / v_numNSP) * 100, 0);
			END IF;
			
			c_pay_rec_id 	:= null; 
			c_due_type 		:= null; 
			c_receipt_no 	:= v_Record.receipt_no;
					 
			return next;		 
					 
	END LOOP;

RETURN;

END;
$BODY$;

ALTER FUNCTION public.view_card_itsreal_soa_boi(character varying, character varying, character varying, integer, boolean)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION public.view_card_itsreal_soa_boi(character varying, character varying, character varying, integer, boolean) TO PUBLIC;

GRANT EXECUTE ON FUNCTION public.view_card_itsreal_soa_boi(character varying, character varying, character varying, integer, boolean) TO employee;

GRANT EXECUTE ON FUNCTION public.view_card_itsreal_soa_boi(character varying, character varying, character varying, integer, boolean) TO postgres;

COMMENT ON function public.view_card_itsreal_soa_boi(character varying, character varying, character varying, integer, boolean) IS 'This function display ITSREAL SOA BIR IN JSYSTEM CARD'