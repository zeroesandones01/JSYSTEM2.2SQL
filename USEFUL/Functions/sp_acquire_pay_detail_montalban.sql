-- FUNCTION: public.sp_acquire_pay_detail_montalban(character varying, character varying, character varying, character varying, character varying)

-- DROP FUNCTION IF EXISTS public.sp_acquire_pay_detail_montalban(character varying, character varying, character varying, character varying, character varying);

CREATE OR REPLACE FUNCTION public.sp_acquire_pay_detail_montalban(
	p_host character varying,
	p_user character varying,
	p_password character varying,
	p_database character varying,
	p_date character varying)
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE

v_payments record;
v_Entity record;
v_Entity_ID varchar;
v_payments_Part_ID varchar; 

BEGIN
	PERFORM dblink_connect('dbPayHeader', 'host='||p_host||' user='||p_user||' password='||p_password||' dbname='||p_database||'');

	FOR v_payments IN (SELECT * FROM dblink('dbPayHeader', concat('select * from rf_pay_detail a where exists (select * from rf_pay_header where client_seqno = a.client_seqno and date_created::dATE = ''',p_date,'''::DATE AND branch_id = ''11'') order by pay_detail_id')) AS a
			(pay_detail_id bigint, 
			client_seqno character varying, 
			entity_id character varying, 
			part_type character varying, 
			bank character varying, 
			branch character varying, 
			acct_no character varying, 
			check_no character varying, 
			check_date timestamp without time zone, 
			amount numeric, 
			guarantee character varying, 
			receipt_no character varying, 
			receipt_type character varying, 
			status_id character, 
			brstn character varying, 
			bank_remit character varying, 
			inout_voucher boolean, 
			ar_no character varying, 
			arno_type character varying, 
			check_type character varying, 
			cashier_receipt boolean, 
			pending_or boolean, 
			client_ledger_part character varying, 
			ref_rec_id integer, 
			pay_for_lot character varying, 
			due_type character varying,
			ud_id integer,
			co_id character varying,
			reference_no character varying,
			remarks character varying)) LOOP

		v_Entity_ID := (select entity_id from rf_pay_header where client_seqno = v_payments.client_seqno);
		
		IF NOT EXISTS (SELECT * FROM rf_pay_detail where client_seqno = v_payments.client_seqno and part_type = v_payments.part_type) then
	           --IF v_payments.client_seqno = '060200213001' THEN
		   insert into rf_pay_detail (
				pay_detail_id, client_seqno, entity_id, part_type, bank, 
				branch, acct_no, check_no, check_date, amount, 
				guarantee, receipt_no, receipt_type, status_id, brstn, 
				bank_remit, inout_voucher, ar_no, arno_type, check_type, 
				cashier_receipt, pending_or, client_ledger_part, ref_rec_id, pay_for_lot
			) values (
				nextval('rf_pay_detail_id_seq'::regclass), v_payments.client_seqno, v_Entity_ID, v_payments.part_type, v_payments.bank, 
				v_payments.branch, v_payments.acct_no, v_payments.check_no, v_payments.check_date, v_payments.amount, 
				v_payments.guarantee, v_payments.receipt_no, v_payments.receipt_type, v_payments.status_id, v_payments.brstn, 
				v_payments.bank_remit, v_payments.inout_voucher, v_payments.ar_no, v_payments.arno_type, v_payments.check_type, 
				v_payments.cashier_receipt, v_payments.pending_or, v_payments.client_ledger_part, v_payments.ref_rec_id, v_payments.pay_for_lot
			);
			RAISE INFO 'Client Seq No: %', v_payments.client_seqno;   
		     --END IF;
		
		end if;

		
		
	END LOOP;

	PERFORM dblink_disconnect('dbPayHeader');
 
  RETURN TRUE;
  
END;
$BODY$;

ALTER FUNCTION public.sp_acquire_pay_detail_montalban(character varying, character varying, character varying, character varying, character varying)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION public.sp_acquire_pay_detail_montalban(character varying, character varying, character varying, character varying, character varying) TO PUBLIC;

GRANT EXECUTE ON FUNCTION public.sp_acquire_pay_detail_montalban(character varying, character varying, character varying, character varying, character varying) TO employee;

GRANT EXECUTE ON FUNCTION public.sp_acquire_pay_detail_montalban(character varying, character varying, character varying, character varying, character varying) TO postgres;

