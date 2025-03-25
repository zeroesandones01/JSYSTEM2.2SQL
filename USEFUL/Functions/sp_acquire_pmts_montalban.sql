-- FUNCTION: public.sp_acquire_pmts_montalban(character varying, character varying, character varying, character varying, character varying)

-- DROP FUNCTION IF EXISTS public.sp_acquire_pmts_montalban(character varying, character varying, character varying, character varying, character varying);

CREATE OR REPLACE FUNCTION public.sp_acquire_pmts_montalban(
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
v_Entity_ID varchar;
v_Pay_Part_ID varchar;
v_pay_rec_id integer;
v_DOW integer;
v_Actual_Date timestamp without time zone;
v_Trans_Date Timestamp without time zone;  

BEGIN

	v_DOW  := (select extract(isodow from p_date::Date));
	
	PERFORM dblink_connect('dbPayments', 'host='||p_host||' user='||p_user||' password='||p_password||' dbname='||p_database||'');

	FOR v_payments IN (
		SELECT * FROM dblink('dbPayments', concat('select * from rf_payments where date_created::DATE = ''',p_date,'''::date and status_id = ''A'' and branch_id = ''11'' order by pay_rec_id')) AS a
		(entity_id character varying, 
			proj_id character varying, 
			pbl_id character varying, 
			seq_no integer, 
			actual_date timestamp without time zone, 
			trans_date timestamp without time zone, 
			pay_part_id character varying, 
			pymnt_type character, 
			bank_id character varying, 
			bank_branch_id character varying, 
			amount numeric, 
			acct_no character varying, 
			check_no character varying, 
			check_date timestamp without time zone, 
			check_stat_id character varying, 
			bounce_reason_id character varying, 
			or_no character varying, 
			or_date timestamp without time zone, 
			ar_no character varying, 
			brstn character varying, 
			request_no character varying, 
			applied_amt numeric, 
			cancelled boolean, 
			remarks character varying, 
			branch_id character varying, 
			post_date timestamp without time zone, 
			client_seqno character varying, 
			or_doc_id character varying, 
			pr_doc_id character varying, 
			status_id character varying, 
			wdraw_stat character, 
			date_wdrawn timestamp without time zone, 
			wdraw_no character varying, 
			wdraw_reason character varying, 
			repl_wdraw_by character varying, 
			date_remitted timestamp without time zone, 
			remit_batch character varying, 
			reversed boolean, 
			pay_rec_id integer, 
			check_type character varying, 
			receipt_id character varying, 
			co_id character varying, 
			unit_id character varying, 
			total_ar_amt numeric, 
			created_by character varying, 
			date_created timestamp without time zone, 
			refund_date timestamp without time zone, 
			from_pay_rec_id integer)) LOOP

	v_Entity_ID := (SELECT entity_id from rf_pay_header where client_seqno = v_payments.client_seqno);

	if not exists (select * from rf_payments where entity_id = v_Entity_ID and proj_id = v_payments.proj_id and pbl_id = v_payments.pbl_id and seq_no = v_payments.seq_no and pay_part_id = v_payments.pay_part_id and client_seqno = v_payments.client_seqno and status_id = 'A') then

	   --IF  v_payments.client_seqno = '060200213001' THEN
           v_pay_rec_id := (select nextval('rf_payments_rec_id_seq'));

	   /*if v_DOW = 6 then
		   v_Actual_Date := v_payments.date_created + '1 days'::interval;
		   v_Trans_Date  := v_payments.date_created + '1 days'::interval;
	   else*/
		   v_Actual_Date := v_payments.actual_date;
		   v_Trans_Date  := v_payments.trans_date;
	   --end if;
           
	   insert into rf_payments (
				entity_id, proj_id, pbl_id, seq_no, actual_date, 
				trans_date, pay_part_id, pymnt_type, bank_id, bank_branch_id, 
				amount, acct_no, check_no, check_date, check_stat_id, 
				bounce_reason_id, or_no, or_date, ar_no, brstn, 
				request_no, applied_amt, cancelled, remarks, branch_id, 
				post_date, client_seqno, or_doc_id, pr_doc_id, status_id, 
				wdraw_stat, date_wdrawn, wdraw_no, wdraw_reason, repl_wdraw_by, 
				date_remitted, remit_batch, reversed, pay_rec_id, check_type, 
				receipt_id, co_id, unit_id, total_ar_amt, created_by, date_created, 
				refund_date, from_pay_rec_id
			) values (
				v_Entity_ID, v_payments.proj_id, v_payments.pbl_id, v_payments.seq_no, v_Actual_Date, 
				v_Trans_Date, v_payments.pay_part_id, v_payments.pymnt_type, v_payments.bank_id, v_payments.bank_branch_id, 
				v_payments.amount, v_payments.acct_no, v_payments.check_no, v_payments.check_date, v_payments.check_stat_id, 
				v_payments.bounce_reason_id, v_payments.or_no, v_payments.or_date, v_payments.ar_no, v_payments.brstn, 
				v_payments.request_no, v_payments.applied_amt, v_payments.cancelled, v_payments.remarks, v_payments.branch_id, 
				v_payments.post_date, v_payments.client_seqno, v_payments.or_doc_id, v_payments.pr_doc_id, v_payments.status_id, 
				v_payments.wdraw_stat, v_payments.date_wdrawn, v_payments.wdraw_no, v_payments.wdraw_reason, v_payments.repl_wdraw_by, 
				v_payments.date_remitted, v_payments.remit_batch, v_payments.reversed, v_pay_rec_id, v_payments.check_type, 
				v_payments.receipt_id, v_payments.co_id, v_payments.unit_id, v_payments.total_ar_amt, v_payments.created_by, 
				v_payments.date_created, v_payments.refund_date, v_payments.from_pay_rec_id
			);

			IF v_payments.or_doc_id = '01' THEN
				PERFORM sp_journalize_or_v2 (v_Entity_ID, v_payments.proj_id, v_payments.pbl_id, v_payments.seq_no, v_pay_rec_id, v_payments.created_by);
			END IF;

			IF v_payments.or_doc_id = '03' THEN
			
			PERFORM sp_journalize_ar(v_Entity_ID, v_payments.proj_id, v_payments.pbl_id, v_payments.seq_no, '02', v_payments.or_no, v_payments.amount, v_payments.pay_part_id, v_pay_rec_id, v_payments.created_by);

			END IF;
			
           RAISE INFO 'Client Seq No: %', v_payments.client_seqno;   
	   --END IF;
	end if;
	
	END LOOP;

	PERFORM dblink_disconnect('dbPayments');
 
  RETURN TRUE;
  
END;
$BODY$;

ALTER FUNCTION public.sp_acquire_pmts_montalban(character varying, character varying, character varying, character varying, character varying)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION public.sp_acquire_pmts_montalban(character varying, character varying, character varying, character varying, character varying) TO PUBLIC;

GRANT EXECUTE ON FUNCTION public.sp_acquire_pmts_montalban(character varying, character varying, character varying, character varying, character varying) TO employee;

GRANT EXECUTE ON FUNCTION public.sp_acquire_pmts_montalban(character varying, character varying, character varying, character varying, character varying) TO postgres;

