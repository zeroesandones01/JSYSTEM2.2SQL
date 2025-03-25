-- FUNCTION: public.sp_acquire_pay_header_montalban(character varying, character varying, character varying, character varying, character varying)

-- DROP FUNCTION IF EXISTS public.sp_acquire_pay_header_montalban(character varying, character varying, character varying, character varying, character varying);

CREATE OR REPLACE FUNCTION public.sp_acquire_pay_header_montalban(
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

	FOR v_payments IN (SELECT * FROM dblink('dbPayHeader', concat('select * from rf_pay_header where date_created::DATE = ''',p_date,'''::DATE and status_id = ''A'' and branch_id = ''11'' order by client_seqno')) AS a
			(client_seqno character varying, 
			entity_id character varying, 
			branch_id character varying, 
			trans_date timestamp without time zone, 
			booking_date timestamp without time zone, 
			proj_id character varying, 
			pbl_id character varying, 
			seq_no bigint, 
			selling_price numeric, 
			mktgarm character varying, 
			selling_agent character varying, 
			type_id character varying, 
			pmt_scheme_id character varying, 
			type_of_sale character varying, 
			total_amt_paid numeric, 
			new_reserved character varying, 
			res_status character varying, 
			status_id character, 
			model_id character varying, 
			co_id character varying, 
			jv_no character varying, 
			created_by character varying, 
			date_created timestamp without time zone, 
			printed boolean, 
			rs_time_out character varying, 
			cs_time_out character varying, 
			chk_replacement boolean, 
			in_out_voucher boolean, 
			op_status character, 
			rs_term smallint, 
			dp_term smallint, 
			ma_term smallint, 
			res_amount numeric, 
			dp_rate numeric, 
			dp_amount numeric, 
			loan_availed numeric, 
			disc_rate numeric, 
			pn_no character varying, 
			unit_id character varying, 
			part_sequence smallint, 
			pay_header_id bigint, 
			request_no character varying,
			income_cluster character varying,
			disc_amt numeric, 
			credit_itsreal boolean, 
			batch_no character varying)) LOOP

		
		
		IF NOT EXISTS (SELECT * FROM rf_pay_header where client_seqno = v_payments.client_seqno) then

		    --IF v_payments.client_seqno = '060200213001' THEN

		    insert into rf_pay_header (
				client_seqno, entity_id, branch_id, trans_date, booking_date, 
				proj_id, pbl_id, seq_no, selling_price, mktgarm, 
				selling_agent, type_id, pmt_scheme_id, type_of_sale, total_amt_paid, 
				new_reserved, res_status, status_id, model_id, co_id, 
				jv_no, created_by, date_created, printed, rs_time_out, 
				cs_time_out, chk_replacement, in_out_voucher, op_status, rs_term, 
				dp_term, ma_term, res_amount, dp_rate, dp_amount, 
				loan_availed, disc_rate, pn_no, unit_id, part_sequence, 
				pay_header_id, request_no
			) values (
				v_payments.client_seqno, v_payments.entity_id, v_payments.branch_id, v_payments.trans_date, v_payments.booking_date, 
				v_payments.proj_id, v_payments.pbl_id, v_payments.seq_no, v_payments.selling_price, v_payments.mktgarm, 
				v_payments.selling_agent, v_payments.type_id, v_payments.pmt_scheme_id, v_payments.type_of_sale, v_payments.total_amt_paid, 
				v_payments.new_reserved, v_payments.res_status, v_payments.status_id, v_payments.model_id, v_payments.co_id, 
				v_payments.jv_no, v_payments.created_by, v_payments.date_created, v_payments.printed, v_payments.rs_time_out, 
				v_payments.cs_time_out, v_payments.chk_replacement, v_payments.in_out_voucher, v_payments.op_status, v_payments.rs_term, 
				v_payments.dp_term, v_payments.ma_term, v_payments.res_amount, v_payments.dp_rate, v_payments.dp_amount, 
				v_payments.loan_availed, v_payments.disc_rate, v_payments.pn_no, v_payments.unit_id, v_payments.part_sequence, 
				nextval('rf_pay_header_id_seq'::regclass), v_payments.request_no);

		RAISE INFO 'Client Seq No: %', v_payments.client_seqno;

			--END IF;
		end if;

		
		
	END LOOP;

	PERFORM dblink_disconnect('dbPayHeader');
 
  RETURN TRUE;
  
END;
$BODY$;

ALTER FUNCTION public.sp_acquire_pay_header_montalban(character varying, character varying, character varying, character varying, character varying)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION public.sp_acquire_pay_header_montalban(character varying, character varying, character varying, character varying, character varying) TO PUBLIC;

GRANT EXECUTE ON FUNCTION public.sp_acquire_pay_header_montalban(character varying, character varying, character varying, character varying, character varying) TO employee;

GRANT EXECUTE ON FUNCTION public.sp_acquire_pay_header_montalban(character varying, character varying, character varying, character varying, character varying) TO postgres;

