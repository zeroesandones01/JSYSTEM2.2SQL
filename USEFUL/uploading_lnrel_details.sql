
--UPLOAD LOAN RELEASED AND RETFEE PAYMENTS
--- mf_sold_unit_new_itsreal
DO $$
DECLARE
   --also applicable for mf_sold_unit_new_itsreal
   v_Rf_Payments RECORD;
   v_Entity RECORD;
   v_Entity_ItsReal RECORD;
   
   v_Entity_ID VARCHAR;
   v_Count INTEGER;
   v_Pay_Rec_ID INTEGER;


BEGIN

		for v_Rf_Payments in (SELECT * 
							  FROM acerhomes.rf_payments where TRIM(entity_id) = '0000056444' AND TRIM(pay_part_id) IN ('087','218', '219', '246', '247')) loop
                               
			
			SELECT INTO v_Entity_ItsReal * FROM rf_entity_itsreal where TRIM(entity_id) = TRIM(v_Rf_Payments.entity_id) and TRIM(proj_server) = TRIM(v_Rf_Payments.proj_server);
		
            IF EXISTS (SELECT * FROM rf_entity where TRIM(first_name) = TRIM(v_Entity_ItsReal.first_name) and TRIM(middle_name) = TRIM(v_Entity_ItsReal.middle_name) and TRIM(last_name) = TRIM(v_Entity_ItsReal.last_name)) then
				v_Entity_ID := (SELECT entity_id FROM rf_entity where TRIM(first_name) = TRIM(v_Entity_ItsReal.first_name) and TRIM(middle_name) = TRIM(v_Entity_ItsReal.middle_name) and TRIM(last_name) = TRIM(v_Entity_ItsReal.last_name));
			END IF;
			
			IF TRIM(v_Rf_Payments.entity_id)::VARCHAR = '0000056444' then
				v_Entity_ID := '2669157452';
			end if;
		
			IF NULLIF(TRIM(v_Entity_ID), '') IS NOT NULL THEN
				v_Pay_Rec_ID := (SELECT nextval('rf_payments_rec_id_seq'));
				
				INSERT INTO public.rf_payments(entity_id, proj_id, pbl_id, seq_no, actual_date, trans_date, pay_part_id, pymnt_type, bank_id, 
											   bank_branch_id, amount, acct_no, check_no, check_date, check_stat_id, bounce_reason_id, or_no, 
											   or_date, ar_no, brstn, request_no, applied_amt, cancelled, remarks, branch_id, post_date, client_seqno, 
											   or_doc_id, pr_doc_id, status_id, wdraw_stat, date_wdrawn, wdraw_no, wdraw_reason, repl_wdraw_by, 
											   date_remitted, remit_batch, reversed, pay_rec_id, check_type, receipt_id, co_id, unit_id, total_ar_amt, 
											   created_by, date_created, refund_date, from_pay_rec_id, warehoused, proj_server, server_id, si_no, si_date, 
											   si_doc_id, itsreal_payrecid)
				VALUES (v_Entity_ID, TRIM(v_Rf_Payments.proj_id), TRIM(v_Rf_Payments.pbl_id), v_Rf_Payments.seq_no, v_Rf_Payments.actual_date, v_Rf_Payments.trans_date, 
						TRIM(v_Rf_Payments.pay_part_id), TRIM(v_Rf_Payments.pymnt_type), TRIM(v_Rf_Payments.bank_id), TRIM(v_Rf_Payments.bank_branch_id), v_Rf_Payments.amount, 
						TRIM(v_Rf_Payments.acct_no), TRIM(v_Rf_Payments.check_no), v_Rf_Payments.check_date, TRIM(v_Rf_Payments.check_stat_id), TRIM(v_Rf_Payments.bounce_reason_id), 
						TRIM(v_Rf_Payments.or_no), v_Rf_Payments.or_date, TRIM(v_Rf_Payments.ar_no), null, TRIM(v_Rf_Payments.request_no), v_Rf_Payments.applied_amt, v_Rf_Payments.cancelled, 
						TRIM(v_Rf_Payments.remarks), TRIM(v_Rf_Payments.branch_id), v_Rf_Payments.post_date, TRIM(v_Rf_Payments.client_seqno), TRIM(v_Rf_Payments.or_doc_id), 
						TRIM(v_Rf_Payments.pr_doc_id), TRIM(v_Rf_Payments.status_id), TRIM(v_Rf_Payments.wdraw_stat), v_Rf_Payments.date_wdrawn, TRIM(v_Rf_Payments.wdraw_no), 
						TRIM(v_Rf_Payments.wdraw_reason), TRIM(v_Rf_Payments.repl_wdraw_by), v_Rf_Payments.date_remitted, TRIM(v_Rf_Payments.remit_batch), v_Rf_Payments.reversed, 
						v_Pay_Rec_ID, TRIM(v_Rf_Payments.check_type), TRIM(v_Rf_Payments.receipt_id), TRIM(v_Rf_Payments.co_id), null, null, '900876', now(), 
						v_Rf_Payments.refund_date, null, null, TRIM(v_Rf_Payments.proj_server), TRIM(v_Rf_Payments.server_id), null, null, null, v_Rf_Payments.pay_rec_id::VARCHAR::INT);
				
			END IF;
			
		end loop;

END $$;