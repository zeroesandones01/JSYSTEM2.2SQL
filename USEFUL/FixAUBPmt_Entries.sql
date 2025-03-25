
DO $$
DECLARE
   
v_Loop RECORD;
v_TD VARCHAR;
v_Acct_ID VARCHAR;

BEGIN
	
	for v_Loop in (select get_client_name(a.entity_id), 
					get_merge_unit_desc_v3(a.entity_id, a.proj_id, a.pbl_id, a.seq_no) , a.actual_date::DATE, a.trans_date::dATE, coalesce(a.or_no, a.ar_no), b.particulars, a.amount, a.pay_rec_id
					, a.entity_id, a.proj_id, a.pbl_id, a.seq_no, a.co_id
					from rf_payments a
					LEFT JOIN mf_pay_particular b on b.pay_part_id = a.pay_part_id
					where a.remarks ~*'AUB Bills Payment'
					and a.trans_date::DATE BETWEEN '2025-02-01' and '2025-02-07'
					and exists (SELECT *
					  			FROM rf_crb_detail
								where pay_rec_id::INT = a.pay_rec_id
								and acct_id = '01-01-01-001'
								and status_id = 'A'
								and doc_id = '01')
					and a.co_id = '02'
					ORDER BY a.actual_date, coalesce(a.or_no, a.ar_no)) loop

		IF v_Loop.co_id = '' THEN

		END IF;

		IF v_Loop.co_id = '' THEN

		END IF;

		IF v_Loop.co_id = '' THEN

		END IF;


		UPDATE rf_crb_detail set acct_id = '01-01-04-074' where pay_rec_id::INT = v_Loop.pay_rec_id and acct_id = '01-01-01-001' and co_id = v_Loop.co_id and entity_id = v_Loop.entity_id and proj_id = v_Loop.proj_id and pbl_id = v_Loop.pbl_id and seq_no = v_Loop.seq_no and status_id = 'A';
	     

	end loop;

END $$;