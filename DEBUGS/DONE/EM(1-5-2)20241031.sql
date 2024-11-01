		SELECT a.pay_rec_id, a.bank_id as pmt_bank, e.bank_id as bank_bank_id, *
		FROM rf_payments a
		INNER JOIN mf_pay_particular b ON b.pay_part_id = a.pay_part_id
		LEFT JOIN mf_office_branch c ON c.branch_id = a.branch_id		
		left JOIN mf_bank e ON e.bank_id = a.bank_id
		LEFT JOIN mf_bank_branch f ON f.bank_id = a.bank_id AND f.bank_branch_id = a.bank_branch_id
		LEFT JOIN rf_payments g ON (TRIM(a.receipt_id) = TRIM(g.or_no) OR TRIM(a.receipt_id) = TRIM(g.ar_no)) and g.status_id = 'A'
			/* 	Added by Mann2x; Date Added: May 18, 2017; Additional filter for a more precise row selection;	*/
			and (a.entity_id = g.entity_id and (case when a.pymnt_type = 'B' then a.check_no = g.check_no end))
		LEFT JOIN mf_check_status d ON d.checkstat_id = (case when g.check_stat_id is null then a.check_stat_id else g.check_stat_id end)
		WHERE a.entity_id = '7641188474' AND a.proj_id = '007' AND a.pbl_id = '323' AND a.seq_no = 1
		--AND (CASE WHEN $5 = 'I' AND $6 = FALSE THEN a.status_id = 'I' ELSE a.status_id = 'A' END) --REMOVED TO DISPLAY INACTIVE PAYMENTS FOR TRANSFER-REAPP 
		--AND a.status_id = 'A'
		AND a.status_id != 'X' -- ADDED BY MONIQUE DTD 7-26-2022; TO FILTER PAYMENTS THAT SHOULD NOT BE DISPLAYED
		AND COALESCE(a.remarks, '') !~*'Canceled Receipt'
		AND (a.pay_part_id != '203' and a.pay_part_id != '185')
		and case when a.entity_id in ('3436559580') then COALESCE(a.amount, 0) - COALESCE(a.applied_amt, 0) != 0 else true end
		AND COALESCE(a.client_seqno, '') NOT IN ('010171030008', '010171030011', '010171030009', '010201009030', '010201015009')
		AND a.pay_rec_id not in (36076, 38373,39814,43026,43027,43028, 92673)
		
		ORDER BY (case when a.pay_part_id = '260' then a.trans_date else a.actual_date::DATE end) ,b.apply_order, COALESCE(get_ledger_apply_date(a.pay_rec_id), a.check_date),a.amount desc, a.pay_rec_id

BEGIN
--update rf_payments set bank_id = '51' where pay_rec_id = 782281

COMMIT


select * from mf_bank where TRIM(bank_id) = '51';

SELECT * FROM rf_entity where entity_id = '0308496748';
