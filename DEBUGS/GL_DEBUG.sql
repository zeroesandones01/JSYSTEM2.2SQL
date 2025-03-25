
select * from view_gen_ledger_detailed_includeactive_v4_debug_erick('11-01-00-000','02', 'Mon Jan 01 00:00:00 PST 2024', 'Tue Dec 31 00:00:00 PST 2024', '', '', 'P', '',null,null);

select * from rf_crb_detail limit 50;

select status_id from rf_crb_header group by status_id;

select sum(a.trans_amt) from rf_crb_detail a 
						where a.status_id = 'A' 
						and a.co_id = '02' 
						and a.trans_amt > 0
						and trim(a.acct_id) = '11-01-00-000'
						and a.rb_fiscal_year < 2024
						and exists (SELECT *
									FROM rf_crb_header 
									where pay_rec_id = a.pay_rec_id
									and rb_id = a.rb_id
									and co_id = a.co_id
									and status_id = 'P')


select * from rf_crb_detail a 
						where a.status_id = 'A' 
						and a.co_id = '02' 
						and a.trans_amt > 0
						and trim(a.acct_id) = '11-01-00-000'
						and a.rb_fiscal_year < 2024
						and exists (SELECT *
									FROM rf_crb_header 
									where pay_rec_id = a.pay_rec_id
									and rb_id = a.rb_id
									and co_id = a.co_id
									and status_id = 'A')

SELECT * FROM rf_crb_header where pay_rec_id::int IN (10716, 49212);

SELECT get_client_name(entity_id), * from rf_payments where pay_rec_id IN (10716, 49212);

select * from rf_crb_detail limit 1;
SELECT * FROM rf_crb_header limit 1;

select * 
from rf_crb_header a
where a.issued_date::DATE < '2024-01-01'
AND a.co_id = '02'
and a.status_id = 'A'
and (SELECT sum(trans_amt)
	 from rf_crb_detail
	 where pay_rec_id = a.pay_rec_id
	 and rb_id = a.rb_id
	 AND doc_id = a.doc_id
	 and co_id = a.co_id
	 and acct_id = '11-01-00-000'
	 and status_id = 'A') != 0


select a.pay_rec_id, * 
from rf_crb_detail a
where a.rb_fiscal_year < 2024
AND a.co_id = '02'
and a.status_id = 'A'
--and a.trans_amt > 0
and a.acct_id = '11-01-00-000'
and (SELECT sum(trans_amt)
	 from rf_crb_detail
	 where pay_rec_id = a.pay_rec_id
	 and rb_id = a.rb_id
	 AND doc_id = a.doc_id
	 and co_id = a.co_id
	 --and acct_id = '11-01-00-000'
	 and status_id = 'A') != 0





select * 
from rf_crb_header a
where a.issued_date::DATE < '2024-01-01'
AND a.co_id = '02'
and a.status_id = 'P'
and (SELECT sum(trans_amt)
	 from rf_crb_detail
	 where pay_rec_id = a.pay_rec_id
	 and rb_id = a.rb_id
	 AND doc_id = a.doc_id
	 and co_id = a.co_id
	 and acct_id = '11-01-00-000'
	 and status_id = 'A') != 0