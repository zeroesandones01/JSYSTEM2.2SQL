

select sp_update_ma_sched_with_amt('2041770384', '004', '7162', 10, '2024-11-14');

select * from rf_client_schedule where entity_id = '2041770384' and scheddate::DATE = '2024-12-14';

select pay_rec_id, * from rf_payments where entity_id = '2041770384' order by pay_rec_id desc;

--update rf_client_schedule set amount = 16783.05, principal = 11516.29 where entity_id = '2041770384' and scheddate::DATE = '2024-12-14';

select sp_apply_ledger_again(788979, true, 'V2');

select get_client_name(a.entity_id), a.trans_date, a.or_doc_id, a.pr_doc_id, a.or_doc_id is not null,
(
	case
		when or_doc_id is not null
			then 'select sp_journalize_or_v2 (''' || a.entity_id || ''', ''' || COALESCE(a.proj_id, '') || ''', ''' || COALESCE(a.pbl_id, '') || ''', ''' || COALESCE(a.seq_no, 0) || ''', ''' || a.pay_rec_id || ''', ''' || a.created_by || ''');' 
		else 'select sp_journalize_ar (''' || coalescE(a.entity_id, '') || ''', ''' || coalescE(a.proj_id, '') || ''', ''' || coalescE(a.pbl_id, '') || ''', ' || coalescE(a.seq_no::varchar, 'null') || ', ''' || coalescE(a.co_id, '') || ''', ''' || coalescE(a.or_no, '') || ''', ''' || a.amount || ''', ''' || coalescE(a.pay_part_id, '') || ''', ''' || coalescE(a.pay_rec_id::varchar, '') || ''', ''' || coalescE(a.created_by, '') || ''');' 
	end
) AS "script"
from rf_payments a
where a.pay_rec_id IN (789208)
and not exists(select * from rf_crb_detail x where (x.rb_id = a.or_no or x.rb_id = a.ar_no) and x.pay_rec_id::int = a.pay_rec_id::int /*and x.status_id = 'A'*/)
and a.branch_id in ('01', '06', '10')
and date_part('year', a.trans_date) >= '2019'
and a.status_id != 'I'
and get_client_name(a.entity_id) NOT IN ('CENQHOMES DEVELOPMENT CORPORATION', 'ACERHOMES DEVELOPMENT CORPORATION', 'VERDANTPOINT  DEVELOPMENT CORPORATION', 'VERDANTPOINT  DEVELOPMENT CORPORATION');

