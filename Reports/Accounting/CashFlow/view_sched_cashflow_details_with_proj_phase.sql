-- FUNCTION: public.view_sched_cashflow_details_with_proj_phase(character varying, character varying, character varying, timestamp without time zone, timestamp without time zone)

-- DROP FUNCTION IF EXISTS public.view_sched_cashflow_details_with_proj_phase(character varying, character varying, character varying, timestamp without time zone, timestamp without time zone);

CREATE OR REPLACE FUNCTION public.view_sched_cashflow_details_with_proj_phase(
	p_co_id character varying,
	p_proj_id character varying,
	p_phase_no character varying,
	p_date_from timestamp without time zone,
	p_date_to timestamp without time zone)
    RETURNS TABLE(c_cf_type character varying, c_acct_desc character varying, c_inflow_type character varying, c_particulars character varying, c_pv_no character varying, c_cv_no character varying, c_date_paid timestamp without time zone, c_pv_amt numeric, c_acct_id character varying, c_amount numeric, c_div character varying, c_dept character varying, c_proj character varying, c_phase character varying, c_check_no character varying, c_status character varying) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE

  v_rec RECORD;
 --ORIGINAL CODE FROM view_sched_cashflow_details_with_proj_phase
  v_CV_Header RECORD;
BEGIN

  FOR v_rec IN (
-------------------------------BEGIN SQL

/***********************START OF NOT YET DEPOSITED CASH INFLOW*********************************/
select A.*, B.check_no from (

select 'CASH INFLOW' as cf_type, e.acct_name as acct_desc, 'Buyers Payment' as inflow_type, b.partdesc as particulars, 
'' as pv_no, '' as cv_no, null::date as date_paid, 0 as pv_amt,
'01-01-01-000' as acct_id, a.amount, '' as div, '' as dept, proj_alias as proj, phase as phase

	from (

		--PAYMENTS FROM RF_PAYMENTS (CASH COLLECTION FOR THE END OF PERIOD)
		select a.pay_part_id, a.trans_date, a.amount, c.proj_alias, b.phase
			from rf_payments a
			left join mf_unit_info b on a.proj_id = b.proj_id and a.pbl_id = b.pbl_id
			left join mf_project c on a.proj_id = c.proj_id
			where a.status_id != 'I'
			and a.trans_date::date in (select cash_date from cs_dp_header where dep_no in (select dep_no from cs_dp_csh_detail where status_id != 'I')
				and cash_date::date >= p_date_from and cash_date::date <= p_date_to and dep_date::date >= p_date_to)
			and a.receipt_id is null
			and coalesce(request_no,'') = ''
			and a.co_id = p_co_id
			and (case when p_proj_id = '' then true else a.proj_id = p_proj_id end)
			and (case when p_phase_no = '' then true else b.sub_proj_id = p_phase_no end) 
			and a.pymnt_type = 'A'
			and a.pay_part_id in ('168','033','163','042','040','041','106','203')
							
		union all
		
		--PAYMENTS FROM RF_TRA_DETAIL (CASH COLLECTION FOR THE END OF PERIOD)
		select a.part_id, a.tran_date, a.amount , d.proj_alias, c.phase
			from rf_tra_detail a 
			join (select distinct on (client_seqno) * from rf_tra_header where co_id = p_co_id) b  on a.client_seqno = b.client_seqno
			left join mf_unit_info c on b.proj_id = c.proj_id and b.pbl_id = c.pbl_id
			left join mf_project d on b.proj_id = d.proj_id
			where a.status_id != 'I'
			and a.tran_date::date in (select cash_date from cs_dp_header where dep_no in (select dep_no from cs_dp_csh_detail where status_id != 'I')
				and cash_date::date >= p_date_from and cash_date::date <= p_date_to and dep_date::date >= p_date_to)
			and a.receipt_no is not null
			and (case when p_proj_id = '' then true else b.proj_id = p_proj_id end)
			and (case when p_phase_no = '' then true else c.sub_proj_id = p_phase_no end) 
			and bank is null
			and (case when check_no is not null then checkstat_id in ('01','02') else true end)
			and part_id in ('168','033','163','042','040','041','106','203')
		
		
	) a 
	left join mf_pay_particular b on a.pay_part_id = b.pay_part_id
	left join (select distinct on (cash_date) * from cs_dp_header) c on a.trans_date::date = c.cash_date::date
	left join mf_bank_account d on c.bank_acct_id = d.bank_acct_id
	left join mf_boi_chart_of_accounts e on d.acct_id = e.acct_id
	

UNION ALL

select 'CASH INFLOW' as cf_type, e.acct_name, 'Collection from Third Parties' as inflow_type, b.partdesc, 
'' as pv_no, '' as cv_no, null::date as date_paid, 0 as pv_amt,
'01-01-01-000', a.amount, '' as div, '' as dept, proj_alias as proj, phase as phase
	from (  

		--PAYMENTS FROM RF_PAYMENTS (CASH COLLECTION FOR THE END OF PERIOD)
		select a.pay_part_id, a.trans_date, a.amount, c.proj_alias, b.phase
			from rf_payments a
			left join mf_unit_info b on a.proj_id = b.proj_id and a.pbl_id = b.pbl_id
			left join mf_project c on a.proj_id = c.proj_id
			where a.status_id != 'I'
			and a.trans_date::date in (select cash_date from cs_dp_header where dep_no in (select dep_no from cs_dp_csh_detail where status_id != 'I')
				and cash_date::date >= p_date_from and cash_date::date <= p_date_to and dep_date::date >= p_date_to)
			and a.receipt_id is null
			and coalesce(request_no,'') = ''
			and a.co_id = p_co_id
			and (case when p_proj_id = '' then true else a.proj_id = p_proj_id end)
			and (case when p_phase_no = '' then true else b.sub_proj_id = p_phase_no end) 
			and a.pymnt_type = 'A'
			and a.pay_part_id in ('166','180','187','185','197','220','178','182' )	
					
		union all
		
		--PAYMENTS FROM RF_TRA_DETAIL (CASH COLLECTION FOR THE END OF PERIOD)
		select a.part_id, a.tran_date, a.amount , d.proj_alias, c.phase
			from rf_tra_detail a 
			join (select distinct on (client_seqno) * from rf_tra_header where co_id = p_co_id) b  on a.client_seqno = b.client_seqno
			left join mf_unit_info c on b.proj_id = c.proj_id and b.pbl_id = c.pbl_id
			left join mf_project d on b.proj_id = d.proj_id
			where a.status_id != 'I'
			and a.tran_date::date in (select cash_date from cs_dp_header where dep_no in (select dep_no from cs_dp_csh_detail where status_id != 'I')
				and cash_date::date >= p_date_from and cash_date::date <= p_date_to and dep_date::date >= p_date_to)
			and a.receipt_no is not null
			and (case when p_proj_id = '' then true else b.proj_id = p_proj_id end)
			and (case when p_phase_no = '' then true else c.sub_proj_id = p_phase_no end) 
			and bank is null
			and (case when check_no is not null then checkstat_id in ('01','02') else true end)
			and part_id in ('166','180','187','185','197','220','178','182' )	
					
	) a 
	left join mf_pay_particular b on a.pay_part_id = b.pay_part_id
	left join (select distinct on (cash_date) * from cs_dp_header) c on a.trans_date::date = c.cash_date::date
	left join mf_bank_account d on c.bank_acct_id = d.bank_acct_id
	left join mf_boi_chart_of_accounts e on d.acct_id = e.acct_id

UNION ALL

select 'CASH INFLOW' as cf_type, e.acct_name, 'Miscellaneous' as inflow_type, b.partdesc, 
'' as pv_no, '' as cv_no, null::date as date_paid, 0 as pv_amt,
'01-01-01-000', a.amount, '' as div, '' as dept, proj_alias as proj, phase as phase
	from (  

		--PAYMENTS FROM RF_PAYMENTS (CASH COLLECTION FOR THE END OF PERIOD)			
		select a.pay_part_id, a.trans_date, a.amount, c.proj_alias, b.phase
			from rf_payments a
			left join mf_unit_info b on a.proj_id = b.proj_id and a.pbl_id = b.pbl_id
			left join mf_project c on a.proj_id = c.proj_id
			where a.status_id != 'I'
			and a.trans_date::date in (select cash_date from cs_dp_header where dep_no in (select dep_no from cs_dp_csh_detail where status_id != 'I')
				and cash_date::date >= p_date_from and cash_date::date <= p_date_to and dep_date::date >= p_date_to)
			and a.receipt_id is null
			and coalesce(request_no,'') = ''
			and a.co_id = p_co_id
			and (case when p_proj_id = '' then true else a.proj_id = p_proj_id end)
			and (case when p_phase_no = '' then true else b.sub_proj_id = p_phase_no end) 
			and a.pymnt_type = 'A'
			and a.pay_part_id not in ('166','180','187','185','197','220','178','182','168','033','163','042','040','041','106','203')
				
		union all
		
		--PAYMENTS FROM RF_TRA_DETAIL (CASH COLLECTION FOR THE END OF PERIOD)
		select a.part_id, a.tran_date, a.amount , d.proj_alias, c.phase
			from rf_tra_detail a 
			join (select distinct on (client_seqno) * from rf_tra_header where co_id = p_co_id) b  on a.client_seqno = b.client_seqno
			left join mf_unit_info c on b.proj_id = c.proj_id and b.pbl_id = c.pbl_id
			left join mf_project d on b.proj_id = d.proj_id
			where a.status_id != 'I'
			and a.tran_date::date in (select cash_date from cs_dp_header where dep_no in (select dep_no from cs_dp_csh_detail where status_id != 'I')
				and cash_date::date >= p_date_from and cash_date::date <= p_date_to and dep_date::date >= p_date_to)
			and a.receipt_no is not null
			and (case when p_proj_id = '' then true else b.proj_id = p_proj_id end)
			and (case when p_phase_no = '' then true else c.sub_proj_id = p_phase_no end) 
			and a.bank is null
			and (case when a.check_no is not null then checkstat_id in ('01','02') else true end)
			and a.part_id not in ('166','180','187','185','197','220','178','182','168','033','163','042','040','041','106','203')		

		
	) a 
	left join mf_pay_particular b on a.pay_part_id = b.pay_part_id
	left join (select distinct on (cash_date) * from cs_dp_header) c on a.trans_date::date = c.cash_date::date
	left join mf_bank_account d on c.bank_acct_id = d.bank_acct_id
	left join mf_boi_chart_of_accounts e on d.acct_id = e.acct_id

/***********************END OF NOT YET DEPOSITED CASH INFLOW*********************************/

UNION ALL

/***********************START OF DEPOSITED CASH INFLOW*********************************/

select 'CASH INFLOW' as cf_type, e.acct_name, 'Buyers Payment' as inflow_type, b.partdesc, 
'' as pv_no, '' as cv_no, null::date as date_paid, 0 as pv_amt,
a.acct_id, a.amount, '' as div, '' as dept, proj_alias as proj, phase as phase
	from (

		--PAYMENTS FROM RF_PAYMENTS (CASH / DATED CHECK / PDC-DUE : WITHIN THE PERIOD)			
		select a.pay_part_id, a.trans_date, a.amount, c.proj_alias, b.phase,
			(case when b.sub_proj_id = '001' and coalesce(receipt_id,'') != 'MBTC' then '01-01-04-054' else 
				case when b.sub_proj_id = '002' and coalesce(receipt_id,'') != 'MBTC' then '01-01-04-057' else 
				case when a.receipt_id = 'MBTC' then '01-01-04-056' else '01-01-01-000' 
			end end end) acct_id
			from rf_payments a
			left join mf_unit_info b on a.proj_id = b.proj_id and a.pbl_id = b.pbl_id
			left join mf_project c on a.proj_id = c.proj_id
			where a.status_id != 'I'
			and a.trans_date::date >= p_date_from and a.trans_date::date <=  p_date_to 
			and coalesce(request_no,'') = ''
			and (coalesce(receipt_id,'') = '' or coalesce(receipt_id,'') = 'MBTC')
			and a.co_id = p_co_id
			and (case when p_proj_id = '' then true else a.proj_id = p_proj_id end)
			and (case when p_phase_no = '' then true else trim(b.sub_proj_id) = p_phase_no end) 
			and (case when a.pymnt_type = 'A' then a.trans_date::date not in (select cash_date from cs_dp_header where dep_no in (select dep_no 
				from cs_dp_csh_detail where status_id != 'I') 
				and cash_date::date >= p_date_from and cash_date::date <= p_date_to 
				and dep_date::date >= p_date_to) else true end)
			and (case when a.pymnt_type = 'B' then a.pay_rec_id in (select pay_rec_id from cs_dp_chk_detail where dep_no in (select distinct on (dep_no) dep_no from 			    cs_dp_header where post_date::date >= p_date_from and post_date::date <= p_date_to and status_id != 'I')) 
				else true end)
			and a.pay_part_id in ('168','033','163','042','040','041','106','203')
			and (case when a.check_no is not null then a.pay_rec_id in (select pay_rec_id::int 
				from rf_check_history where new_checkstat_id = '05' and status_id != 'I' and trans_date::date >= p_date_from 
				and trans_date::date <= p_date_to) else true end)
		
		
		union all
		
		--PAYMENTS FROM RF_PAYMENTS (PDC-DUE : OUTSIDE THE PERIOD)
		select a.pay_part_id, a.trans_date, a.amount, c.proj_alias, b.phase,
			(case when b.sub_proj_id = '001' then '01-01-04-054' else 
			case when b.sub_proj_id = '002' then '01-01-04-057' else '01-01-01-000' end end) acct_id
			from rf_payments a
			left join mf_unit_info b on a.proj_id = b.proj_id and a.pbl_id = b.pbl_id
			left join mf_project c on a.proj_id = c.proj_id
			where a.status_id != 'I'
			and a.trans_date::date < p_date_from 
			and a.co_id = p_co_id
			and (case when p_proj_id = '' then true else a.proj_id = p_proj_id end)
			and (case when p_phase_no = '' then true else trim(b.sub_proj_id) = p_phase_no end) 
			and a.pay_rec_id in (select pay_rec_id from cs_dp_chk_detail where dep_no in (select distinct on (dep_no) dep_no from cs_dp_header 
				where post_date::date >= p_date_from and post_date::date <= p_date_to and status_id != 'I'))
			and coalesce(request_no,'') = ''
			and (coalesce(receipt_id,'') = '' or coalesce(receipt_id,'') = 'MBTC')
			and a.check_date::date > get_next_bank_day(a.actual_date::date) 
			and a.pay_part_id in ('168','033','163','042','040','041','106','203')
			and (case when a.check_no is not null then a.pay_rec_id in (select pay_rec_id::int from rf_check_history 
				where new_checkstat_id = '05' and status_id != 'I' and trans_date::date >= p_date_from 
				and trans_date::date <= p_date_to) else true end)			
		
		union all
		
		--PAYMENTS FROM RF_TRA_DETAIL (CASH / DATED CHECK / PDC-DUE : WITHIN THE PERIOD)
		select a.part_id, a.tran_date, a.amount, d.proj_alias, c.phase,
			(case when c.sub_proj_id = '001' then '01-01-04-054' else 
			case when c.sub_proj_id = '002' then '01-01-04-057' else '01-01-01-000' end end) acct_id
			from rf_tra_detail a 
			join (select distinct on (client_seqno) * from rf_tra_header where co_id = p_co_id) b  on a.client_seqno = b.client_seqno
			left join mf_unit_info c on b.proj_id = c.proj_id and b.pbl_id = c.pbl_id
			left join mf_project d on b.proj_id = d.proj_id
			where a.status_id != 'I'
			and (case when p_proj_id = '' then true else b.proj_id = p_proj_id end)
			and (case when p_phase_no = '' then true else trim(c.sub_proj_id) = p_phase_no end) 
			and a.tran_date::date >= p_date_from and a.tran_date::date <=  p_date_to 
			and (case when check_no is null then tran_date::date not in (select cash_date from cs_dp_header 
				where dep_no in (select dep_no from cs_dp_csh_detail where status_id != 'I') and cash_date::date >= p_date_from 
				and cash_date::date <= p_date_to and dep_date::date >= p_date_to) else true end)
			and a.receipt_no is not null
			and (case when a.check_no is not null then checkstat_id in ('05') else true end)
			and a.part_id in ('168','033','163','042','040','041','106','203')

		union all
		
		--PAYMENTS FROM RF_TRA_DETAIL (PDC-DUE : OUTSIDE THE PERIOD)
		select a.part_id, a.tran_date, a.amount, d.proj_alias, c.phase,
			(case when c.sub_proj_id = '001' then '01-01-04-054' else 
			case when c.sub_proj_id = '002' then '01-01-04-057' else '01-01-01-000' end end) acct_id
			from rf_tra_detail a 
			join (select distinct on (client_seqno) * from rf_tra_header where co_id = p_co_id) b  on a.client_seqno = b.client_seqno
			left join mf_unit_info c on b.proj_id = c.proj_id and b.pbl_id = c.pbl_id
			left join mf_project d on b.proj_id = d.proj_id
			where a.status_id != 'I'
			and a.tran_date::date < p_date_from 
			and a.check_date::date >= p_date_from and a.check_date::date <=  p_date_to 
			and a.receipt_no is not null
			and (case when p_proj_id = '' then true else b.proj_id = p_proj_id end)
			and (case when p_phase_no = '' then true else trim(c.sub_proj_id) = p_phase_no end) 
			and check_no is not null
			and a.check_date::date > get_next_bank_day(a.actual_date::date) 
			and (case when a.check_no is not null then checkstat_id in ('05') else true end)
			and a.part_id in ('168','033','163','042','040','041','106','203')
		
	) a 
	left join mf_pay_particular b on a.pay_part_id = b.pay_part_id
	left join (select distinct on (cash_date) * from cs_dp_header) c on a.trans_date::date = c.cash_date::date
	left join mf_bank_account d on c.bank_acct_id = d.bank_acct_id
	left join mf_boi_chart_of_accounts e on a.acct_id = e.acct_id
	

UNION ALL

select 'CASH INFLOW' as cf_type, e.acct_name, 'Collection from Third Parties' as inflow_type, b.partdesc, 
'' as pv_no, '' as cv_no, null::date as date_paid, 0 as pv_amt,
a.acct_id, a.amount, '' as div, '' as dept, proj_alias as proj, phase as phase
	from (  

		--PAYMENTS FROM RF_PAYMENTS (CASH / DATED CHECK / PDC-DUE : WITHIN THE PERIOD)
		select a.pay_part_id, a.trans_date, a.amount, c.proj_alias, b.phase,
			(case when b.sub_proj_id = '001' then '01-01-04-054' else 
			case when b.sub_proj_id = '002' then '01-01-04-057' else '01-01-01-000' end end) acct_id
			from rf_payments a
			left join mf_unit_info b on a.proj_id = b.proj_id and a.pbl_id = b.pbl_id
			left join mf_project c on a.proj_id = c.proj_id
			where a.status_id != 'I'
			and a.trans_date::date >= p_date_from and a.trans_date::date <=  p_date_to 
			and coalesce(request_no,'') = ''
			and (coalesce(receipt_id,'') = '' or coalesce(receipt_id,'') = 'MBTC')
			and a.co_id = p_co_id
			and (case when p_proj_id = '' then true else a.proj_id = p_proj_id end)
			and (case when p_phase_no = '' then true else trim(b.sub_proj_id) = p_phase_no end) 
			and (case when a.pymnt_type = 'B' then a.pay_rec_id in (select pay_rec_id from cs_dp_chk_detail where dep_no in (select distinct on (dep_no) dep_no from 			    cs_dp_header where post_date::date >= p_date_from and post_date::date <= p_date_to and status_id != 'I')) 
				else true end)
			and a.pay_part_id in ('166','180','187','185','197','220','178','182' )
			and (case when a.pymnt_type = 'A' then a.trans_date::date not in (select cash_date from cs_dp_header 
				where dep_no in (select dep_no from cs_dp_csh_detail where status_id != 'I') and cash_date::date >= p_date_from 
				and cash_date::date <= p_date_to and dep_date::date >= p_date_to) else true end)
			and (case when a.check_no is not null then a.pay_rec_id in (select pay_rec_id::int from rf_check_history 
				where new_checkstat_id = '05' and status_id != 'I' and trans_date::date >= p_date_from 
				and trans_date::date <= p_date_to) else true end)
		
		union all
		
		--PAYMENTS FROM RF_PAYMENTS (PDC-DUE : OUTSIDE THE PERIOD)
		select a.pay_part_id, a.trans_date, a.amount, c.proj_alias, b.phase,
			(case when b.sub_proj_id = '001' then '01-01-04-054' else 
			case when b.sub_proj_id = '002' then '01-01-04-057' else '01-01-01-000' end end) acct_id
			from rf_payments a
			left join mf_unit_info b on a.proj_id = b.proj_id and a.pbl_id = b.pbl_id
			left join mf_project c on a.proj_id = c.proj_id
			where a.status_id != 'I'
			and a.trans_date::date < p_date_from 
			and a.pay_rec_id in (select pay_rec_id from cs_dp_chk_detail where dep_no in (select distinct on (dep_no) dep_no from cs_dp_header 
				where post_date::date >= p_date_from and post_date::date <= p_date_to and status_id != 'I'))
			and coalesce(request_no,'') = ''
			and (coalesce(receipt_id,'') = '' or coalesce(receipt_id,'') = 'MBTC')
			and a.co_id = p_co_id
			and (case when p_proj_id = '' then true else a.proj_id = p_proj_id end)
			and (case when p_phase_no = '' then true else trim(b.sub_proj_id) = p_phase_no end) 
			and a.check_date::date > get_next_bank_day(a.actual_date::date) 
			and a.pay_part_id in ('166','180','187','185','197','220','178','182' )
			and (case when a.check_no is not null then pay_rec_id in (select pay_rec_id::int 
				from rf_check_history where new_checkstat_id = '05' and status_id != 'I' 
				and trans_date::date >= p_date_from and trans_date::date <= p_date_to) else true end)
		
		union all
		
		--PAYMENTS FROM RF_TRA_DETAIL (CASH / DATED CHECK / PDC-DUE : WITHIN THE PERIOD)
		select a.part_id, a.tran_date, a.amount, d.proj_alias, c.phase,
			(case when c.sub_proj_id = '001' then '01-01-04-054' else 
			case when c.sub_proj_id = '002' then '01-01-04-057' else '01-01-01-000' end end) acct_id
			from rf_tra_detail a 
			join (select distinct on (client_seqno) * from rf_tra_header where co_id = p_co_id) b  on a.client_seqno = b.client_seqno
			left join mf_unit_info c on b.proj_id = c.proj_id and b.pbl_id = c.pbl_id
			left join mf_project d on b.proj_id = d.proj_id
			where a.status_id != 'I'
			and a.tran_date::date >= p_date_from and a.tran_date::date <=  p_date_to 
			and a.receipt_no is not null
			and (case when p_proj_id = '' then true else b.proj_id = p_proj_id end)
			and (case when p_phase_no = '' then true else trim(c.sub_proj_id) = p_phase_no end) 
			and (case when a.check_no is not null then a.checkstat_id in ('05') else true end)
			and a.part_id in ('166','180','187','185','197','220','178','182' )
			and (case when a.check_no is null  then a.tran_date::date not in (select cash_date 
				from cs_dp_header where dep_no in (select dep_no from cs_dp_csh_detail where status_id != 'I') 
				and cash_date::date >= p_date_from and cash_date::date <= p_date_to and dep_date::date >= p_date_to) else true end)

		union all
		
		--PAYMENTS FROM RF_TRA_DETAIL (PDC-DUE : OUTSIDE THE PERIOD)
		select a.part_id, a.tran_date, a.amount, d.proj_alias, c.phase,
			(case when c.sub_proj_id = '001' then '01-01-04-054' else 
			case when c.sub_proj_id = '002' then '01-01-04-057' else '01-01-01-000' end end) acct_id
			from rf_tra_detail a 
			join (select distinct on (client_seqno) * from rf_tra_header where co_id = p_co_id) b  on a.client_seqno = b.client_seqno
			left join mf_unit_info c on b.proj_id = c.proj_id and b.pbl_id = c.pbl_id
			left join mf_project d on b.proj_id = d.proj_id
			where a.status_id != 'I'
			and (case when p_proj_id = '' then true else b.proj_id = p_proj_id end)
			and (case when p_phase_no = '' then true else trim(c.sub_proj_id) = p_phase_no end) 
			and a.tran_date::date < p_date_from 
			and a.check_date::date >= p_date_from and a.check_date::date <=  p_date_to 
			and a.receipt_no is not null
			and a.check_date::date > get_next_bank_day(a.actual_date::date) 
			and (case when a.check_no is not null then a.checkstat_id in ('05') else true end)
			and a.part_id in ('166','180','187','185','197','220','178','182' )
		
	) a 
	left join mf_pay_particular b on a.pay_part_id = b.pay_part_id
	left join (select distinct on (cash_date) * from cs_dp_header) c on a.trans_date::date = c.cash_date::date
	left join mf_bank_account d on c.bank_acct_id = d.bank_acct_id
	left join mf_boi_chart_of_accounts e on a.acct_id = e.acct_id

UNION ALL

select 'CASH INFLOW' as cf_type, e.acct_name, 'Miscellaneous' as inflow_type, b.partdesc, 
'' as pv_no, '' as cv_no, null::date as date_paid, 0 as pv_amt,
a.acct_id, a.amount, '' as div, '' as dept, proj_alias as proj, phase as phase
	from (  

		--PAYMENTS FROM RF_PAYMENTS (CASH / DATED CHECK / PDC-DUE : WITHIN THE PERIOD)
		select a.pay_part_id, a.trans_date, a.amount, c.proj_alias, b.phase,
			(case when b.sub_proj_id = '001' then '01-01-04-054' else 
			case when b.sub_proj_id = '002' then '01-01-04-057' else '01-01-01-000' end end) acct_id
			from rf_payments a
			left join mf_unit_info b on a.proj_id = b.proj_id and a.pbl_id = b.pbl_id
			left join mf_project c on a.proj_id = c.proj_id
			where a.status_id != 'I'
			and a.trans_date::date >= p_date_from and a.trans_date::date <=  p_date_to 
			and coalesce(request_no,'') = ''
			and (coalesce(receipt_id,'') = '' or coalesce(receipt_id,'') = 'MBTC')
			and a.co_id = p_co_id
			and (case when p_proj_id = '' then true else a.proj_id = p_proj_id end)
			and (case when p_phase_no = '' then true else trim(b.sub_proj_id) = p_phase_no end) 
			and (case when a.pymnt_type = 'B' then a.pay_rec_id in (select pay_rec_id from cs_dp_chk_detail 
				where dep_no in (select distinct on (dep_no) dep_no from cs_dp_header where post_date::date >= p_date_from 
				and post_date::date <= p_date_to and status_id != 'I')) else true end)
			and a.pay_part_id not in ('166','180','187','185','197','220','178','182','168','033','163','042','040','041','106','203')
			and (case when a.check_no is not null then a.pay_rec_id in (select pay_rec_id::int from rf_check_history 
				where new_checkstat_id = '05' and status_id != 'I' and trans_date::date >= p_date_from 
				and trans_date::date <= p_date_to) else true end)
			and (case when a.pymnt_type = 'A' then a.trans_date::date not in (select cash_date from cs_dp_header 
				where dep_no in (select dep_no from cs_dp_csh_detail where status_id != 'I') and cash_date::date >= p_date_from 
				and cash_date::date <= p_date_to and dep_date::date >= p_date_to) else true end)
		
		union all
		
		--PAYMENTS FROM RF_PAYMENTS (PDC-DUE : OUTSIDE THE PERIOD)
		select a.pay_part_id, a.trans_date, a.amount, c.proj_alias, b.phase,
			(case when b.sub_proj_id = '001' then '01-01-04-054' else 
			case when b.sub_proj_id = '002' then '01-01-04-057' else '01-01-01-000' end end) acct_id
			from rf_payments a
			left join mf_unit_info b on a.proj_id = b.proj_id and a.pbl_id = b.pbl_id
			left join mf_project c on a.proj_id = c.proj_id
			where a.status_id != 'I'
			and a.co_id = p_co_id
			and (case when p_proj_id = '' then true else a.proj_id = p_proj_id end)
			and (case when p_phase_no = '' then true else trim(b.sub_proj_id) = p_phase_no end) 
			and a.trans_date::date < p_date_from 
			and a.pay_rec_id in (select pay_rec_id from cs_dp_chk_detail where dep_no in (select distinct on (dep_no) dep_no from cs_dp_header 
				where post_date::date >= p_date_from and post_date::date <= p_date_to and status_id != 'I'))
			and coalesce(request_no,'') = ''
			and (coalesce(receipt_id,'') = '' or coalesce(receipt_id,'') = 'MBTC')
			and a.check_date::date > get_next_bank_day(a.actual_date::date) 
			and a.pay_part_id not in ('166','180','187','185','197','220','178','182','168','033','163','042','040','041','106','203')
			and (case when a.check_no is not null then a.pay_rec_id in (select pay_rec_id::int from rf_check_history 
				where new_checkstat_id = '05' and status_id != 'I' and trans_date::date >= p_date_from and trans_date::date <= p_date_to) else true end)
		
		union all
		
		--PAYMENTS FROM RF_TRA_DETAIL (CASH / DATED CHECK / PDC-DUE : WITHIN THE PERIOD)
		select a.part_id, a.tran_date, a.amount, d.proj_alias, c.phase,
			(case when c.sub_proj_id = '001' then '01-01-04-054' else 
			case when c.sub_proj_id = '002' then '01-01-04-057' else '01-01-01-000' end end) acct_id
			from rf_tra_detail a 
			join (select distinct on (client_seqno) * from rf_tra_header where co_id = p_co_id) b  on a.client_seqno = b.client_seqno
			left join mf_unit_info c on b.proj_id = c.proj_id and b.pbl_id = c.pbl_id
			left join mf_project d on b.proj_id = d.proj_id
			where a.status_id != 'I'
			and (case when p_proj_id = '' then true else b.proj_id = p_proj_id end)
			and (case when p_phase_no = '' then true else trim(c.sub_proj_id) = p_phase_no end) 
			and a.tran_date::date >= p_date_from and tran_date::date <=  p_date_to 
			and a.receipt_no is not null
			and (case when a.check_no is not null then a.checkstat_id in ('05') else true end)
			and a.part_id not in ('166','180','187','185','197','220','178','182','168','033','163','042','040','041','106','203')
			and (case when check_no is null  then a.tran_date::date not in (select cash_date from cs_dp_header 
				where dep_no in (select dep_no from cs_dp_csh_detail where status_id != 'I') and cash_date::date >= p_date_from 
				and cash_date::date <= p_date_to and dep_date::date >= p_date_to) else true end)

		union all
		
		--PAYMENTS FROM RF_TRA_DETAIL (PDC-DUE : OUTSIDE THE PERIOD)
		select a.part_id, a.tran_date, a.amount, d.proj_alias, c.phase,
			(case when c.sub_proj_id = '001' then '01-01-04-054' else 
			case when c.sub_proj_id = '002' then '01-01-04-057' else '01-01-01-000' end end) acct_id
			from rf_tra_detail a 
			join (select distinct on (client_seqno) * from rf_tra_header where co_id = p_co_id) b  on a.client_seqno = b.client_seqno
			left join mf_unit_info c on b.proj_id = c.proj_id and b.pbl_id = c.pbl_id
			left join mf_project d on b.proj_id = d.proj_id
			where a.status_id != 'I'
			and (case when p_proj_id = '' then true else b.proj_id = p_proj_id end)
			and (case when p_phase_no = '' then true else trim(c.sub_proj_id) = p_phase_no end) 
			and a.tran_date::date < p_date_from 
			and a.check_date::date >= p_date_from and a.check_date::date <=  p_date_to 
			and a.receipt_no is not null
			and a.check_date::date > get_next_bank_day(a.actual_date::date) 
			and (case when a.check_no is not null then a.checkstat_id in ('05') else true end)
			and a.part_id not in ('166','180','187','185','197','220','178','182','168','033','163','042','040','041','106','203')
		
	) a 
	left join mf_pay_particular b on a.pay_part_id = b.pay_part_id
	left join (select distinct on (cash_date) * from cs_dp_header) c on a.trans_date::date = c.cash_date::date
	left join mf_bank_account d on c.bank_acct_id = d.bank_acct_id
	left join mf_boi_chart_of_accounts e on a.acct_id = e.acct_id

/***********************END OF DEPOSITED CASH INFLOW*********************************/

UNION ALL

/***********************START OF CASH OUTFLOW*********************************/

select 'CASH OUTFLOW' as cf_type, d.acct_name, 'Advances to Officers/Employees' as inflow_type, c.acct_name, 
a.pv_no, b.cv_no, b.date_paid::date, bb.tran_amt as pv_amt,
d.acct_id, 
--w.pv_amt*-1 as real_outflow_amt, 
w.tran_amt*-1 as real_outflow_amt, 
coalesce(f.division_alias,'') as div, 
coalesce(e.dept_alias,'') as dept, g.proj_alias, h.phase
-- from ( --select distinct on (co_id, pv_no, acct_id) co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid, sum(tran_amt) as tran_amt 
-- 		select distinct on (co_id, pv_no, acct_id) co_id, pv_no, acct_id, sum(tran_amt) as tran_amt 
-- 	from rf_pv_detail where status_id != 'I' and bal_side = 'D' 
-- 	and acct_id != '01-99-03-000'
-- 	and acct_id like '01-02%'
-- 	and co_id = p_co_id
-- 	and (case when p_proj_id = '' then true else project_id = '' end)
-- 	and (case when p_phase_no = '' then true else trim(sub_projectid) = '' end) 
-- 	--group by co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid ) a
-- 	group by co_id, pv_no, acct_id ) a
from ( --select distinct on (co_id, pv_no, acct_id) co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid, sum(tran_amt) as tran_amt 
		select --distinct on (co_id, pv_no, acct_id) 
	co_id, pv_no, acct_id, sum(tran_amt) as tran_amt, div_id, dept_id, project_id, sub_projectid
	from rf_pv_detail where status_id != 'I' and bal_side = 'D' 
	and acct_id != '01-99-03-000'
	and acct_id = '01-02-04-000' --ADDED BY LESTER TO FILTER ONLY ADVANCES TO OFFICERS AND EMPLOYEES ACCOUNTS
	and acct_id like '01-02%'
	and co_id = p_co_id
	and (case when p_proj_id = '' then true else project_id = '' end)
	and (case when p_phase_no = '' then true else trim(sub_projectid) = '' end) 
	group by co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid  ) a
-- left join (select --distinct on (co_id, rplf_no, line_no) commented by jed 2021-05-12
-- 	* from rf_request_detail 
-- 	where co_id = p_co_id
-- 	and status_id = 'A' 
-- 	and acct_id != '08-03-03-006' /**added by jed : remove SOE Personnel**/
-- 	--and acct_id !~* '08-03'
-- 	--and acct_id like '01-02%'
-- 	and (acct_id like '01-02%' or rplf_no in (select rplf_no from rf_request_header where rplf_type_id = '02' and status_id = 'A'))
-- 	) w on a.pv_no = w.rplf_no and a.co_id = w.co_id --added by DG on 03/01/2017
left join (select --distinct on (co_id, rplf_no, acct_id) --commented by jed 2021-05-12
	co_id, rplf_no, pv_amt as tran_amt, acct_id, dept_id, div_id, project_id, sub_projectid from rf_request_detail
	where co_id = p_co_id
	and status_id = 'A' 
	and acct_id != '08-03-03-006' /**added by jed : remove SOE Personnel**/
	and (acct_id like '01-02%' or rplf_no in (select rplf_no from rf_request_header where rplf_type_id = '02' and status_id = 'A' and co_id = p_co_id))
	--group by co_id, rplf_no, acct_id, dept_id, div_id, project_id, sub_projectid 
	order by co_id, rplf_no, pv_amt, acct_id, dept_id, div_id, project_id, sub_projectid 
	)w on a.pv_no = w.rplf_no and a.co_id = w.co_id and a.dept_id = coalesce(w.dept_id, '') and a.div_id = coalesce(w.div_id, '') and a.project_id = coalesce(w.project_id, '') and a.sub_projectid = coalesce(w.sub_projectid, '')
left join (select * from rf_pv_header where co_id = p_co_id) x on a.pv_no = x.pv_no and a.co_id = x.co_id
left join (select distinct on (a.co_id, b.cv_no) a.co_id, b.cv_no, sum(a.tran_amt) as tran_amt 
	from rf_pv_detail a
	left join rf_pv_header b on a.pv_no = b.pv_no
	where a.status_id != 'I' 
	and b.status_id != 'I'
	and a.co_id = p_co_id
	and b.co_id = p_co_id
	and a.bal_side = 'D' and a.acct_id != '01-99-03-000'
	group by a.co_id, b.cv_no ) y on x.cv_no = y.cv_no and x.co_id = y.co_id
left join (select * from rf_pv_header where status_id = 'P' and co_id = p_co_id) aa on a.pv_no = aa.pv_no and a.co_id = aa.co_id
join (select * from rf_cv_header where status_id != 'I' and date_paid is not null 
	and date_paid::date >= p_date_from and date_paid::date <= p_date_to and co_id = p_co_id) b on aa.cv_no = b.cv_no and aa.co_id = b.co_id
join (select distinct on (co_id, cv_no) co_id, cv_no, acct_id, sum(tran_amt) as tran_amt from rf_cv_detail 
	where bal_side = 'C'
	and status_id != 'I'
	and acct_id not like '01-01-01%'
	and co_id = p_co_id
	group by co_id, cv_no, acct_id
	) bb on b.cv_no = bb.cv_no and b.co_id = bb.co_id
left join mf_boi_chart_of_accounts c on w.acct_id = c.acct_id
left join mf_boi_chart_of_accounts d on bb.acct_id = d.acct_id
left join mf_department e on w.dept_id = e.dept_code
left join mf_division f on w.div_id = f.division_code
left join mf_project g on w.project_id = g.proj_id
left join mf_sub_project h on w.project_id = h.proj_id and w.sub_projectid = h.sub_proj_id AND h.status_id = 'A' --(2024-09-30)ADDED STATUS ID BY LESTER TO PREVENT DOUBLE ROWS IN DISPLAY

UNION ALL

select 'CASH OUTFLOW' as cf_type, 
d.acct_name, 
'Property Development Cost' as inflow_type, 
c.acct_name,
a.pv_no, 
b.cv_no,
b.date_paid, 
bb.tran_amt as pv_amt,
d.acct_id, 
--(a.tran_amt*(bb.tran_amt/y.tran_amt))*-1 as real_outflow_amt,
w.tran_amt*-1 as real_outflow_amt,
coalesce(f.division_alias,'') as div, 
coalesce(e.dept_alias,'') as dept,
g.proj_alias,
h.phase
from ( select --distinct on (co_id, pv_no, acct_id, dept_id, div_id, project_id, sub_projectid) 
	  co_id, pv_no, acct_id, dept_id, div_id, project_id, sub_projectid, sum(tran_amt) as tran_amt 
	from (select * from rf_pv_detail where acct_id like '01-03%' or acct_id like '04-02%')a where status_id != 'I' and bal_side = 'D' 
	and acct_id != '01-99-03-000'
	and co_id = p_co_id
	and (case when p_proj_id = '' then true else project_id = '' end)
	and (case when p_phase_no = '' then true else trim(sub_projectid) = '' end) 
	group by co_id, pv_no, acct_id, dept_id, div_id, project_id, sub_projectid) a
left join (select --distinct on (co_id, rplf_no, acct_id) --commented by jed 2021-05-12
				co_id, rplf_no, sum(pv_amt) as tran_amt, acct_id, dept_id, div_id, project_id, sub_projectid from rf_request_detail
				where co_id = p_co_id
				and status_id = 'A' 
				and (acct_id like '01-03%' or acct_id like '04-02%')
				--and rplf_no = '000059899'
		   		group by co_id, rplf_no, acct_id, dept_id, div_id, project_id, sub_projectid 
				) w on a.pv_no = w.rplf_no and a.co_id = w.co_id and a.acct_id = w.acct_id and a.dept_id = coalesce(w.dept_id, '') and a.div_id = coalesce(w.div_id, '') and a.project_id = coalesce(w.project_id, '') and a.sub_projectid = coalesce(w.sub_projectid, '')
left join (select * from rf_pv_header where co_id = p_co_id) x on a.pv_no = x.pv_no and a.co_id = x.co_id
-- left join ( select distinct on (a.co_id, b.cv_no) a.co_id, b.cv_no, sum(a.tran_amt) as tran_amt 
-- 	from rf_pv_detail a
-- 	left join rf_pv_header b on a.pv_no = b.pv_no
-- 	where a.status_id != 'I' 
-- 	and b.status_id != 'I'
-- 	and a.bal_side = 'D' and a.acct_id != '01-99-03-000'
-- 	and a.co_id = p_co_id
-- 	and b.co_id = p_co_id
-- 	group by a.co_id, b.cv_no ) y on x.cv_no = y.cv_no and x.co_id = y.co_id
left join (select * from rf_pv_header where status_id = 'P' and co_id = p_co_id) aa on a.pv_no = aa.pv_no and a.co_id = aa.co_id
join (select * from rf_cv_header where status_id != 'I' and date_paid is not null 
	and date_paid::date >= p_date_from and date_paid::date <= p_date_to and co_id = p_co_id) b on aa.cv_no = b.cv_no and aa.co_id = b.co_id
join (select distinct on (co_id, cv_no) co_id, cv_no, acct_id, sum(tran_amt) as tran_amt from rf_cv_detail 
	where bal_side = 'C'
	and status_id != 'I'
	and acct_id not like '01-01-01%'
	and co_id = p_co_id
	group by co_id, cv_no, acct_id
	) bb on b.cv_no = bb.cv_no and b.co_id = bb.co_id
left join mf_boi_chart_of_accounts c on a.acct_id = c.acct_id
left join mf_boi_chart_of_accounts d on bb.acct_id = d.acct_id
left join mf_department e on w.dept_id = e.dept_code
left join mf_division f on w.div_id = f.division_code
left join mf_project g on w.project_id = g.proj_id
left join mf_sub_project h on w.project_id = h.proj_id and w.sub_projectid = h.sub_proj_id and h.status_id = 'A' --(2024-09-30)ADDED STATUS ID BY LESTER TO PREVENT DOUBLE ROWS IN DISPLAY

UNION ALL

select 'CASH OUTFLOW' as cf_type, d.acct_name, 'Commission' as inflow_type, c.acct_name, 
a.pv_no, b.cv_no, b.date_paid, bb.tran_amt as pv_amt,
d.acct_id, 
--(a.tran_amt*(bb.tran_amt/y.tran_amt))*-1 as real_outflow_amt, 
w.tran_amt*-1 as real_outflow_amt,
coalesce(f.division_alias,'') as div, 
coalesce(e.dept_alias,'') as dept, g.proj_alias, h.phase
from ( select --distinct on (co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid) 
	  co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid, sum(tran_amt) as tran_amt 
	from rf_pv_detail where status_id != 'I' and bal_side = 'D' 
		and acct_id != '01-99-03-000'
		and acct_id = '08-03-01-000'
		--and pv_no not in (select distinct on (pv_no) pv_no from rf_pv_detail where acct_id != '01-99-03-000'
		--	and acct_id like '01-02%' and status_id != 'I' and bal_side = 'D' )
		and co_id = p_co_id
		and (case when p_proj_id = '' then true else project_id = '' end)
		and (case when p_phase_no = '' then true else trim(sub_projectid) = '' end) 
		group by co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid ) a
left join (select --distinct on (co_id, rplf_no, acct_id) --commented by jed 2021-05-12
				co_id, rplf_no, sum(pv_amt) as tran_amt, acct_id, dept_id, div_id, project_id, sub_projectid from rf_request_detail
				where co_id = p_co_id
				and status_id = 'A' 
				and acct_id = '08-03-01-000'
				--and rplf_no = '000059253'
		   		group by co_id, rplf_no, acct_id, dept_id, div_id, project_id, sub_projectid 
				)w on a.pv_no = w.rplf_no and a.co_id = w.co_id and a.acct_id = w.acct_id and a.dept_id = coalesce(w.dept_id, '') and a.div_id = coalesce(w.div_id, '') and a.project_id = coalesce(w.project_id, '') and a.sub_projectid = coalesce(w.sub_projectid, '')
left join (select * from rf_pv_header where co_id = p_co_id) x on a.pv_no = x.pv_no and a.co_id = x.co_id
-- left join (select distinct on (a.co_id, b.cv_no) a.co_id, b.cv_no, sum(a.tran_amt) as tran_amt 
-- 	from rf_pv_detail a
-- 	left join rf_pv_header b on a.pv_no = b.pv_no
-- 	where a.status_id != 'I' 
-- 	and b.status_id != 'I'
-- 	and a.co_id = p_co_id
-- 	and b.co_id = p_co_id
-- 	and a.bal_side = 'D' and a.acct_id != '01-99-03-000'
-- 	group by a.co_id, b.cv_no) y on x.cv_no = y.cv_no and x.co_id = y.co_id
left join (select * from rf_pv_header where status_id = 'P' and co_id = p_co_id) aa on a.pv_no = aa.pv_no and a.co_id = aa.co_id
join (select * from rf_cv_header where status_id != 'I' and date_paid is not null 
	and date_paid::date >= p_date_from and date_paid::date <= p_date_to and co_id = p_co_id) b on aa.cv_no = b.cv_no and aa.co_id = b.co_id
join (select distinct on (co_id, cv_no) co_id, cv_no, acct_id, sum(tran_amt) as tran_amt from rf_cv_detail 
	where bal_side = 'C'
	and status_id != 'I'
	and co_id = p_co_id 
	and acct_id not like '01-01-01%'
	group by co_id, cv_no, acct_id
	) bb on b.cv_no = bb.cv_no and b.co_id = bb.co_id
left join mf_boi_chart_of_accounts c on a.acct_id = c.acct_id
left join mf_boi_chart_of_accounts d on bb.acct_id = d.acct_id
left join mf_department e on w.dept_id = e.dept_code
left join mf_division f on w.div_id = f.division_code
left join mf_project g on w.project_id = g.proj_id
left join mf_sub_project h on w.project_id = h.proj_id and w.sub_projectid = h.sub_proj_id AND h.status_id = 'A'

UNION ALL

select 'CASH OUTFLOW' as cf_type, d.acct_name, 'MAF/SOE' as inflow_type, c.acct_name, 
a.pv_no, b.cv_no, b.date_paid, 
--y.tran_amt as pv_amt,
bb.tran_amt as pv_amt,
d.acct_id, 
--(a.tran_amt*(bb.tran_amt/y.tran_amt))*-1 as real_outflow_amt, 
w.tran_amt*-1 as real_outflow_amt,
coalesce(f.division_alias,'') as div, 
coalesce(e.dept_alias,'') as dept, g.proj_alias, h.phase
from ( --select distinct on (co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid) co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid, sum(tran_amt) as tran_amt 
	  select --distinct on (co_id, pv_no, acct_id) 
	co_id, pv_no, acct_id, sum(tran_amt) as tran_amt, div_id, dept_id, project_id, sub_projectid
	from rf_pv_detail where status_id != 'I' and bal_side = 'D' 
		and acct_id != '01-99-03-000'
		and acct_id != '08-03-01-000'
		and acct_id like '08-03%'
		--and pv_no not in (select distinct on (pv_no) pv_no from rf_pv_detail where acct_id != '01-99-03-000'
		--	and acct_id like '01-02%' and status_id != 'I' and bal_side = 'D' )
		and co_id = p_co_id
		and (case when p_proj_id = '' then true else project_id = '' end)
		and (case when p_phase_no = '' then true else trim(sub_projectid) = '' end) 
		--and pv_no = '000059899'
		group by co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid ) a
left join (select --distinct on (co_id, rplf_no, acct_id) --commented by jed 2021-05-12
				co_id, rplf_no, sum(pv_amt) as tran_amt, acct_id, dept_id, div_id, project_id, sub_projectid from rf_request_detail
				where co_id = p_co_id
				and status_id = 'A' 
				and acct_id like '08-03%'
				--and rplf_no = '000059899'
		   		group by co_id, rplf_no, acct_id, dept_id, div_id, project_id, sub_projectid 
				)w on a.pv_no = w.rplf_no and a.co_id = w.co_id and a.acct_id = w.acct_id and a.dept_id = coalesce(w.dept_id, '') and a.div_id = coalesce(w.div_id, '') and a.project_id = coalesce(w.project_id, '') and a.sub_projectid = coalesce(w.sub_projectid, '')
--left join (select * from rf_pv_header where co_id = p_co_id) x on a.pv_no = x.pv_no and a.co_id = x.co_id
-- left join (select distinct on (a.co_id, b.cv_no) a.co_id, b.cv_no, sum(a.tran_amt) as tran_amt 
-- 	from rf_pv_detail a
-- 	left join rf_pv_header b on a.pv_no = b.pv_no
-- 	where a.status_id != 'I' 
-- 	and b.status_id != 'I'
-- 	and a.co_id = p_co_id
-- 	and b.co_id = p_co_id
-- 	and a.bal_side = 'D' and a.acct_id != '01-99-03-000'
-- 	group by a.co_id, b.cv_no ) y on x.cv_no = y.cv_no and x.co_id = y.co_id
left join (select * from rf_pv_header where status_id = 'P' and co_id = p_co_id) aa on a.pv_no = aa.pv_no
join (select * from rf_cv_header where status_id != 'I' and date_paid is not null 
	and date_paid::date >= p_date_from and date_paid::date <= p_date_to and co_id = p_co_id) b on aa.cv_no = b.cv_no and aa.co_id = b.co_id
join (select distinct on (co_id, cv_no) co_id, cv_no, acct_id, sum(tran_amt) as tran_amt from rf_cv_detail 
	where bal_side = 'C'
	and status_id != 'I'
	and co_id = p_co_id
	and acct_id not like '01-01-01%'
	group by co_id, cv_no, acct_id
	) bb on b.cv_no = bb.cv_no and b.co_id = bb.co_id
left join mf_boi_chart_of_accounts c on a.acct_id = c.acct_id
left join mf_boi_chart_of_accounts d on bb.acct_id = d.acct_id
left join mf_department e on w.dept_id = e.dept_code
left join mf_division f on w.div_id = f.division_code
left join mf_project g on w.project_id = g.proj_id
left join mf_sub_project h on w.project_id = h.proj_id and w.sub_projectid = h.sub_proj_id and h.status_id = 'A'

UNION ALL

select 'CASH OUTFLOW' as cf_type, d.acct_name, 'SME/GAE' as inflow_type, c.acct_name, 
a.pv_no, b.cv_no, b.date_paid, bb.tran_amt as pv_amt,
d.acct_id, 
--(a.tran_amt*(bb.tran_amt/y.tran_amt))*-1 as real_outflow_amt,
w.tran_amt*-1 real_outflow_amt,
coalesce(f.division_alias,'') as div, 
coalesce(e.dept_alias,'') as dept, g.proj_alias, h.phase
from ( select --distinct on (co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid) 
	co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid, sum(tran_amt) as tran_amt 
	from (select * from rf_pv_detail where acct_id like '08-01%' or acct_id like '08-02%'
	) a where status_id != 'I' and bal_side = 'D' 
		and acct_id != '01-99-03-000'
		and acct_id != '08-03-01-000'
		--and pv_no not in (select distinct on (pv_no) pv_no from rf_pv_detail where acct_id != '01-99-03-000'
		--	and acct_id like '01-02%' and status_id != 'I' and bal_side = 'D' )
		and co_id = p_co_id
		and (case when p_proj_id = '' then true else project_id = '' end)
		and (case when p_phase_no = '' then true else trim(sub_projectid) = '' end) 
	  	--and pv_no = '000057769'
		group by co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid ) a
left join (select --distinct on (co_id, rplf_no, acct_id) --commented by jed 2021-05-12
		co_id, rplf_no, sum(pv_amt) as tran_amt, acct_id, dept_id, div_id, project_id, sub_projectid from rf_request_detail
		where co_id = p_co_id
		and status_id = 'A' 
		and (acct_id like '08-01%' or acct_id like '08-02%')
		--and rplf_no = '000057769'
		group by co_id, rplf_no, acct_id, dept_id, div_id, project_id, sub_projectid 
		) w on a.pv_no = w.rplf_no and a.co_id = w.co_id and a.acct_id = w.acct_id and a.dept_id = coalesce(w.dept_id, '') and a.div_id = coalesce(w.div_id, '') and a.project_id = coalesce(w.project_id, '') and a.sub_projectid = coalesce(w.sub_projectid, '')
left join (select * from rf_pv_header where co_id = p_co_id) x on a.pv_no = x.pv_no and a.co_id = x.co_id
-- left join ( select distinct on (a.co_id, b.cv_no) a.co_id, b.cv_no, sum(a.tran_amt) as tran_amt 
-- 	from rf_pv_detail a
-- 	left join rf_pv_header b on a.pv_no = b.pv_no
-- 	where a.status_id != 'I' 
-- 	and b.status_id != 'I'
-- 	and a.co_id = p_co_id
-- 	and b.co_id = p_co_id
-- 	and a.bal_side = 'D' and a.acct_id != '01-99-03-000'
-- 	group by a.co_id, b.cv_no ) y on x.cv_no = y.cv_no and x.co_id = y.co_id
left join (select * from rf_pv_header where status_id = 'P' and co_id = p_co_id) aa on a.pv_no = aa.pv_no and a.co_id = aa.co_id
join (select * from rf_cv_header where status_id != 'I' and date_paid is not null 
	and date_paid::date >= p_date_from and date_paid::date <= p_date_to and co_id = p_co_id) b on aa.cv_no = b.cv_no and aa.co_id = b.co_id
join (select distinct on (co_id, cv_no) co_id, cv_no, acct_id, sum(tran_amt) as tran_amt from rf_cv_detail 
	where bal_side = 'C'
	and status_id != 'I'
	and co_id = p_co_id
	and acct_id not like '01-01-01%'
	group by co_id, cv_no, acct_id
	) bb on b.cv_no = bb.cv_no and b.co_id = bb.co_id
left join mf_boi_chart_of_accounts c on a.acct_id = c.acct_id
left join mf_boi_chart_of_accounts d on bb.acct_id = d.acct_id
left join mf_department e on w.dept_id = e.dept_code
left join mf_division f on w.div_id = f.division_code
left join mf_project g on w.project_id = g.proj_id
left join mf_sub_project h on w.project_id = h.proj_id and w.sub_projectid = h.sub_proj_id and h.status_id = 'A' --(2024-09-30)ADDED STATUS ID BY LESTER TO PREVENT DOUBLE ROWS IN DISPLAY
--where a.pv_no = '000059464'

UNION ALL

select 'CASH OUTFLOW' as cf_type, d.acct_name, 'PPE' as inflow_type, c.acct_name, 
a.pv_no, b.cv_no, b.date_paid, bb.tran_amt as pv_amt,
d.acct_id, (a.tran_amt*(bb.tran_amt/y.tran_amt))*-1 as real_outflow_amt, coalesce(f.division_alias,'') as div, 
coalesce(e.dept_alias,'') as dept, g.proj_alias, h.phase
from ( select distinct on (co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid) co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid, sum(tran_amt) as tran_amt 
	from (select * from rf_pv_detail where acct_id like '02-03%' ) a where status_id != 'I' and bal_side = 'D' 
		and acct_id != '01-99-03-000'
		and acct_id != '08-03-01-000'
		--and pv_no not in (select distinct on (pv_no) pv_no from rf_pv_detail where acct_id != '01-99-03-000'
		--	and acct_id like '01-02%' and status_id != 'I' and bal_side = 'D' )
		and co_id = p_co_id
		and (case when p_proj_id = '' then true else project_id = '' end)
		and (case when p_phase_no = '' then true else trim(sub_projectid) = '' end) 
		group by co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid ) a
left join (select * from rf_pv_header where co_id = p_co_id) x on a.pv_no = x.pv_no and a.co_id = x.co_id
left join ( select distinct on (a.co_id, b.cv_no) a.co_id, b.cv_no, sum(a.tran_amt) as tran_amt 
	from rf_pv_detail a
	left join rf_pv_header b on a.pv_no = b.pv_no
	where a.status_id != 'I' 
	and b.status_id != 'I'
	and a.co_id = p_co_id
	and b.co_id = p_co_id
	and a.bal_side = 'D' and a.acct_id != '01-99-03-000'
	group by a.co_id, b.cv_no ) y on x.cv_no = y.cv_no and x.co_id = y.co_id
left join (select * from rf_pv_header where status_id = 'P' and co_id = p_co_id) aa on a.pv_no = aa.pv_no and a.co_id = aa.co_id
join (select * from rf_cv_header where status_id != 'I' and date_paid is not null 
	and date_paid::date >= p_date_from and date_paid::date <= p_date_to and co_id = p_co_id) b on aa.cv_no = b.cv_no and aa.co_id = b.co_id
join (select distinct on (cv_no) co_id, cv_no, acct_id, sum(tran_amt) as tran_amt from rf_cv_detail 
	where bal_side = 'C'
	and status_id != 'I'
	and acct_id not like '01-01-01%'
	and co_id = p_co_id
	group by co_id, cv_no, acct_id
	) bb on b.cv_no = bb.cv_no and b.co_id = bb.co_id
left join mf_boi_chart_of_accounts c on a.acct_id = c.acct_id
left join mf_boi_chart_of_accounts d on bb.acct_id = d.acct_id
left join mf_department e on a.dept_id = e.dept_code
left join mf_division f on a.div_id = f.division_code
left join mf_project g on a.project_id = g.proj_id
left join mf_sub_project h on a.project_id = h.proj_id and a.sub_projectid = h.sub_proj_id AND h.status_id = 'A'

UNION ALL

select 'CASH OUTFLOW' as cf_type, d.acct_name, 'Return of Payment Withheld' as inflow_type, c.acct_name, 
a.pv_no, b.cv_no, b.date_paid, bb.tran_amt as pv_amt,
d.acct_id, 
--(a.tran_amt*(bb.tran_amt/y.tran_amt))*-1 as real_outflow_amt, 
w.tran_amt*-1 as real_outflow_amt,
coalesce(f.division_alias,'') as div, 
coalesce(e.dept_alias,'') as dept, g.proj_alias, h.phase
from ( select --distinct on (co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid) 
	  co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid, sum(tran_amt) as tran_amt 
	from (select * from rf_pv_detail where acct_id like '03-01%' ) a where status_id != 'I' and bal_side = 'D' 
		and acct_id != '01-99-03-000'
		and acct_id != '08-03-01-000'
		--and pv_no not in (select distinct on (pv_no) pv_no from rf_pv_detail where acct_id != '01-99-03-000'
		--	and acct_id like '01-02%' and status_id != 'I' and bal_side = 'D' )
		and co_id = p_co_id
		and (case when p_proj_id = '' then true else project_id = '' end)
		and (case when p_phase_no = '' then true else trim(sub_projectid) = '' end) 
		group by co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid ) a
left join (select --distinct on (co_id, rplf_no, acct_id) --commented by jed 2021-05-12
				co_id, rplf_no, sum(pv_amt) as tran_amt, acct_id, dept_id, div_id, project_id, sub_projectid from rf_request_detail
				where co_id = p_co_id
				and status_id = 'A' 
				and acct_id like '03-01%'
				--and rplf_no = '000059253'
		   		group by co_id, rplf_no, acct_id, dept_id, div_id, project_id, sub_projectid 
				) w on a.pv_no = w.rplf_no and a.co_id = w.co_id and a.acct_id = w.acct_id and a.dept_id = coalesce(w.dept_id, '') and a.div_id = coalesce(w.div_id, '') and a.project_id = coalesce(w.project_id, '') and a.sub_projectid = coalesce(w.sub_projectid, '')
left join (select * from rf_pv_header where co_id = p_co_id) x on a.pv_no = x.pv_no and a.co_id = x.co_id
-- left join ( select distinct on (a.co_id, b.cv_no) a.co_id, b.cv_no, sum(a.tran_amt) as tran_amt 
-- 	from rf_pv_detail a
-- 	left join rf_pv_header b on a.pv_no = b.pv_no
-- 	where a.status_id != 'I' 
-- 	and b.status_id != 'I'
-- 	and a.co_id = p_co_id
-- 	and b.co_id = p_co_id
-- 	and a.bal_side = 'D' and a.acct_id != '01-99-03-000'
-- 	group by a.co_id, b.cv_no ) y on x.cv_no = y.cv_no
left join (select * from rf_pv_header where status_id = 'P' and co_id = p_co_id) aa on a.pv_no = aa.pv_no and a.co_id = aa.co_id
join (select * from rf_cv_header where status_id != 'I' and date_paid is not null 
	and date_paid::date >= p_date_from and date_paid::date <= p_date_to and co_id = p_co_id) b on aa.cv_no = b.cv_no and aa.co_id = b.co_id
join (select distinct on (co_id, cv_no) co_id, cv_no, acct_id, sum(tran_amt) as tran_amt from rf_cv_detail 
	where bal_side = 'C'
	and status_id != 'I'
	and acct_id not like '01-01-01%'
	and co_id = p_co_id
	group by co_id, cv_no, acct_id
	) bb on b.cv_no = bb.cv_no and b.co_id = bb.co_id
left join mf_boi_chart_of_accounts c on a.acct_id = c.acct_id
left join mf_boi_chart_of_accounts d on bb.acct_id = d.acct_id
left join mf_department e on w.dept_id = e.dept_code
left join mf_division f on w.div_id = f.division_code
left join mf_project g on w.project_id = g.proj_id
left join mf_sub_project h on w.project_id = h.proj_id and w.sub_projectid = h.sub_proj_id and h.status_id = 'A' --(2024-09-30)ADDED STATUS ID BY LESTER TO PREVENT DOUBLE ROWS IN DISPLAY

UNION ALL

select 'CASH OUTFLOW' as cf_type, d.acct_name, 'Fund Transfer' as inflow_type, c.acct_name, 
a.pv_no, b.cv_no, b.date_paid, bb.tran_amt as pv_amt,
d.acct_id, 
--(a.tran_amt*(bb.tran_amt/y.tran_amt))*-1 as real_outflow_amt, 
w.tran_amt*-1 as real_outflow_amt,
coalesce(f.division_alias,'') as div, 
coalesce(e.dept_alias,'') as dept, g.proj_alias, h.phase
from ( select --distinct on (co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid) 
	  co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid, sum(tran_amt) as tran_amt 
	from (select * from rf_pv_detail where acct_id like '01-01%' ) a where status_id != 'I' and bal_side = 'D' 
		and acct_id != '01-99-03-000'
		and acct_id != '08-03-01-000'
		--and pv_no not in (select distinct on (pv_no) pv_no from rf_pv_detail where acct_id != '01-99-03-000'
		--	and acct_id like '01-02%' and status_id != 'I' and bal_side = 'D' )
		and co_id = p_co_id
		and (case when p_proj_id = '' then true else project_id = '' end)
		and (case when p_phase_no = '' then true else trim(sub_projectid) = '' end) 
		group by co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid ) a
left join (select --distinct on (co_id, rplf_no, acct_id) --commented by jed 2021-05-12
				co_id, rplf_no, sum(pv_amt) as tran_amt, acct_id, dept_id, div_id, project_id, sub_projectid from rf_request_detail
				where co_id = p_co_id
				and status_id = 'A' 
				and acct_id like '01-01%'
				--and rplf_no = '000059253'
		   		group by co_id, rplf_no, acct_id, dept_id, div_id, project_id, sub_projectid 
				) w on a.pv_no = w.rplf_no and a.co_id = w.co_id and a.acct_id = w.acct_id and a.dept_id = coalesce(w.dept_id, '') and a.div_id = coalesce(w.div_id, '') and a.project_id = coalesce(w.project_id, '') and a.sub_projectid = coalesce(w.sub_projectid, '')
left join (select * from rf_pv_header where co_id = p_co_id) x on a.pv_no = x.pv_no and a.co_id = x.co_id
-- left join ( select distinct on (a.co_id, b.cv_no) a.co_id, b.cv_no, sum(a.tran_amt) as tran_amt 
-- 	from rf_pv_detail a
-- 	left join rf_pv_header b on a.pv_no = b.pv_no
-- 	where a.status_id != 'I' 
-- 	and b.status_id != 'I'
-- 	and a.bal_side = 'D' 
-- 	and a.acct_id != '01-99-03-000'
-- 	and a.co_id = p_co_id
-- 	and b.co_id = p_co_id
-- 	group by a.co_id, b.cv_no ) y on x.cv_no = y.cv_no and x.co_id = y.co_id
left join (select * from rf_pv_header where status_id = 'P' and co_id = p_co_id) aa on a.pv_no = aa.pv_no and a.co_id = aa.co_id
join (select * from rf_cv_header where status_id != 'I' and date_paid is not null 
	and date_paid::date >= p_date_from and date_paid::date <= p_date_to and co_id = p_co_id)  b on aa.cv_no = b.cv_no and aa.co_id = b.co_id
join (select distinct on (co_id, cv_no) co_id, cv_no, acct_id, sum(tran_amt) as tran_amt from rf_cv_detail 
	where bal_side = 'C'
	and status_id != 'I'
	and acct_id not like '01-01-01%'
	and co_id = p_co_id
	group by co_id, cv_no, acct_id
	) bb on b.cv_no = bb.cv_no and b.co_id = bb.co_id
left join mf_boi_chart_of_accounts c on a.acct_id = c.acct_id
left join mf_boi_chart_of_accounts d on bb.acct_id = d.acct_id
left join mf_department e on w.dept_id = e.dept_code
left join mf_division f on w.div_id = f.division_code
left join mf_project g on w.project_id = g.proj_id
left join mf_sub_project h on w.project_id = h.proj_id and w.sub_projectid = h.sub_proj_id and h.status_id = 'A'

UNION ALL

select 'CASH OUTFLOW' as cf_type, d.acct_name, 'Others' as inflow_type, c.acct_name, 
a.pv_no, b.cv_no, b.date_paid, bb.tran_amt as pv_amt,
d.acct_id, 
--(a.tran_amt*(bb.tran_amt/y.tran_amt))*-1 as real_outflow_amt, 
w.tran_amt*-1 as real_outflow_amt,
coalesce(f.division_alias,'') as div, 
coalesce(e.dept_alias,'') as dept, g.proj_alias, h.phase
from ( select --distinct on (co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid) 
	  co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid, sum(tran_amt) as tran_amt 
	from rf_pv_detail a where status_id != 'I' and bal_side = 'D' 
		and acct_id != '01-99-03-000'
		and acct_id not like '01-01%'  --added by DG : 03/01/2017
		and acct_id not like '01-02%'
		and acct_id not like '01-03%'
		and acct_id != '08-03-01-000'
		and acct_id not like '08-03%'
		and acct_id not like '08-01%' 
		and acct_id not like '08-02%'
		and acct_id not like '02-03%'
		and acct_id not like '03-01%'
		and acct_id not like '04-02%'
		--and pv_no not in (select distinct on (pv_no) pv_no from rf_pv_detail where acct_id != '01-99-03-000'
		--	and acct_id like '01-02%' and status_id != 'I' and bal_side = 'D' and co_id = p_co_id )
		and co_id = p_co_id
		and (case when p_proj_id = '' then true else project_id = '' end)
		and (case when p_phase_no = '' then true else trim(sub_projectid) = '' end) 
		group by co_id, pv_no, acct_id, dept_id, div_id, project_id, sub_projectid ) a
left join (select --distinct on (co_id, rplf_no, acct_id) --commented by jed 2021-05-12
				co_id, rplf_no, sum(pv_amt) as tran_amt, acct_id, dept_id, div_id, project_id, sub_projectid from rf_request_detail
				where co_id = p_co_id
				and status_id = 'A' 
				and acct_id != '01-99-03-000'
				and acct_id not like '01-01%'
				and acct_id not like '01-02%'
				and acct_id not like '01-03%'
				and acct_id != '08-03-01-000'
				and acct_id not like '08-03%'
				and acct_id not like '08-01%' 
				and acct_id not like '08-02%'
				and acct_id not like '02-03%'
				and acct_id not like '03-01%'
				and acct_id not like '04-02%'
				--and rplf_no = '000059253'
		   		group by co_id, rplf_no, acct_id, dept_id, div_id, project_id, sub_projectid 
				) w on a.pv_no = w.rplf_no and a.co_id = w.co_id and a.acct_id = w.acct_id and a.dept_id = coalesce(w.dept_id, '') and a.div_id = coalesce(w.div_id, '') and a.project_id = coalesce(w.project_id, '') and a.sub_projectid = coalesce(w.sub_projectid, '')
left join (select * from rf_pv_header where co_id = p_co_id) x on a.pv_no = x.pv_no and a.co_id = x.co_id
-- left join ( select distinct on (a.co_id, b.cv_no) a.co_id, b.cv_no, sum(a.tran_amt) as tran_amt 
-- 	from rf_pv_detail a
-- 	left join rf_pv_header b on a.pv_no = b.pv_no
-- 	where a.status_id != 'I' 
-- 	and b.status_id != 'I'
-- 	and a.co_id = p_co_id
-- 	and b.co_id = p_co_id
-- 	and a.bal_side = 'D' and a.acct_id != '01-99-03-000'
-- 	group by a.co_id, b.cv_no ) y on x.cv_no = y.cv_no and x.co_id = y.co_id
left join (select * from rf_pv_header where status_id = 'P' and co_id = p_co_id) aa on a.pv_no = aa.pv_no and a.co_id = aa.co_id
join (select * from rf_cv_header where status_id != 'I' and date_paid is not null 
	and date_paid::date >= p_date_from and date_paid::date <= p_date_to and co_id = p_co_id) b on aa.cv_no = b.cv_no and aa.co_id = b.co_id
join (select distinct on (co_id, cv_no) co_id, cv_no, acct_id, sum(tran_amt) as tran_amt from rf_cv_detail 
	where bal_side = 'C'
	and status_id != 'I'
	and acct_id not like '01-01-01%'
	and co_id = p_co_id
	group by co_id, cv_no, acct_id
	) bb on b.cv_no = bb.cv_no and b.co_id = bb.co_id
left join mf_boi_chart_of_accounts c on a.acct_id = c.acct_id
left join mf_boi_chart_of_accounts d on bb.acct_id = d.acct_id
left join mf_department e on w.dept_id = e.dept_code
left join mf_division f on w.div_id = f.division_code
left join mf_project g on w.project_id = g.proj_id
left join mf_sub_project h on w.project_id = h.proj_id and w.sub_projectid = h.sub_proj_id AND h.status_id = 'A'

/***********************END OF CASH OUTFLOW*********************************/

-- select 'CASH OUTFLOW' as cf_type, d.acct_name, 'Advances to Officers/Employees' as inflow_type, c.acct_name, 
-- a.pv_no, b.cv_no, b.date_paid::date, y.tran_amt as pv_amt,
-- d.acct_id, w.pv_amt*-1 as real_outflow_amt, coalesce(f.division_alias,'') as div, 
-- coalesce(e.dept_alias,'') as dept, g.proj_alias, h.phase
-- from ( --select distinct on (co_id, pv_no, acct_id) co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid, sum(tran_amt) as tran_amt 
-- 		select distinct on (co_id, pv_no, acct_id) co_id, pv_no, acct_id, sum(tran_amt) as tran_amt 
-- 	from rf_pv_detail where status_id != 'I' and bal_side = 'D' 
-- 	and acct_id != '01-99-03-000'
-- 	and acct_id like '01-02%'
-- 	and co_id = p_co_id
-- 	and (case when p_proj_id = '' then true else project_id = p_proj_id end)
-- 	and (case when p_phase_no = '' then true else trim(sub_projectid) = p_phase_no end) 
-- 	group by co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid ) a
-- left join (select --distinct on (co_id, rplf_no, line_no) commented by jed 2021-05-12
-- 	* from rf_request_detail 
-- 	where co_id = p_co_id
-- 	and status_id = 'A' 
-- 	and acct_id != '08-03-03-006' /**added by jed : remove SOE Personnel**/
-- 	and acct_id !~* '08-03'
-- 	) w on a.pv_no = w.rplf_no and a.co_id = w.co_id --added by DG on 03/01/2017
-- left join (select * from rf_pv_header where co_id = p_co_id) x on a.pv_no = x.pv_no and a.co_id = x.co_id
-- left join (select distinct on (a.co_id, b.cv_no) a.co_id, b.cv_no, sum(a.tran_amt) as tran_amt 
-- 	from rf_pv_detail a
-- 	left join rf_pv_header b on a.pv_no = b.pv_no
-- 	where a.status_id != 'I' 
-- 	and b.status_id != 'I'
-- 	and a.co_id = p_co_id
-- 	and b.co_id = p_co_id
-- 	and a.bal_side = 'D' and a.acct_id != '01-99-03-000'
-- 	group by a.co_id, b.cv_no ) y on x.cv_no = y.cv_no and x.co_id = y.co_id
-- left join (select * from rf_pv_header where status_id = 'P' and co_id = p_co_id) aa on a.pv_no = aa.pv_no and a.co_id = aa.co_id
-- join (select * from rf_cv_header where status_id != 'I' and date_paid is not null 
-- 	and date_paid::date >= p_date_from and date_paid::date <= p_date_to and co_id = p_co_id) b on aa.cv_no = b.cv_no and aa.co_id = b.co_id
-- join (select distinct on (co_id, cv_no) co_id, cv_no, acct_id, sum(tran_amt) as tran_amt from rf_cv_detail 
-- 	where bal_side = 'C'
-- 	and status_id != 'I'
-- 	and acct_id not like '01-01-01%'
-- 	and co_id = p_co_id
-- 	group by co_id, cv_no, acct_id
-- 	) bb on b.cv_no = bb.cv_no and b.co_id = bb.co_id
-- left join mf_boi_chart_of_accounts c on w.acct_id = c.acct_id
-- left join mf_boi_chart_of_accounts d on bb.acct_id = d.acct_id
-- left join mf_department e on w.dept_id = e.dept_code
-- left join mf_division f on w.div_id = f.division_code
-- left join mf_project g on w.project_id = g.proj_id
-- left join mf_sub_project h on w.sub_projectid = h.sub_proj_id

-- UNION ALL

-- select 'CASH OUTFLOW' as cf_type, 
-- d.acct_name, 
-- 'Property Development Cost' as inflow_type, 
-- c.acct_name,
-- a.pv_no, 
-- b.cv_no,
-- b.date_paid, 
-- y.tran_amt as pv_amt,
-- d.acct_id, 
-- (a.tran_amt*(bb.tran_amt/y.tran_amt))*-1 as real_outflow_amt,
-- coalesce(f.division_alias,'') as div, 
-- coalesce(e.dept_alias,'') as dept,
-- g.proj_alias,
-- h.phase
-- from ( select distinct on (co_id, pv_no, acct_id, dept_id, div_id, project_id, sub_projectid) co_id, pv_no, acct_id, dept_id, div_id, project_id, sub_projectid, sum(tran_amt) as tran_amt 
-- 	from (select * from rf_pv_detail where acct_id like '01-03%' or acct_id like '04-02%')a where status_id != 'I' and bal_side = 'D' 
-- 	and acct_id != '01-99-03-000'
-- 	and co_id = p_co_id
-- 	and (case when p_proj_id = '' then true else project_id = p_proj_id end)
-- 	and (case when p_phase_no = '' then true else trim(sub_projectid) = p_phase_no end)
-- 	group by co_id, pv_no, acct_id, dept_id, div_id, project_id, sub_projectid) a
-- left join (select * from rf_pv_header where co_id = p_co_id) x on a.pv_no = x.pv_no and a.co_id = x.co_id
-- left join ( select distinct on (a.co_id, b.cv_no) a.co_id, b.cv_no, sum(a.tran_amt) as tran_amt 
-- 	from rf_pv_detail a
-- 	left join rf_pv_header b on a.pv_no = b.pv_no
-- 	where a.status_id != 'I' 
-- 	and b.status_id != 'I'
-- 	and a.bal_side = 'D' and a.acct_id != '01-99-03-000'
-- 	and a.co_id = p_co_id
-- 	and b.co_id = p_co_id
-- 	group by a.co_id, b.cv_no ) y on x.cv_no = y.cv_no and x.co_id = y.co_id
-- left join (select * from rf_pv_header where status_id = 'P' and co_id = p_co_id) aa on a.pv_no = aa.pv_no and a.co_id = aa.co_id
-- join (select * from rf_cv_header where status_id != 'I' and date_paid is not null 
-- 	and date_paid::date >= p_date_from and date_paid::date <= p_date_to and co_id = p_co_id) b on aa.cv_no = b.cv_no and aa.co_id = b.co_id
-- join (select distinct on (co_id, cv_no) co_id, cv_no, acct_id, sum(tran_amt) as tran_amt from rf_cv_detail 
-- 	where bal_side = 'C'
-- 	and status_id != 'I'
-- 	and acct_id not like '01-01-01%'
-- 	and co_id = p_co_id
-- 	group by co_id, cv_no, acct_id
-- 	) bb on b.cv_no = bb.cv_no and b.co_id = bb.co_id
-- left join mf_boi_chart_of_accounts c on a.acct_id = c.acct_id
-- left join mf_boi_chart_of_accounts d on bb.acct_id = d.acct_id
-- left join mf_department e on a.dept_id = e.dept_code
-- left join mf_division f on a.div_id = f.division_code
-- left join mf_project g on a.project_id = g.proj_id
-- left join mf_sub_project h on a.sub_projectid = h.sub_proj_id

-- UNION ALL

-- select 'CASH OUTFLOW' as cf_type, d.acct_name, 'Commission' as inflow_type, c.acct_name, 
-- a.pv_no, b.cv_no, b.date_paid, y.tran_amt as pv_amt,
-- d.acct_id, (a.tran_amt*(bb.tran_amt/y.tran_amt))*-1 as real_outflow_amt, coalesce(f.division_alias,'') as div, 
-- coalesce(e.dept_alias,'') as dept, g.proj_alias, h.phase
-- from ( select distinct on (co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid) co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid, sum(tran_amt) as tran_amt 
-- 	from rf_pv_detail where status_id != 'I' and bal_side = 'D' 
-- 		and acct_id != '01-99-03-000'
-- 		and acct_id = '08-03-01-000'
-- 		--and pv_no not in (select distinct on (pv_no) pv_no from rf_pv_detail where acct_id != '01-99-03-000'
-- 		--	and acct_id like '01-02%' and status_id != 'I' and bal_side = 'D' )
-- 		and co_id = p_co_id
-- 		and (case when p_proj_id = '' then true else project_id = p_proj_id end)
-- 		and (case when p_phase_no = '' then true else trim(sub_projectid) = p_phase_no end) 
-- 		group by co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid ) a
-- left join (select * from rf_pv_header where co_id = p_co_id) x on a.pv_no = x.pv_no and a.co_id = x.co_id
-- left join (select distinct on (a.co_id, b.cv_no) a.co_id, b.cv_no, sum(a.tran_amt) as tran_amt 
-- 	from rf_pv_detail a
-- 	left join rf_pv_header b on a.pv_no = b.pv_no
-- 	where a.status_id != 'I' 
-- 	and b.status_id != 'I'
-- 	and a.co_id = p_co_id
-- 	and b.co_id = p_co_id
-- 	and a.bal_side = 'D' and a.acct_id != '01-99-03-000'
-- 	group by a.co_id, b.cv_no) y on x.cv_no = y.cv_no and x.co_id = y.co_id
-- left join (select * from rf_pv_header where status_id = 'P' and co_id = p_co_id) aa on a.pv_no = aa.pv_no and a.co_id = aa.co_id
-- join (select * from rf_cv_header where status_id != 'I' and date_paid is not null 
-- 	and date_paid::date >= p_date_from and date_paid::date <= p_date_to and co_id = p_co_id) b on aa.cv_no = b.cv_no and aa.co_id = b.co_id
-- join (select distinct on (co_id, cv_no) co_id, cv_no, acct_id, sum(tran_amt) as tran_amt from rf_cv_detail 
-- 	where bal_side = 'C'
-- 	and status_id != 'I'
-- 	and co_id = p_co_id 
-- 	and acct_id not like '01-01-01%'
-- 	group by co_id, cv_no, acct_id
-- 	) bb on b.cv_no = bb.cv_no and b.co_id = bb.co_id
-- left join mf_boi_chart_of_accounts c on a.acct_id = c.acct_id
-- left join mf_boi_chart_of_accounts d on bb.acct_id = d.acct_id
-- left join mf_department e on a.dept_id = e.dept_code
-- left join mf_division f on a.div_id = f.division_code
-- left join mf_project g on a.project_id = g.proj_id
-- left join mf_sub_project h on a.sub_projectid = h.sub_proj_id

-- UNION ALL

-- select 'CASH OUTFLOW' as cf_type, d.acct_name, 'MAF/SOE' as inflow_type, c.acct_name, 
-- a.pv_no, b.cv_no, b.date_paid, y.tran_amt as pv_amt,
-- d.acct_id, (a.tran_amt*(bb.tran_amt/y.tran_amt))*-1 as real_outflow_amt, coalesce(f.division_alias,'') as div, 
-- coalesce(e.dept_alias,'') as dept, g.proj_alias, h.phase
-- from ( select distinct on (co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid) co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid, sum(tran_amt) as tran_amt 
-- 	from rf_pv_detail where status_id != 'I' and bal_side = 'D' 
-- 		and acct_id != '01-99-03-000'
-- 		and acct_id != '08-03-01-000'
-- 		and acct_id like '08-03%'
-- 		--and pv_no not in (select distinct on (pv_no) pv_no from rf_pv_detail where acct_id != '01-99-03-000'
-- 		--	and acct_id like '01-02%' and status_id != 'I' and bal_side = 'D' )
-- 		and co_id = p_co_id
-- 		and (case when p_proj_id = '' then true else project_id = p_proj_id end)
-- 		and (case when p_phase_no = '' then true else trim(sub_projectid) = p_phase_no end) 
-- 		group by co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid ) a
-- left join (select * from rf_pv_header where co_id = p_co_id) x on a.pv_no = x.pv_no and a.co_id = x.co_id
-- left join (select distinct on (a.co_id, b.cv_no) a.co_id, b.cv_no, sum(a.tran_amt) as tran_amt 
-- 	from rf_pv_detail a
-- 	left join rf_pv_header b on a.pv_no = b.pv_no
-- 	where a.status_id != 'I' 
-- 	and b.status_id != 'I'
-- 	and a.co_id = p_co_id
-- 	and b.co_id = p_co_id
-- 	and a.bal_side = 'D' and a.acct_id != '01-99-03-000'
-- 	group by a.co_id, b.cv_no ) y on x.cv_no = y.cv_no and x.co_id = y.co_id
-- left join (select * from rf_pv_header where status_id = 'P' and co_id = p_co_id) aa on a.pv_no = aa.pv_no
-- join (select * from rf_cv_header where status_id != 'I' and date_paid is not null 
-- 	and date_paid::date >= p_date_from and date_paid::date <= p_date_to and co_id = p_co_id) b on aa.cv_no = b.cv_no and aa.co_id = b.co_id
-- join (select distinct on (co_id, cv_no) co_id, cv_no, acct_id, sum(tran_amt) as tran_amt from rf_cv_detail 
-- 	where bal_side = 'C'
-- 	and status_id != 'I'
-- 	and co_id = p_co_id
-- 	and acct_id not like '01-01-01%'
-- 	group by co_id, cv_no, acct_id
-- 	) bb on b.cv_no = bb.cv_no and b.co_id = bb.co_id
-- left join mf_boi_chart_of_accounts c on a.acct_id = c.acct_id
-- left join mf_boi_chart_of_accounts d on bb.acct_id = d.acct_id
-- left join mf_department e on a.dept_id = e.dept_code
-- left join mf_division f on a.div_id = f.division_code
-- left join mf_project g on a.project_id = g.proj_id
-- left join mf_sub_project h on a.sub_projectid = h.sub_proj_id

-- UNION ALL

-- select 'CASH OUTFLOW' as cf_type, d.acct_name, 'SME/GAE' as inflow_type, c.acct_name, 
-- a.pv_no, b.cv_no, b.date_paid, y.tran_amt as pv_amt,
-- d.acct_id, (a.tran_amt*(bb.tran_amt/y.tran_amt))*-1 as real_outflow_amt, coalesce(f.division_alias,'') as div, 
-- coalesce(e.dept_alias,'') as dept, g.proj_alias, h.phase
-- from ( select distinct on (co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid) co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid, sum(tran_amt) as tran_amt 
-- 	from (select * from rf_pv_detail where acct_id like '08-01%' or acct_id like '08-02%'
-- 	) a where status_id != 'I' and bal_side = 'D' 
-- 		and acct_id != '01-99-03-000'
-- 		and acct_id != '08-03-01-000'
-- 		--and pv_no not in (select distinct on (pv_no) pv_no from rf_pv_detail where acct_id != '01-99-03-000'
-- 		--	and acct_id like '01-02%' and status_id != 'I' and bal_side = 'D' )
-- 		and co_id = p_co_id
-- 		and (case when p_proj_id = '' then true else project_id = p_proj_id end)
-- 		and (case when p_phase_no = '' then true else trim(sub_projectid) = p_phase_no end) 
-- 		group by co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid ) a
-- left join (select * from rf_pv_header where co_id = p_co_id) x on a.pv_no = x.pv_no and a.co_id = x.co_id
-- left join ( select distinct on (a.co_id, b.cv_no) a.co_id, b.cv_no, sum(a.tran_amt) as tran_amt 
-- 	from rf_pv_detail a
-- 	left join rf_pv_header b on a.pv_no = b.pv_no
-- 	where a.status_id != 'I' 
-- 	and b.status_id != 'I'
-- 	and a.co_id = p_co_id
-- 	and b.co_id = p_co_id
-- 	and a.bal_side = 'D' and a.acct_id != '01-99-03-000'
-- 	group by a.co_id, b.cv_no ) y on x.cv_no = y.cv_no and x.co_id = y.co_id
-- left join (select * from rf_pv_header where status_id = 'P' and co_id = p_co_id) aa on a.pv_no = aa.pv_no and a.co_id = aa.co_id
-- join (select * from rf_cv_header where status_id != 'I' and date_paid is not null 
-- 	and date_paid::date >= p_date_from and date_paid::date <= p_date_to and co_id = p_co_id) b on aa.cv_no = b.cv_no and aa.co_id = b.co_id
-- join (select distinct on (co_id, cv_no) co_id, cv_no, acct_id, sum(tran_amt) as tran_amt from rf_cv_detail 
-- 	where bal_side = 'C'
-- 	and status_id != 'I'
-- 	and co_id = p_co_id
-- 	and acct_id not like '01-01-01%'
-- 	group by co_id, cv_no, acct_id
-- 	) bb on b.cv_no = bb.cv_no and b.co_id = bb.co_id
-- left join mf_boi_chart_of_accounts c on a.acct_id = c.acct_id
-- left join mf_boi_chart_of_accounts d on bb.acct_id = d.acct_id
-- left join mf_department e on a.dept_id = e.dept_code
-- left join mf_division f on a.div_id = f.division_code
-- left join mf_project g on a.project_id = g.proj_id
-- left join mf_sub_project h on a.sub_projectid = h.sub_proj_id

-- UNION ALL

-- select 'CASH OUTFLOW' as cf_type, d.acct_name, 'PPE' as inflow_type, c.acct_name, 
-- a.pv_no, b.cv_no, b.date_paid, y.tran_amt as pv_amt,
-- d.acct_id, (a.tran_amt*(bb.tran_amt/y.tran_amt))*-1 as real_outflow_amt, coalesce(f.division_alias,'') as div, 
-- coalesce(e.dept_alias,'') as dept, g.proj_alias, h.phase
-- from ( select distinct on (co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid) co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid, sum(tran_amt) as tran_amt 
-- 	from (select * from rf_pv_detail where acct_id like '02-03%' ) a where status_id != 'I' and bal_side = 'D' 
-- 		and acct_id != '01-99-03-000'
-- 		and acct_id != '08-03-01-000'
-- 		--and pv_no not in (select distinct on (pv_no) pv_no from rf_pv_detail where acct_id != '01-99-03-000'
-- 		--	and acct_id like '01-02%' and status_id != 'I' and bal_side = 'D' )
-- 		and co_id = p_co_id
-- 		and (case when p_proj_id = '' then true else project_id = p_proj_id end)
-- 		and (case when p_phase_no = '' then true else trim(sub_projectid) = p_phase_no end) 
-- 		group by co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid ) a
-- left join (select * from rf_pv_header where co_id = p_co_id) x on a.pv_no = x.pv_no and a.co_id = x.co_id
-- left join ( select distinct on (a.co_id, b.cv_no) a.co_id, b.cv_no, sum(a.tran_amt) as tran_amt 
-- 	from rf_pv_detail a
-- 	left join rf_pv_header b on a.pv_no = b.pv_no
-- 	where a.status_id != 'I' 
-- 	and b.status_id != 'I'
-- 	and a.co_id = p_co_id
-- 	and b.co_id = p_co_id
-- 	and a.bal_side = 'D' and a.acct_id != '01-99-03-000'
-- 	group by a.co_id, b.cv_no ) y on x.cv_no = y.cv_no and x.co_id = y.co_id
-- left join (select * from rf_pv_header where status_id = 'P' and co_id = p_co_id) aa on a.pv_no = aa.pv_no and a.co_id = aa.co_id
-- join (select * from rf_cv_header where status_id != 'I' and date_paid is not null 
-- 	and date_paid::date >= p_date_from and date_paid::date <= p_date_to and co_id = p_co_id) b on aa.cv_no = b.cv_no and aa.co_id = b.co_id
-- join (select distinct on (cv_no) co_id, cv_no, acct_id, sum(tran_amt) as tran_amt from rf_cv_detail 
-- 	where bal_side = 'C'
-- 	and status_id != 'I'
-- 	and acct_id not like '01-01-01%'
-- 	and co_id = p_co_id
-- 	group by co_id, cv_no, acct_id
-- 	) bb on b.cv_no = bb.cv_no and b.co_id = bb.co_id
-- left join mf_boi_chart_of_accounts c on a.acct_id = c.acct_id
-- left join mf_boi_chart_of_accounts d on bb.acct_id = d.acct_id
-- left join mf_department e on a.dept_id = e.dept_code
-- left join mf_division f on a.div_id = f.division_code
-- left join mf_project g on a.project_id = g.proj_id
-- left join mf_sub_project h on a.sub_projectid = h.sub_proj_id

-- UNION ALL

-- select 'CASH OUTFLOW' as cf_type, d.acct_name, 'Return of Payment Withheld' as inflow_type, c.acct_name, 
-- a.pv_no, b.cv_no, b.date_paid, y.tran_amt as pv_amt,
-- d.acct_id, (a.tran_amt*(bb.tran_amt/y.tran_amt))*-1 as real_outflow_amt, coalesce(f.division_alias,'') as div, 
-- coalesce(e.dept_alias,'') as dept, g.proj_alias, h.phase
-- from ( select distinct on (co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid) co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid, sum(tran_amt) as tran_amt 
-- 	from (select * from rf_pv_detail where acct_id like '03-01%' ) a where status_id != 'I' and bal_side = 'D' 
-- 		and acct_id != '01-99-03-000'
-- 		and acct_id != '08-03-01-000'
-- 		--and pv_no not in (select distinct on (pv_no) pv_no from rf_pv_detail where acct_id != '01-99-03-000'
-- 		--	and acct_id like '01-02%' and status_id != 'I' and bal_side = 'D' )
-- 		and co_id = p_co_id
-- 		and (case when p_proj_id = '' then true else project_id = p_proj_id end)
-- 		and (case when p_phase_no = '' then true else trim(sub_projectid) = p_phase_no end) 
-- 		group by co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid ) a
-- left join (select * from rf_pv_header where co_id = p_co_id) x on a.pv_no = x.pv_no and a.co_id = x.co_id
-- left join ( select distinct on (a.co_id, b.cv_no) a.co_id, b.cv_no, sum(a.tran_amt) as tran_amt 
-- 	from rf_pv_detail a
-- 	left join rf_pv_header b on a.pv_no = b.pv_no
-- 	where a.status_id != 'I' 
-- 	and b.status_id != 'I'
-- 	and a.co_id = p_co_id
-- 	and b.co_id = p_co_id
-- 	and a.bal_side = 'D' and a.acct_id != '01-99-03-000'
-- 	group by a.co_id, b.cv_no ) y on x.cv_no = y.cv_no
-- left join (select * from rf_pv_header where status_id = 'P' and co_id = p_co_id) aa on a.pv_no = aa.pv_no and a.co_id = aa.co_id
-- join (select * from rf_cv_header where status_id != 'I' and date_paid is not null 
-- 	and date_paid::date >= p_date_from and date_paid::date <= p_date_to and co_id = p_co_id) b on aa.cv_no = b.cv_no and aa.co_id = b.co_id
-- join (select distinct on (co_id, cv_no) co_id, cv_no, acct_id, sum(tran_amt) as tran_amt from rf_cv_detail 
-- 	where bal_side = 'C'
-- 	and status_id != 'I'
-- 	and acct_id not like '01-01-01%'
-- 	and co_id = p_co_id
-- 	group by co_id, cv_no, acct_id
-- 	) bb on b.cv_no = bb.cv_no and b.co_id = bb.co_id
-- left join mf_boi_chart_of_accounts c on a.acct_id = c.acct_id
-- left join mf_boi_chart_of_accounts d on bb.acct_id = d.acct_id
-- left join mf_department e on a.dept_id = e.dept_code
-- left join mf_division f on a.div_id = f.division_code
-- left join mf_project g on a.project_id = g.proj_id
-- left join mf_sub_project h on a.sub_projectid = h.sub_proj_id

-- UNION ALL

-- select 'CASH OUTFLOW' as cf_type, d.acct_name, 'Fund Transfer' as inflow_type, c.acct_name, 
-- a.pv_no, b.cv_no, b.date_paid, y.tran_amt as pv_amt,
-- d.acct_id, (a.tran_amt*(bb.tran_amt/y.tran_amt))*-1 as real_outflow_amt, coalesce(f.division_alias,'') as div, 
-- coalesce(e.dept_alias,'') as dept, g.proj_alias, h.phase
-- from ( select distinct on (co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid) co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid, sum(tran_amt) as tran_amt 
-- 	from (select * from rf_pv_detail where acct_id like '01-01%' ) a where status_id != 'I' and bal_side = 'D' 
-- 		and acct_id != '01-99-03-000'
-- 		and acct_id != '08-03-01-000'
-- 		--and pv_no not in (select distinct on (pv_no) pv_no from rf_pv_detail where acct_id != '01-99-03-000'
-- 		--	and acct_id like '01-02%' and status_id != 'I' and bal_side = 'D' )
-- 		and co_id = p_co_id
-- 		and (case when p_proj_id = '' then true else project_id = p_proj_id end)
-- 		and (case when p_phase_no = '' then true else trim(sub_projectid) = p_phase_no end) 
-- 		group by co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid ) a
-- left join (select * from rf_pv_header where co_id = p_co_id) x on a.pv_no = x.pv_no and a.co_id = x.co_id
-- left join ( select distinct on (a.co_id, b.cv_no) a.co_id, b.cv_no, sum(a.tran_amt) as tran_amt 
-- 	from rf_pv_detail a
-- 	left join rf_pv_header b on a.pv_no = b.pv_no
-- 	where a.status_id != 'I' 
-- 	and b.status_id != 'I'
-- 	and a.bal_side = 'D' 
-- 	and a.acct_id != '01-99-03-000'
-- 	and a.co_id = p_co_id
-- 	and b.co_id = p_co_id
-- 	group by a.co_id, b.cv_no ) y on x.cv_no = y.cv_no and x.co_id = y.co_id
-- left join (select * from rf_pv_header where status_id = 'P' and co_id = p_co_id) aa on a.pv_no = aa.pv_no and a.co_id = aa.co_id
-- join (select * from rf_cv_header where status_id != 'I' and date_paid is not null 
-- 	and date_paid::date >= p_date_from and date_paid::date <= p_date_to and co_id = p_co_id)  b on aa.cv_no = b.cv_no and aa.co_id = b.co_id
-- join (select distinct on (co_id, cv_no) co_id, cv_no, acct_id, sum(tran_amt) as tran_amt from rf_cv_detail 
-- 	where bal_side = 'C'
-- 	and status_id != 'I'
-- 	and acct_id not like '01-01-01%'
-- 	and co_id = p_co_id
-- 	group by co_id, cv_no, acct_id
-- 	) bb on b.cv_no = bb.cv_no and b.co_id = bb.co_id
-- left join mf_boi_chart_of_accounts c on a.acct_id = c.acct_id
-- left join mf_boi_chart_of_accounts d on bb.acct_id = d.acct_id
-- left join mf_department e on a.dept_id = e.dept_code
-- left join mf_division f on a.div_id = f.division_code
-- left join mf_project g on a.project_id = g.proj_id
-- left join mf_sub_project h on a.sub_projectid = h.sub_proj_id

-- UNION ALL

-- select 'CASH OUTFLOW' as cf_type, d.acct_name, 'Others' as inflow_type, c.acct_name, 
-- a.pv_no, b.cv_no, b.date_paid, y.tran_amt as pv_amt,
-- d.acct_id, (a.tran_amt*(bb.tran_amt/y.tran_amt))*-1 as real_outflow_amt, coalesce(f.division_alias,'') as div, 
-- coalesce(e.dept_alias,'') as dept, g.proj_alias, h.phase
-- from ( select distinct on (co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid) co_id, pv_no, acct_id, div_id, dept_id, project_id, sub_projectid, sum(tran_amt) as tran_amt 
-- 	from rf_pv_detail a where status_id != 'I' and bal_side = 'D' 
-- 		and acct_id != '01-99-03-000'
-- 		and acct_id not like '01-01%'  --added by DG : 03/01/2017
-- 		and acct_id not like '01-02%'
-- 		and acct_id not like '01-03%'
-- 		and acct_id != '08-03-01-000'
-- 		and acct_id not like '08-03%'
-- 		and acct_id not like '08-01%' 
-- 		and acct_id not like '08-02%'
-- 		and acct_id not like '02-03%'
-- 		and acct_id not like '03-01%'
-- 		and acct_id not like '04-02%'
-- 		--and pv_no not in (select distinct on (pv_no) pv_no from rf_pv_detail where acct_id != '01-99-03-000'
-- 		--	and acct_id like '01-02%' and status_id != 'I' and bal_side = 'D' and co_id = p_co_id )
-- 		and co_id = p_co_id
-- 		and (case when p_proj_id = '' then true else project_id = p_proj_id end)
-- 		and (case when p_phase_no = '' then true else trim(sub_projectid) = p_phase_no end) 
-- 		group by co_id, pv_no, acct_id, dept_id, div_id, project_id, sub_projectid ) a
-- left join (select * from rf_pv_header where co_id = p_co_id) x on a.pv_no = x.pv_no and a.co_id = x.co_id
-- left join ( select distinct on (a.co_id, b.cv_no) a.co_id, b.cv_no, sum(a.tran_amt) as tran_amt 
-- 	from rf_pv_detail a
-- 	left join rf_pv_header b on a.pv_no = b.pv_no
-- 	where a.status_id != 'I' 
-- 	and b.status_id != 'I'
-- 	and a.co_id = p_co_id
-- 	and b.co_id = p_co_id
-- 	and a.bal_side = 'D' and a.acct_id != '01-99-03-000'
-- 	group by a.co_id, b.cv_no ) y on x.cv_no = y.cv_no and x.co_id = y.co_id
-- left join (select * from rf_pv_header where status_id = 'P' and co_id = p_co_id) aa on a.pv_no = aa.pv_no and a.co_id = aa.co_id
-- join (select * from rf_cv_header where status_id != 'I' and date_paid is not null 
-- 	and date_paid::date >= p_date_from and date_paid::date <= p_date_to and co_id = p_co_id) b on aa.cv_no = b.cv_no and aa.co_id = b.co_id
-- join (select distinct on (co_id, cv_no) co_id, cv_no, acct_id, sum(tran_amt) as tran_amt from rf_cv_detail 
-- 	where bal_side = 'C'
-- 	and status_id != 'I'
-- 	and acct_id not like '01-01-01%'
-- 	and co_id = p_co_id
-- 	group by co_id, cv_no, acct_id
-- 	) bb on b.cv_no = bb.cv_no and b.co_id = bb.co_id
-- left join mf_boi_chart_of_accounts c on a.acct_id = c.acct_id
-- left join mf_boi_chart_of_accounts d on bb.acct_id = d.acct_id
-- left join mf_department e on a.dept_id = e.dept_code
-- left join mf_division f on a.div_id = f.division_code
-- left join mf_project g on a.project_id = g.proj_id
-- left join mf_sub_project h on a.sub_projectid = h.sub_proj_id

-- /***********************END OF CASH OUTFLOW*********************************/

) A

left join (select * from rf_check where status_id = 'A') B on A.cv_no = B.cv_no	  
where B.co_id = p_co_id
	  
order by cf_type, acct_desc, particulars

	
-------------------------------END SQL
) LOOP

SELECT INTO v_CV_Header * FROM rf_cv_header where cv_no = v_rec.cv_no and co_id = p_co_id;

c_cf_type	:= v_rec.cf_type;
c_acct_desc	:= v_rec.acct_desc;
c_inflow_type	:= v_rec.inflow_type;
c_particulars	:= v_rec.particulars;
c_pv_no		:= v_rec.pv_no;
c_cv_no		:= v_rec.cv_no;
c_date_paid	:= v_rec.date_paid;
c_pv_amt	:= v_rec.pv_amt;
c_acct_id	:= v_rec.acct_id;
c_amount	:= v_rec.amount;
c_div		:= v_rec.div;
c_dept		:= v_rec.dept;
c_proj		:= v_rec.proj;
c_phase		:= v_rec.phase;
c_check_no	:= v_rec.check_no;
c_status	:= (SELECT get_proc_desc(v_CV_Header.proc_id));

  RETURN NEXT;
  END LOOP;
  /*
if exists(select * from rf_trigger_filter where table_name = 'view_sched_cashflow_details_with_proj_phase') then
		update rf_trigger_filter
		set date_updated = now()
		where table_name = 'view_sched_cashflow_details_with_proj_phase'; 
	else
		insert into rf_trigger_filter (user_id,activity,table_name,date_updated) values ('00000','EXECUTE','view_sched_cashflow_details_with_proj_phase',now());
	end if; */
END;
$BODY$;

ALTER FUNCTION public.view_sched_cashflow_details_with_proj_phase(character varying, character varying, character varying, timestamp without time zone, timestamp without time zone)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION public.view_sched_cashflow_details_with_proj_phase(character varying, character varying, character varying, timestamp without time zone, timestamp without time zone) TO PUBLIC;

GRANT EXECUTE ON FUNCTION public.view_sched_cashflow_details_with_proj_phase(character varying, character varying, character varying, timestamp without time zone, timestamp without time zone) TO employee;

GRANT EXECUTE ON FUNCTION public.view_sched_cashflow_details_with_proj_phase(character varying, character varying, character varying, timestamp without time zone, timestamp without time zone) TO postgres;

COMMENT ON FUNCTION IS 
